"""
YouTube Livestream Data Extractor - CSV Only (GitHub Actions Safe)
=================================================================
- Extracts latest livestream data
- Skips scheduled/upcoming streams (only already streamed)
- Adds: teacher_name, live_status, published_at, published_time,
         days_since_published, likes, comments
- Teacher name is detected:
    1) Directly from title (Danish, Isha, Kajal, etc.)
    2) If Unknown, from subject/patterns in title using get_teacher()
- Adds derived metrics:
    engagement_score, duration_minutes, views_per_minute, views_per_day,
    engagement_per_view, like_rate, comment_rate
- Always creates CSV:
    data/latest_20_livestreams_precise.csv
- NO Excel, NO openpyxl required
"""

import re
import json
import os
import requests
import pandas as pd
from datetime import datetime, timezone, date
import time
import warnings

warnings.filterwarnings('ignore')

# ================= CONFIG =================

CHANNEL_URL = "https://www.youtube.com/@teachingpariksha"
TARGET_LIVESTREAMS = 20

# ================= SESSION =================

session = requests.Session()
# Use Colab-style rich headers to match working behaviour
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
})

print("✅ HTTP session configured")

# ================= HELPERS =================

def extract_json_from_html(html, var_name='ytInitialData'):
    """Generic extractor for embedded JSON blobs like ytInitialData."""
    pattern = rf'var {var_name}\s*=\s*(\{{.*?\}});'
    match = re.search(pattern, html, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except Exception:
            pass

    idx = html.find(var_name)
    if idx == -1:
        return None

    start = html.find('{', idx)
    if start == -1:
        return None

    depth = 0
    for i in range(start, len(html)):
        if html[i] == '{':
            depth += 1
        elif html[i] == '}':
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(html[start:i+1])
                except Exception:
                    return None
    return None


def safe_get(d, *keys, default=None):
    for k in keys:
        if isinstance(d, dict):
            d = d.get(k, default)
        elif isinstance(d, list) and isinstance(k, int):
            if k < len(d):
                d = d[k]
            else:
                return default
        else:
            return default
    return d


def parse_exact_count(text):
    if not text:
        return 0
    text = re.sub(r"[^\d]", "", str(text))
    return int(text) if text.isdigit() else 0


def parse_duration_text(text):
    if not text:
        return 0
    parts = [int(p) for p in text.split(":") if p.isdigit()]
    if len(parts) == 3:
        return parts[0]*3600 + parts[1]*60 + parts[2]
    if len(parts) == 2:
        return parts[0]*60 + parts[1]
    return parts[0] if parts else 0


# ----- Teacher detection -----
# 1) Direct name detection
TEACHER_MAP_DIRECT = {
    "danish": "Danish Sir",
    "deepali": "Deepali Ma'am",
    "isha": "Isha Ma'am",
    "kuldeep": "Kuldeep Sir",
    "kajal": "Kajal Ma'am",
    "mona": "Mona Ma'am",
    "pawan": "Pawan Sir",
    "narjis": "Narjis Ma'am",
    "sachin": "Sachin Sir",
    "abha": "Abha Ma'am",
    "pooja": "Pooja Ma'am",  # for direct "Pooja Ma'am" in title if present
}

def detect_teacher_by_name(text: str) -> str:
    """Direct name-based detection in title."""
    if not text:
        return "Unknown"
    low = text.lower()

    for key, value in TEACHER_MAP_DIRECT.items():
        if key in low:
            return value

    # Pattern: 'Name Sir' / 'Name Ma'am'
    m = re.search(r'([A-Za-z]+)\s+(sir|ma[\'a]?am)', low)
    if m:
        name = m.group(1).lower()
        if name in TEACHER_MAP_DIRECT:
            return TEACHER_MAP_DIRECT[name]

    return "Unknown"


# 2) Subject-based mapping you provided
def get_teacher(t: str) -> str:
    t = t.lower()  # Normalize to lowercase for easier matching

    # --- EVS & Science ---
    # Mona Ma'am for Core EVS
    if " evs" in t or "environmental studies" in t or "environment studies" in t:
        return "Mona Ma'am"
    
    # Kuldeep Sir for General Science (Physics/Chem/Bio)
    if "science" in t and "social" not in t: 
        return "Kuldeep Sir"

    # --- Languages ---
    # Hindi -> Isha Ma'am
    if " hindi" in t:
        return "Isha Ma'am"

    # English -> Pooja Ma'am
    if " english" in t:
        return "Pooja Ma'am"

    # --- Reasoning & Computer ---
    # Kajal Ma'am covers both Reasoning and Computer recently
    if " reasoning" in t or "logical" in t or "mental ability" in t:
        return "Kajal Ma'am"

    if " computer" in t:
        return "Kajal Ma'am"  # Updated from Abha Ma'am

    # --- Maths ---
    # Maths -> Pawan Sir
    if " maths" in t or " math " in t or "mathematics" in t or "numerical" in t or "quant" in t:
        return "Pawan Sir"

    # --- SST / GK / CDP ---
    # Danish Sir covers the broad "General Awareness" and "Child Dev" spectrum
    if " cdp" in t or "child development" in t:
        return "Danish Sir"
        
    if " gk" in t or "general knowledge" in t or "current affairs" in t or " gs" in t:
        return "Danish Sir"

    # SST -> Danish Sir (Updated from Sachin Sir)
    if " sst " in t or "social science" in t or "social studies" in t:
        return "Danish Sir"

    return "Unknown"


def extract_teacher_name_from_title(title: str) -> str:
    """
    Combined logic:
    1) Try direct name in title
    2) If Unknown, try subject-based mapping using get_teacher()
    """
    # 1. Direct detection
    name = detect_teacher_by_name(title or "")
    if name != "Unknown":
        return name

    # 2. Subject-based heuristic
    return get_teacher(title or "")


def days_since(date_str, fmt="%d-%m-%Y"):
    """Compute days since given date string in dd-mm-YYYY format."""
    if not date_str:
        return None
    try:
        d = datetime.strptime(date_str, fmt).date()
    except Exception:
        return None
    return (date.today() - d).days


def is_scheduled_or_upcoming(video):
    """
    Try to detect scheduled/upcoming streams and filter them out.
    """
    # upcomingEventData present
    if safe_get(video, 'upcomingEventData') is not None:
        return True

    # badges saying "UPCOMING" or "Scheduled"
    badges = safe_get(video, 'badges', default=[])
    for b in badges:
        label = (safe_get(b, 'metadataBadgeRenderer', 'label', default='') or '').lower()
        if 'upcoming' in label or 'scheduled' in label:
            return True

    # thumbnail overlays detecting upcoming
    overlays = safe_get(video, 'thumbnailOverlays', default=[])
    for o in overlays:
        style = safe_get(o, 'thumbnailOverlayTimeStatusRenderer', 'style', default='')
        if isinstance(style, str) and 'upcoming' in style.lower():
            return True
        text_label = (safe_get(o, 'thumbnailOverlayTimeStatusRenderer', 'text', 'simpleText', default='') or '').lower()
        if 'upcoming' in text_label or 'scheduled' in text_label:
            return True

    # viewCountText typical phrases: "Waiting...", "Scheduled for ..."
    vc_text = safe_get(video, 'viewCountText', 'simpleText', default='') or ''
    if isinstance(vc_text, str):
        vc_low = vc_text.lower()
        if 'waiting' in vc_low or 'scheduled for' in vc_low:
            return True

    return False

# ================= SCRAPER =================

def fetch_channel_videos(url):
    tabs = [f"{url}/streams", f"{url}/videos", url]

    for tab_url in tabs:
        print("Trying:", tab_url)
        r = session.get(tab_url, timeout=30)
        yt_data = extract_json_from_html(r.text, 'ytInitialData')
        if not yt_data:
            continue

        tab_data = safe_get(yt_data, 'contents', 'twoColumnBrowseResultsRenderer', 'tabs', default=[])
        videos = []

        for tab in tab_data:
            content = safe_get(tab, 'tabRenderer', 'content', default={})
            rich = safe_get(content, 'richGridRenderer', 'contents', default=[])

            for item in rich:
                vid = safe_get(item, 'richItemRenderer', 'content', 'videoRenderer')
                if vid:
                    videos.append(vid)

        if videos:
            return videos

    return []


def extract_video_details(video_url):
    """
    For a given video URL:
    - likes
    - comments
    - published_at (dd-mm-YYYY)  <-- SAME FORMAT AS COLAB
    - published_time (HH:MM:SS)
    - days_since_published (int)
    """
    try:
        r = session.get(video_url, timeout=30)
        html = r.text

        # -------- Likes --------
        likes_match = re.search(r'"label":"([\d,]+) likes"', html)
        likes = parse_exact_count(likes_match.group(1)) if likes_match else 0

        # -------- Comments --------
        comments_match = re.search(r'"commentCount":"(\d+)"', html)
        comments = int(comments_match.group(1)) if comments_match else 0

        # -------- Published date/time (Colab logic) --------
        published_at = ""
        published_time = ""
        days = None

        upload_match = re.search(r'"uploadDate":\s*"([^"]+)"', html)
        if upload_match:
            try:
                raw = upload_match.group(1)  # e.g., 2025-02-10T15:30:00Z  or 2025-02-10
                # Colab-style: use fromisoformat after replacing Z
                iso = raw.replace('Z', '+00:00')
                if 'T' in iso:
                    dt = datetime.fromisoformat(iso)
                else:
                    dt = datetime.strptime(iso, "%Y-%m-%d")
                published_at = dt.strftime('%d-%m-%Y')  # EXACTLY LIKE COLAB
                published_time = dt.strftime('%H:%M:%S')
                days = (date.today() - dt.date()).days
            except Exception:
                pass

        # Fallback if uploadDate is missing or parsing failed
        if not published_at:
            now = datetime.now(timezone.utc)
            published_at = now.strftime('%d-%m-%Y')
            published_time = now.strftime('%H:%M:%S')
            days = 0

        return likes, comments, published_at, published_time, days

    except Exception:
        # Safe fallback
        now = datetime.now(timezone.utc)
        return (
            0,  # likes
            0,  # comments
            now.strftime('%d-%m-%Y'),
            now.strftime('%H:%M:%S'),
            0
        )


def main():
    videos_data = fetch_channel_videos(CHANNEL_URL)
    print("✅ Total videos extracted from channel tabs:", len(videos_data))

    livestream_data = []

    for video in videos_data:
        if len(livestream_data) >= TARGET_LIVESTREAMS:
            break

        # Skip scheduled/upcoming streams
        if is_scheduled_or_upcoming(video):
            continue

        video_id = safe_get(video, 'videoId')
        if not video_id:
            continue

        title_runs = safe_get(video, 'title', 'runs', default=[])
        title = "".join([r.get("text", "") for r in title_runs])

        view_text = safe_get(video, 'viewCountText', 'simpleText', default='')
        views = parse_exact_count(view_text)

        len_text = safe_get(video, 'lengthText', 'simpleText', default='')
        duration_sec = parse_duration_text(len_text)

        video_url = f"https://www.youtube.com/watch?v={video_id}"
        likes, comments, published_at, published_time, days_since_pub = extract_video_details(video_url)

        # Teacher detection from title (direct name → subject pattern)
        teacher_name = extract_teacher_name_from_title(title)

        livestream_data.append({
            "video_id": video_id,
            "title": title,
            "teacher_name": teacher_name,
            "live_status": "was_live",
            "published_at": published_at,
            "published_time": published_time,
            "days_since_published": days_since_pub if days_since_pub is not None else "",
            "views": views,
            "likes": likes,
            "comments": comments,
            "duration_seconds": duration_sec,
            "url": video_url
        })

        # Gentle delay to avoid hammering YouTube
        time.sleep(0.3)

    print("✅ Final livestream rows (excluding scheduled/upcoming):", len(livestream_data))

    # ------- Build DataFrame & derived metrics -------

    base_columns = [
        "video_id", "title", "teacher_name",
        "live_status", "published_at", "published_time",
        "days_since_published", "views", "likes", "comments",
        "duration_seconds", "url"
    ]

    df = pd.DataFrame(livestream_data)

    # Ensure all base columns exist even if df is empty
    for col in base_columns:
        if col not in df.columns:
            df[col] = ""

    df = df[base_columns]

    # Derived metrics
    df['views'] = pd.to_numeric(df['views'], errors='coerce').fillna(0.0)
    df['likes'] = pd.to_numeric(df['likes'], errors='coerce').fillna(0.0)
    df['comments'] = pd.to_numeric(df['comments'], errors='coerce').fillna(0.0)
    df['duration_seconds'] = pd.to_numeric(df['duration_seconds'], errors='coerce').fillna(0.0)

    # engagement_score
    df['engagement_score'] = df['likes'] + df['comments']

    # duration_minutes
    df['duration_minutes'] = df.apply(
        lambda r: 0 if r['duration_seconds'] <= 0 else r['duration_seconds'] / 60.0,
        axis=1
    )

    # views_per_minute
    df['views_per_minute'] = df.apply(
        lambda r: 0 if r['duration_minutes'] <= 0 else r['views'] / r['duration_minutes'],
        axis=1
    )

    # views_per_day
    def calc_views_per_day(row):
        days = row['days_since_published']
        if days in ["", None]:
            return row['views']
        try:
            d = float(days)
        except Exception:
            return row['views']
        if d <= 0:
            return row['views']
        return row['views'] / d

    df['views_per_day'] = df.apply(calc_views_per_day, axis=1)

    # engagement_per_view
    df['engagement_per_view'] = df.apply(
        lambda r: 0 if r['views'] <= 0 else r['engagement_score'] / r['views'],
        axis=1
    )

    # like_rate
    df['like_rate'] = df.apply(
        lambda r: 0 if r['views'] <= 0 else r['likes'] / r['views'],
        axis=1
    )

    # comment_rate
    df['comment_rate'] = df.apply(
        lambda r: 0 if r['views'] <= 0 else r['comments'] / r['views'],
        axis=1
    )

    # Final column order (base + derived)
    derived_columns = [
        "engagement_score",
        "duration_minutes",
        "views_per_minute",
        "views_per_day",
        "engagement_per_view",
        "like_rate",
        "comment_rate"
    ]

    all_columns = base_columns + derived_columns
    df = df[all_columns]

    # ✅ ALWAYS CREATE CSV (even if empty)
    os.makedirs("data", exist_ok=True)
    csv_path = "data/latest_20_livestreams_precise.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8")

    print("✅ CSV file saved:", csv_path)
    if not df.empty:
        print("\n📊 SAMPLE:")
        print(df[['title', 'published_at', 'published_time', 'days_since_published', 'teacher_name', 'views']].head(5).to_string())


if __name__ == "__main__":
    main()
