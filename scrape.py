"""
YouTube Livestream Data Extractor - CSV Only (GitHub Actions Safe)
=================================================================
- Extracts latest livestream data
- Adds: teacher_name, live_status, published_date, likes, comments,
         days_since_published
- Always creates CSV:
    data/latest_20_livestreams_precise.csv
- NEVER crashes if no data is found
- NO Excel, NO openpyxl required
"""

import re
import json
import os
import requests
import pandas as pd
from datetime import datetime, timezone
import time
import warnings

warnings.filterwarnings('ignore')

# ================= CONFIG =================

CHANNEL_URL = "https://www.youtube.com/@teachingpariksha"
TARGET_LIVESTREAMS = 20
ASSUME_STREAMS_TAB_ALL_LIVE = True

# ================= SESSION =================

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.5',
})

print("✅ HTTP session configured")

# ================= HELPERS =================

def extract_json_from_html(html, var_name='ytInitialData'):
    pattern = rf'var {var_name}\s*=\s*(\{{.*?\}});'
    match = re.search(pattern, html, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except:
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
                except:
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


def extract_teacher_name(title):
    title = title.lower()

    teacher_map = {
        "danish": "Danish Sir",
        "deepali": "Deepali Ma'am",
        "isha": "Isha Ma'am",
        "kuldeep": "Kuldeep Sir",
        "kajal": "Kajal Ma'am",
        "mona": "Mona Ma'am",
        "pawan": "Pawan Sir",
        "narjis": "Narjis Ma'am",
        "sachin": "Sachin Sir",
        "abha": "Abha Ma'am"
    }

    for key, value in teacher_map.items():
        if key in title:
            return value

    return "Unknown"

# ================= SCRAPER =================

def fetch_channel_videos(url):
    tabs = [f"{url}/streams", f"{url}/videos", url]

    for tab_url in tabs:
        print("Trying:", tab_url)
        r = session.get(tab_url)
        yt_data = extract_json_from_html(r.text)
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
                    vid['_from_streams_tab'] = True
                    videos.append(vid)

            if videos:
                return videos

    return []


def extract_likes_comments(video_url):
    try:
        r = session.get(video_url)
        html = r.text

        likes_match = re.search(r'"label":"([\d,]+) likes"', html)
        comments_match = re.search(r'"commentCount":"(\d+)"', html)

        likes = parse_exact_count(likes_match.group(1)) if likes_match else 0
        comments = int(comments_match.group(1)) if comments_match else 0

        upload_match = re.search(r'"uploadDate":"([^"]+)"', html)
        if upload_match:
            published_dt = datetime.fromisoformat(upload_match.group(1).replace("Z", "+00:00"))
        else:
            published_dt = datetime.now(timezone.utc)

        days_since = (datetime.now(timezone.utc) - published_dt).days

        return likes, comments, published_dt.strftime("%Y-%m-%d"), days_since

    except:
        return 0, 0, "", ""


def main():
    videos_data = fetch_channel_videos(CHANNEL_URL)
    print("✅ Total videos extracted:", len(videos_data))

    livestream_data = []

    for video in videos_data:
        if len(livestream_data) >= TARGET_LIVESTREAMS:
            break

        video_id = safe_get(video, 'videoId')
        if not video_id:
            continue

        title_runs = safe_get(video, 'title', 'runs', default=[])
        title = "".join([r.get("text", "") for r in title_runs])

        view_text = safe_get(video, 'viewCountText', 'simpleText')
        views = parse_exact_count(view_text)

        len_text = safe_get(video, 'lengthText', 'simpleText')
        duration_sec = parse_duration_text(len_text)

        teacher_name = extract_teacher_name(title)

        video_url = f"https://www.youtube.com/watch?v={video_id}"
        likes, comments, published_at, days_since = extract_likes_comments(video_url)

        livestream_data.append({
            "video_id": video_id,
            "title": title,
            "teacher_name": teacher_name,
            "live_status": "was_live",
            "published_at": published_at,
            "days_since_published": days_since,
            "views": views,
            "likes": likes,
            "comments": comments,
            "duration_seconds": duration_sec,
            "url": video_url
        })

        time.sleep(0.3)

    print("✅ Final livestream rows:", len(livestream_data))

    # ✅ ALWAYS CREATE CSV (even if empty)
    df = pd.DataFrame(livestream_data)

    os.makedirs("data", exist_ok=True)
    csv_path = "data/latest_20_livestreams_precise.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8")

    print("✅ CSV file saved:", csv_path)


if __name__ == "__main__":
    main()
