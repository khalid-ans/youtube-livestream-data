"""
YouTube Livestream Data Extractor - Selenium DOM + CSV
=====================================================
- Extracts latest livestream data from a channel
- Skips scheduled/upcoming streams
- Uses Selenium (headless Chrome) to:
    • Read likes from the like button (aria-label / text)
    • Read comments from the comments header ("X Comments")
- Adds:
    teacher_name, live_status, published_at, published_time,
    days_since_published, likes, comments
- Teacher name:
    1) Direct name in title (Danish, Isha, etc.)
    2) Else subject-based mapping (get_teacher)
- Derived metrics:
    engagement_score, duration_minutes, views_per_minute, views_per_day,
    engagement_per_view, like_rate, comment_rate
- Always creates CSV:
    data/latest_20_livestreams_precise.csv
"""

import os
import re
import json
import time
import warnings
from datetime import datetime, timezone, date, timedelta

import pandas as pd
import requests

warnings.filterwarnings("ignore")

# ================ CONFIG ================

CHANNEL_URL = "https://www.youtube.com/@teachingpariksha"
TARGET_LIVESTREAMS = 20

# ================ HTTP SESSION (channel pages) ================

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.5",
})
print("✅ HTTP session configured")

# ================ SELENIUM (watch pages) ================

driver = None
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException

    def init_driver():
        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1280,720")
        return webdriver.Chrome(options=opts)

    try:
        driver = init_driver()
        print("✅ Selenium Chrome driver initialized")
    except Exception as e:
        driver = None
        print("⚠️ Selenium init failed, falling back to requests-only:", e)

except ImportError:
    driver = None
    print("⚠️ selenium not installed, using requests-only mode (likes/comments may be 0)")

# ================ GENERIC HELPERS ================

def extract_json_from_html(html, var_name="ytInitialData"):
    pattern = rf"var {var_name}\s*=\s*(\{{.*?\}});"
    match = re.search(pattern, html, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except Exception:
            pass

    idx = html.find(var_name)
    if idx == -1:
        return None

    start = html.find("{", idx)
    if start == -1:
        return None

    depth = 0
    for i in range(start, len(html)):
        if html[i] == "{":
            depth += 1
        elif html[i] == "}":
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

# ================ TEACHER MAPPING ================

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
    "pooja": "Pooja Ma'am",
}

def detect_teacher_by_name(text: str) -> str:
    if not text:
        return "Unknown"
    low = text.lower()

    for key, value in TEACHER_MAP_DIRECT.items():
        if key in low:
            return value

    m = re.search(r"([A-Za-z]+)\s+(sir|ma[\'a]?am)", low)
    if m:
        name = m.group(1).lower()
        if name in TEACHER_MAP_DIRECT:
            return TEACHER_MAP_DIRECT[name]

    return "Unknown"


def get_teacher(t: str) -> str:
    t = t.lower()

    # EVS & Science
    if " evs" in t or "environmental studies" in t or "environment studies" in t:
        return "Mona Ma'am"
    if "science" in t and "social" not in t:
        return "Kuldeep Sir"

    # Languages
    if " hindi" in t:
        return "Isha Ma'am"
    if " english" in t:
        return "Pooja Ma'am"

    # Reasoning & Computer
    if " reasoning" in t or "logical" in t or "mental ability" in t:
        return "Kajal Ma'am"
    if " computer" in t:
        return "Kajal Ma'am"

    # Maths
    if " maths" in t or " math " in t or "mathematics" in t or "numerical" in t or "quant" in t:
        return "Pawan Sir"

    # SST / GK / CDP
    if " cdp" in t or "child development" in t:
        return "Danish Sir"
    if " gk" in t or "general knowledge" in t or "current affairs" in t or " gs" in t:
        return "Danish Sir"
    if " sst " in t or "social science" in t or "social studies" in t:
        return "Danish Sir"

    return "Unknown"


def extract_teacher_name_from_title(title: str) -> str:
    name = detect_teacher_by_name(title or "")
    if name != "Unknown":
        return name
    return get_teacher(title or "")

# ================ DATE HELPERS ================

def parse_relative_published(text: str):
    if not text:
        return None, None

    t = text.lower()
    m = re.search(r"(\d+)", t)
    if not m:
        return None, None

    n = int(m.group(1))
    days_offset = 0

    if "minute" in t or "min" in t or "hour" in t or "hr" in t:
        days_offset = 0
    elif "day" in t:
        days_offset = n
    elif "week" in t:
        days_offset = n * 7
    elif "month" in t:
        days_offset = n * 30
    elif "year" in t:
        days_offset = n * 365
    else:
        days_offset = n

    today = date.today()
    pub_date = today - timedelta(days=days_offset)
    return pub_date.strftime("%d-%m-%Y"), days_offset

# ================ FILTER UPCOMING ================

def is_scheduled_or_upcoming(video):
    if safe_get(video, "upcomingEventData") is not None:
        return True

    badges = safe_get(video, "badges", default=[])
    for b in badges:
        label = (safe_get(b, "metadataBadgeRenderer", "label", default="") or "").lower()
        if "upcoming" in label or "scheduled" in label:
            return True

    overlays = safe_get(video, "thumbnailOverlays", default=[])
    for o in overlays:
        style = safe_get(o, "thumbnailOverlayTimeStatusRenderer", "style", default="")
        if isinstance(style, str) and "upcoming" in style.lower():
            return True
        text_label = (safe_get(o, "thumbnailOverlayTimeStatusRenderer", "text", "simpleText", default="") or "").lower()
        if "upcoming" in text_label or "scheduled" in text_label:
            return True

    vc_text = safe_get(video, "viewCountText", "simpleText", default="") or ""
    if isinstance(vc_text, str):
        vc_low = vc_text.lower()
        if "waiting" in vc_low or "scheduled for" in vc_low:
            return True

    return False

# ================ CHANNEL SCRAPER ================

def fetch_channel_videos(url):
    tabs = [f"{url}/streams", f"{url}/videos", url]
    for tab_url in tabs:
        print("Trying:", tab_url)
        r = session.get(tab_url, timeout=30)
        if r.status_code != 200:
            continue

        yt_data = extract_json_from_html(r.text, "ytInitialData")
        if not yt_data:
            continue

        tab_data = safe_get(yt_data, "contents", "twoColumnBrowseResultsRenderer", "tabs", default=[])
        videos = []

        for tab in tab_data:
            content = safe_get(tab, "tabRenderer", "content", default={})
            rich = safe_get(content, "richGridRenderer", "contents", default=[])

            for item in rich:
                vid = safe_get(item, "richItemRenderer", "content", "videoRenderer")
                if vid:
                    videos.append(vid)

        if videos:
            return videos

    return []


# ================ WATCH PAGE SCRAPER (DOM) ================

def extract_video_details(video_url, approx_published_text=None):
    """
    Uses Selenium DOM (if available) to read:
      - likes from like button aria-label/text
      - comments from comments header
    + date from uploadDate or approximate text.
    """
    # --- Get rendered page ---
    html = ""
    if driver is not None:
        try:
            driver.get(video_url)

            wait = WebDriverWait(driver, 15)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(2)  # allow counters to render

            # Try to read likes from DOM
            likes = 0

            # Common selectors for like button
            like_selectors = [
                "ytd-toggle-button-renderer[is-icon-button] button",
                "ytd-segmented-like-dislike-button-renderer button[aria-pressed]",
                "ytd-toggle-button-renderer[is-icon-button] #button",
                "button[aria-label*='like this video']",
                "button[aria-label*='likes']",
            ]
            for sel in like_selectors:
                try:
                    el = driver.find_element(By.CSS_SELECTOR, sel)
                    aria = el.get_attribute("aria-label") or ""
                    txt = el.text or ""
                    likes_candidate = parse_exact_count(aria) or parse_exact_count(txt)
                    if likes_candidate > 0:
                        likes = likes_candidate
                        break
                except Exception:
                    continue

            # Scroll to comments area
            try:
                driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight * 0.7);")
                time.sleep(2)
            except Exception:
                pass

            # Comments header: "#count > span"
            comments = 0
            try:
                comments_el = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "ytd-comments-header-renderer #count span"))
                )
                comments_text = comments_el.text or ""
                comments = parse_exact_count(comments_text)
            except TimeoutException:
                comments = 0

            # Get HTML for uploadDate parsing
            html = driver.page_source

        except Exception as e:
            print(f"⚠️ Selenium error for {video_url}: {e}")
            likes = 0
            comments = 0
    else:
        # No Selenium: fall back to requests-only
        likes = 0
        comments = 0
        try:
            r = session.get(video_url, timeout=30)
            html = r.text
        except Exception:
            html = ""

    # --- Date logic (same as before) ---
    published_at = ""
    published_time = ""
    days = None

    if html:
        upload_match = re.search(r'"uploadDate":\s*"([^"]+)"', html)
        if upload_match:
            try:
                raw = upload_match.group(1)  # 2025-02-10T15:30:00Z or 2025-02-10
                iso = raw.replace("Z", "+00:00")
                if "T" in iso:
                    dt = datetime.fromisoformat(iso)
                else:
                    dt = datetime.strptime(iso, "%Y-%m-%d")
                published_at = dt.strftime("%d-%m-%Y")
                published_time = dt.strftime("%H:%M:%S")
                days = (date.today() - dt.date()).days
            except Exception:
                published_at = ""
                published_time = ""
                days = None

    if not published_at and approx_published_text:
        approx_date, approx_days = parse_relative_published(approx_published_text)
        if approx_date is not None:
            published_at = approx_date
            published_time = "00:00:00"
            days = approx_days

    if not published_at:
        now = datetime.now(timezone.utc)
        published_at = now.strftime("%d-%m-%Y")
        published_time = now.strftime("%H:%M:%S")
        days = 0

    return likes, comments, published_at, published_time, days


# ================ MAIN ================

def main():
    videos_data = fetch_channel_videos(CHANNEL_URL)
    print("✅ Total videos extracted from channel tabs:", len(videos_data))

    livestream_data = []

    for video in videos_data:
        if len(livestream_data) >= TARGET_LIVESTREAMS:
            break

        if is_scheduled_or_upcoming(video):
            continue

        video_id = safe_get(video, "videoId")
        if not video_id:
            continue

        title_runs = safe_get(video, "title", "runs", default=[])
        title = "".join([r.get("text", "") for r in title_runs])

        view_text = safe_get(video, "viewCountText", "simpleText", default="")
        views = parse_exact_count(view_text)

        len_text = safe_get(video, "lengthText", "simpleText", default="")
        duration_sec = parse_duration_text(len_text)

        published_tile_text = safe_get(video, "publishedTimeText", "simpleText", default="")

        video_url = f"https://www.youtube.com/watch?v={video_id}"
        likes, comments, published_at, published_time, days_since_pub = extract_video_details(
            video_url,
            approx_published_text=published_tile_text,
        )

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
            "url": video_url,
        })

        time.sleep(0.7)  # be gentle with YouTube

    print("✅ Final livestream rows (excluding scheduled/upcoming):", len(livestream_data))

    base_columns = [
        "video_id", "title", "teacher_name",
        "live_status", "published_at", "published_time",
        "days_since_published", "views", "likes", "comments",
        "duration_seconds", "url",
    ]

    df = pd.DataFrame(livestream_data)
    for col in base_columns:
        if col not in df.columns:
            df[col] = ""
    df = df[base_columns]

    # Derived metrics
    df["views"] = pd.to_numeric(df["views"], errors="coerce").fillna(0.0)
    df["likes"] = pd.to_numeric(df["likes"], errors="coerce").fillna(0.0)
    df["comments"] = pd.to_numeric(df["comments"], errors="coerce").fillna(0.0)
    df["duration_seconds"] = pd.to_numeric(df["duration_seconds"], errors="coerce").fillna(0.0)

    df["engagement_score"] = df["likes"] + df["comments"]
    df["duration_minutes"] = df.apply(
        lambda r: 0 if r["duration_seconds"] <= 0 else r["duration_seconds"] / 60.0,
        axis=1,
    )
    df["views_per_minute"] = df.apply(
        lambda r: 0 if r["duration_minutes"] <= 0 else r["views"] / r["duration_minutes"],
        axis=1,
    )

    def calc_views_per_day(row):
        days = row["days_since_published"]
        if days in ["", None]:
            return row["views"]
        try:
            d = float(days)
        except Exception:
            return row["views"]
        if d <= 0:
            return row["views"]
        return row["views"] / d

    df["views_per_day"] = df.apply(calc_views_per_day, axis=1)
    df["engagement_per_view"] = df.apply(
        lambda r: 0 if r["views"] <= 0 else r["engagement_score"] / r["views"],
        axis=1,
    )
    df["like_rate"] = df.apply(
        lambda r: 0 if r["views"] <= 0 else r["likes"] / r["views"],
        axis=1,
    )
    df["comment_rate"] = df.apply(
        lambda r: 0 if r["views"] <= 0 else r["comments"] / r["views"],
        axis=1,
    )

    derived_columns = [
        "engagement_score",
        "duration_minutes",
        "views_per_minute",
        "views_per_day",
        "engagement_per_view",
        "like_rate",
        "comment_rate",
    ]
    df = df[base_columns + derived_columns]

    os.makedirs("data", exist_ok=True)
    csv_path = "data/latest_20_livestreams_precise.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8")
    print("✅ CSV file saved:", csv_path)

    if not df.empty:
        print("\n📊 SAMPLE:")
        print(
            df[
                ["title", "published_at", "days_since_published", "views", "likes", "comments"]
            ]
            .head(5)
            .to_string()
        )

    if driver is not None:
        driver.quit()
        print("🚪 Closed Selenium driver")


if __name__ == "__main__":
    main()
