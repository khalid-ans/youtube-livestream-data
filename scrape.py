"""
YouTube Livestream Data Extractor - Precise Teacher & Real Counts Edition
=========================================================================
Extracts the latest 20 livestreams with:
1. Precise Teacher Name detection (using your specific list).
2. REAL Like & Comment counts (from accessibility data).
3. Timestamps and engagement metrics.

This version:
- Is a plain Python script (no Colab !pip).
- Saves both:
    1) Excel: latest_20_livestreams_precise.xlsx
    2) CSV:   data/latest_20_livestreams_precise.csv
"""

import re
import json
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timezone, timedelta
import time
import warnings

warnings.filterwarnings('ignore')

# ========================================================================
# CONFIGURATION
# ========================================================================

CHANNEL_URL = "https://www.youtube.com/@teachingpariksha"

TARGET_LIVESTREAMS = 20   # Extract exactly 20 livestreams
MAX_RETRIES = 3           # Retry failed requests
ASSUME_STREAMS_TAB_ALL_LIVE = True  # If True, treat all /streams videos as livestreams

print(f"🎯 Target Channel: {CHANNEL_URL}")
print(f"📊 Will extract {TARGET_LIVESTREAMS} livestreams")
print(f"🔧 Streams tab mode: {'ALL videos treated as livestreams' if ASSUME_STREAMS_TAB_ALL_LIVE else 'Badge detection only'}\n")

# ========================================================================
# HTTP SESSION
# ========================================================================

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
})

print("✅ HTTP session configured\n")

# ========================================================================
# HELPER FUNCTIONS
# ========================================================================

def extract_json_from_html(html_content, var_name='ytInitialData'):
    """
    Extract JSON data embedded in YouTube HTML.

    First tries specific patterns:
        var ytInitialData = {...};
        window["ytInitialData"] = {...};
    Then falls back to a generic "find object after ytInitialData" approach.
    """
    # Pattern 1: var ytInitialData = {...};
    pattern = rf'var {var_name}\s*=\s*(\{{.*?\}});'
    match = re.search(pattern, html_content, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except Exception:
            pass

    # Pattern 2: window["ytInitialData"] = {...};
    pattern2 = rf'window\["{var_name}"\]\s*=\s*(\{{.*?\}});'
    match2 = re.search(pattern2, html_content, re.DOTALL)
    if match2:
        try:
            return json.loads(match2.group(1))
        except Exception:
            pass

    # Fallback: generic search around first occurrence of var_name
    idx = html_content.find(var_name)
    if idx == -1:
        return None

    start = html_content.find('{', idx)
    if start == -1:
        return None

    depth = 0
    in_string = False
    prev_char = ''
    for i in range(start, len(html_content)):
        ch = html_content[i]

        if ch == '"' and prev_char != '\\':
            in_string = not in_string

        if not in_string:
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    json_text = html_content[start:i+1]
                    try:
                        return json.loads(json_text)
                    except Exception:
                        return None

        prev_char = ch

    return None

def extract_teacher_name(title, description, uploader):
    """
    PRECISE METHOD: Checks for specific teachers first, then fallbacks.
    """
    t_text = title if title else ""
    d_text = description if description else ""
    
    # 1. PRECISE LIST CHECK
    if 'Danish Sir' in t_text or 'Danish Sir' in d_text:
        return 'Danish Sir'
    elif "Deepali Ma'am" in t_text or "Deepali Ma'am" in d_text or "Deepali Maam" in t_text:
        return 'Deepali Maam'
    elif "Isha Ma'am" in t_text or "Isha Ma'am" in d_text or "Isha Maam" in t_text:
        return 'Isha Maam'
    elif 'Kuldeep Sir' in t_text or 'Kuldeep Sir' in d_text:
        return 'Kuldeep Sir'
    elif "Kajal Ma'am" in t_text or "Kajal Ma'am" in d_text or "Kajal Maam" in t_text:
        return 'Kajal Maam'
    elif "Mona Ma'am" in t_text or "Mona Ma'am" in d_text or "Mona Maam" in t_text:
        return 'Mona Maam'
    elif 'Pawan Sir' in t_text or 'Pawan Sir' in d_text:
        return 'Pawan Sir'
    elif "Narjis Ma'am" in t_text or "Narjis Ma'am" in d_text or "Narjis Maam" in t_text:
        return 'Narjis Maam'
    elif 'Sachin Sir' in t_text or 'Sachin Sir' in d_text:
        return 'Sachin Sir'
    elif "Abha Ma'am" in t_text or "Abha Ma'am" in d_text or "Abha Maam" in t_text:
        return 'Abha Maam'

    # 2. Fallback: " | Name" or "- Name" at end of title
    name_at_end_pattern = r'(?:\||-)\s*([A-Za-z\.]+(?:\s+[A-Za-z\.]+){0,2})\s*$'
    match_end = re.search(name_at_end_pattern, t_text.strip())
    
    if match_end:
        candidate = match_end.group(1).strip()
        blacklist = ['Live', 'Hindi', 'English', 'Science', 'Maths', 'Marathon', 'Class', 'Paper', 'Teaching Pariksha']
        if len(candidate) > 2 and not any(b.lower() in candidate.lower() for b in blacklist):
            return candidate

    # 3. Last Resort: Uploader
    return uploader

def parse_exact_count(text):
    """Extracts exact integers from strings like '1,234 likes'."""
    if not text:
        return 0
    clean = re.sub(r'[^\d]', '', str(text))
    try:
        return int(clean)
    except Exception:
        return 0

def parse_duration_text(duration_text):
    """Parse '1:23:45' to seconds."""
    if not duration_text or duration_text == 'LIVE':
        return 0
    parts = [int(p) for p in duration_text.strip().split(':') if p.isdigit()]
    if len(parts) == 3:
        return parts[0] * 3600 + parts[1] * 60 + parts[2]
    elif len(parts) == 2:
        return parts[0] * 60 + parts[1]
    return parts[0] if parts else 0

def safe_get(data, *keys, default=None):
    """Safely navigate nested dictionary."""
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key, default)
        elif isinstance(data, list) and isinstance(key, int) and len(data) > key:
            data = data[key]
        else:
            return default
    return data if data is not None else default

print("✅ Helper functions loaded\n")

# ========================================================================
# MAIN SCRAPING LOGIC
# ========================================================================

def fetch_channel_videos(channel_url):
    print("🔍 Fetching channel page...")
    tab_urls = [f"{channel_url}/streams", f"{channel_url}/videos", channel_url]
    
    for tab_idx, tab_url in enumerate(tab_urls):
        try:
            print(f"  📥 Trying: {tab_url}")
            response = session.get(tab_url, timeout=30)
            if response.status_code != 200:
                continue
            
            yt_data = extract_json_from_html(response.text, 'ytInitialData')
            if not yt_data:
                continue
            
            tabs = safe_get(yt_data, 'contents', 'twoColumnBrowseResultsRenderer', 'tabs', default=[])
            videos = []
            from_streams = '/streams' in tab_url or tab_idx == 0
            
            for tab in tabs:
                content = safe_get(tab, 'tabRenderer', 'content', default={})
                # Rich Grid
                rich_grid = safe_get(content, 'richGridRenderer', 'contents', default=[])
                for item in rich_grid:
                    vid = safe_get(item, 'richItemRenderer', 'content', 'videoRenderer')
                    if vid:
                        vid['_from_streams_tab'] = from_streams
                        videos.append(vid)
                
                # Section List
                section_list = safe_get(content, 'sectionListRenderer', 'contents', default=[])
                for section in section_list:
                    items = safe_get(section, 'itemSectionRenderer', 'contents', default=[])
                    for item in items:
                        vid = safe_get(item, 'gridVideoRenderer') or safe_get(item, 'videoRenderer')
                        if vid:
                            vid['_from_streams_tab'] = from_streams
                            videos.append(vid)
            
            if videos:
                print(f"    ✅ Found {len(videos)} videos")
                return videos
        except Exception as e:
            print(f"    ❌ Error: {str(e)[:60]}")
            continue
    return []

def fetch_detailed_metadata(livestream_data):
    print("\n📥 Fetching detailed metadata (Real Likes, Comments & Teacher Names)...")
    print("⏳ This will take 1-2 minutes...\n")

    for idx, stream in enumerate(livestream_data, 1):
        try:
            response = session.get(stream['url'], timeout=30)
            if response.status_code != 200:
                continue
            html = response.text
            
            # Teacher name
            stream['teacher_name'] = extract_teacher_name(
                stream['title'],
                stream['description'],
                stream['uploader']
            )
            
            # Timestamp (uploadDate)
            upload_match = re.search(r'"uploadDate":\s*"([^"]+)"', html)
            if upload_match:
                try:
                    dt = datetime.fromisoformat(upload_match.group(1).replace('Z', '+00:00'))
                    stream['published_at'] = dt.strftime('%d-%m-%Y')
                    stream['published_time'] = dt.strftime('%H:%M:%S')
                    stream['actual_iso'] = dt.isoformat()
                except Exception:
                    pass
            
            if 'published_at' not in stream:
                now = datetime.now()
                stream['published_at'] = now.strftime('%d-%m-%Y')
                stream['published_time'] = '00:00:00'

            # Real likes & comments
            yt_data = extract_json_from_html(html, 'ytInitialData')
            
            # Likes
            found_likes = False
            if yt_data:
                results = safe_get(yt_data, 'contents', 'twoColumnWatchNextResults', 'results', 'results', 'contents', default=[])
                for item in results:
                    primary = item.get('videoPrimaryInfoRenderer')
                    if primary:
                        buttons = safe_get(primary, 'videoActions', 'menuRenderer', 'topLevelButtons', default=[])
                        for btn in buttons:
                            like_renderer = safe_get(btn, 'segmentedLikeDislikeButtonRenderer', 'likeButton', 'toggleButtonRenderer') or \
                                            safe_get(btn, 'toggleButtonRenderer')
                            if like_renderer:
                                access_label = safe_get(like_renderer, 'defaultText', 'accessibility', 'accessibilityData', 'label')
                                if access_label:
                                    stream['likes'] = parse_exact_count(access_label)
                                    found_likes = True
                                    break
                        if found_likes:
                            break
            
            if not found_likes:
                like_match = re.search(r'"label":"([\d,]+) likes"', html)
                if like_match:
                    stream['likes'] = parse_exact_count(like_match.group(1))

            # Comments
            found_comments = False
            comment_match = re.search(r'"text":"([\d,]+) Comments"', html)
            if comment_match:
                stream['comments'] = parse_exact_count(comment_match.group(1))
                found_comments = True
            
            if not found_comments:
                raw_count = re.search(r'"commentCount":"(\d+)"', html)
                if raw_count:
                    stream['comments'] = int(raw_count.group(1))

            if idx % 5 == 0:
                print(f"  ✓ Processed {idx}/{len(livestream_data)} videos...")
                time.sleep(0.5)
                
        except Exception as e:
            print(f"  ⚠️ Error on {stream['video_id']}: {str(e)[:50]}")
            continue

    print("\n✅ Detailed metadata fetched successfully")

def main():
    # Step 1: Fetch list of videos
    videos_data = fetch_channel_videos(CHANNEL_URL)
    print(f"\n✅ Total videos extracted: {len(videos_data)}\n")

    # Step 2: Filter livestreams and basic metadata
    print("🔍 Parsing initial metadata...")
    livestream_data = []

    for idx, video in enumerate(videos_data, 1):
        if len(livestream_data) >= TARGET_LIVESTREAMS:
            break
        
        video_id = safe_get(video, 'videoId')
        if not video_id:
            continue
        
        is_stream_tab = video.get('_from_streams_tab', False)
        badges = safe_get(video, 'badges', default=[])
        overlays = safe_get(video, 'thumbnailOverlays', default=[])
        
        is_live_status = 'none'
        if ASSUME_STREAMS_TAB_ALL_LIVE and is_stream_tab:
            is_live_status = 'was_live'
            
        for badge in badges:
            label = safe_get(badge, 'metadataBadgeRenderer', 'label', default='').lower()
            if 'live' in label:
                is_live_status = 'is_live'
        
        if is_live_status == 'none':
            continue

        title_runs = safe_get(video, 'title', 'runs', default=[])
        title = ''.join([r.get('text', '') for r in title_runs])
        
        desc_snippet = safe_get(video, 'descriptionSnippet', 'runs', default=[])
        description = ''.join([r.get('text', '') for r in desc_snippet])
        
        uploader_runs = safe_get(video, 'ownerText', 'runs', default=[])
        uploader = ''.join([r.get('text', '') for r in uploader_runs])
        
        view_text = safe_get(video, 'viewCountText', 'simpleText') or safe_get(video, 'viewCountText', 'runs', 0, 'text')
        views = parse_exact_count(view_text) if view_text else 0
        
        len_text = safe_get(video, 'lengthText', 'simpleText')
        if not len_text:
            for o in overlays:
                len_text = safe_get(o, 'thumbnailOverlayTimeStatusRenderer', 'text', 'simpleText')
                if len_text:
                    break
        duration_sec = parse_duration_text(len_text)

        published_text = safe_get(video, 'publishedTimeText', 'simpleText')
        
        livestream_data.append({
            'video_id': video_id,
            'title': title,
            'description': description,
            'uploader': uploader,
            'url': f"https://www.youtube.com/watch?v={video_id}",
            'live_status': is_live_status,
            'duration_seconds': duration_sec,
            'views': views,
            'published_text_approx': published_text,
            'likes': 0,
            'comments': 0,
            'teacher_name': 'Unknown'
        })

    print(f"✅ Filtered {len(livestream_data)} livestreams to analyze")

    if not livestream_data:
        print("\n❌ No data found.")
        return

    # Step 3: Detailed metadata
    fetch_detailed_metadata(livestream_data)

    # Step 4: Build DataFrame and save to Excel + CSV
    columns = [
        'video_id', 'title', 'teacher_name', 'published_at', 'published_time', 
        'views', 'likes', 'comments', 'duration_seconds', 'url'
    ]

    df = pd.DataFrame(livestream_data)
    
    # Ensure all columns exist
    for col in columns:
        if col not in df.columns:
            df[col] = ''
        
    df = df[columns]
    
    # Calculate Engagement
    df['engagement_score'] = df['likes'] + df['comments']
    
    # Save Excel (optional, for manual use)
    excel_file = 'latest_20_livestreams_precise.xlsx'
    df.to_excel(excel_file, index=False)
    print(f"\n✅ Excel file saved: {excel_file}")

    # Save CSV (for GitHub Actions / Google Sheets pipeline)
    os.makedirs("data", exist_ok=True)
    csv_path = os.path.join("data", "latest_20_livestreams_precise.csv")
    df.to_csv(csv_path, index=False, encoding="utf-8")
    print(f"✅ CSV file saved: {csv_path}")

    # Small summary in console
    print(f"\n📊 SUMMARY:")
    print(df[['title', 'teacher_name', 'views', 'likes', 'comments']].head(10).to_string())


if __name__ == "__main__":
    main()
