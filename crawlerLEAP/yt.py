import os
import requests
from youtube_transcript_api import YouTubeTranscriptApi
import json

# Replace these values as needed
API_KEY = "AIzaSyBIKHx6j2-XFHLoPqkMSIBiAVybFlUPPXI"
CHANNEL_ID = "UChCqE3MptBJlubiJMFxVnlw"

# Optionally create a folder to hold individual JSON files
OUTPUT_FOLDER = "youtube_videos_json"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# This list will collect all video objects for the final "YouTube_Data.json"
all_videos_data = []

# Get videos from the channel
search_url = (
    f"https://www.googleapis.com/youtube/v3/search?key={API_KEY}"
    f"&channelId={CHANNEL_ID}&part=snippet&type=video&maxResults=100"
)
response = requests.get(search_url)
videos = response.json().get("items", [])

# Process each video
for video in videos:
    video_id = video["id"]["videoId"]
    title = video["snippet"]["title"]
    url = f"https://www.youtube.com/watch?v={video_id}"
    published_at = video["snippet"]["publishedAt"]

    # Attempt to fetch the transcript
    try:
        transcript_data = YouTubeTranscriptApi.get_transcript(video_id)
        transcript = "\n".join(
            f"{entry['start']}s: {entry['text']}" for entry in transcript_data
        )
    except:
        transcript = "No transcript available"

    # Construct one Weaviate-style JSON object for this video
    video_json = {
        "class": "YouTubeVideo",
        "title": title,
        "videoId": video_id,
        "url": url,
        "publishedAt": published_at,
        "transcript": transcript
    }

    # 1) Write out a single JSON file per video, named by video_id
    filename = os.path.join(OUTPUT_FOLDER, f"{video_id}.json")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(video_json, f, indent=4, ensure_ascii=False)
    print(f"✅ Saved {title} ({video_id}) to {filename}")

    # 2) Add to the master list
    all_videos_data.append(video_json)

# Finally, write the *combined* data to "YouTube_Data.json"
combined_filename = "YouTube_Data.json"
with open(combined_filename, "w", encoding="utf-8") as f:
    json.dump(all_videos_data, f, indent=4, ensure_ascii=False)

print(f"\nAll videos also saved together in {combined_filename}")
print("Done saving individual JSON files for each video plus a combined JSON.")
