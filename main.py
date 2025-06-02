import boto3
import os
import time
import subprocess
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client("s3")


bucket_name = "admiral-vc-automation"
video_prefix = "01. Video/"
audio_prefix = "02. Audio/"

# ---- 2) STATE ----
processed_files = set()

# ---- 3) HELPERS ----

def list_new_videos():
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=video_prefix)
    new_files = []
    for obj in response.get("Contents", []):
        key = obj["Key"]
        if key.endswith("/") or not key.lower().endswith((".mp4", ".mov", ".mkv", ".avi", ".flv", ".wmv")):
            continue
        if key not in processed_files:
            new_files.append(key)
            processed_files.add(key)
    return new_files

def download_video(key):
    filename = os.path.basename(key)
    local_path = f"/tmp/{filename}"
    s3.download_file(bucket_name, key, local_path)
    print(f"📥 Downloaded: {filename}")
    return local_path

def convert_to_mp3(video_path):
    base, _ = os.path.splitext(video_path)
    audio_path = base + ".mp3"
    if not os.path.exists(audio_path):
        cmd = ["ffmpeg", "-y", "-i", video_path, "-q:a", "0", "-map", "a", audio_path]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode == 0:
            print(f"✅ Converted: {audio_path}")
        else:
            print(f"❌ Conversion failed: {result.stderr.decode()}")
    return audio_path

def upload_audio(audio_path):
    filename = os.path.basename(audio_path)
    s3_key = f"{audio_prefix}{filename}"
    s3.upload_file(audio_path, bucket_name, s3_key)
    print(f"🚀 Uploaded to S3: {s3_key}")

# ---- 4) MONITOR LOOP ----

def monitor_s3(interval=60):
    print("👀 Monitoring S3 for new videos... (Ctrl+C to stop)")
    try:
        while True:
            new_videos = list_new_videos()
            for key in new_videos:
                print(f"\n🆕 Found new video: {key}")
                video_path = download_video(key)
                audio_path = convert_to_mp3(video_path)
                upload_audio(audio_path)
                print(f"🎉 Done processing: {os.path.basename(video_path)}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n🛑 Stopped.")

# ---- 5) START MONITORING ----

if __name__ == "__main__":
    monitor_s3()
