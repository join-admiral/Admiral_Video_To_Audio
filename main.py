import boto3
import os
import time
import subprocess
from dotenv import load_dotenv
import json

load_dotenv()

s3 = boto3.client("s3")

bucket_name = "admiral-vc-automation"
video_prefix = "01. Video/"
audio_prefix = "02. Audio/"

processed_files = set()

# ---- Helpers ----

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

def detect_format(file_path):
    try:
        output = subprocess.check_output([
            'ffprobe',
            '-v', 'error',
            '-show_entries', 'format=format_name',
            '-of', 'json',
            file_path
        ])
        data = json.loads(output)
        return data['format']['format_name']
    except subprocess.CalledProcessError:
        return None

def validate_video(file_path):
    fmt = detect_format(file_path)
    if fmt:
        print(f"✅ Detected video format: {fmt}")
        return True
    else:
        print("❌ Could not detect valid video format. Skipping.")
        return False

def convert_to_mp3(video_path):
    base, _ = os.path.splitext(video_path)
    audio_path = base + ".mp3"
    if os.path.exists(audio_path):
        return audio_path

    cmd = [
        "ffmpeg",
        "-y",
        "-i", video_path,
        "-q:a", "0",
        "-map", "a",
        audio_path
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode == 0:
        print(f"✅ Converted: {audio_path}")
        return audio_path
    else:
        print(f"❌ Conversion failed:\n{result.stderr.decode()}")
        return None

def upload_audio(audio_path):
    if not os.path.exists(audio_path):
        print("❌ Audio file not found, skipping upload.")
        return

    filename = os.path.basename(audio_path)

    # Upload to primary audio folder
    audio_key = f"{audio_prefix}{filename}"
    s3.upload_file(audio_path, bucket_name, audio_key)
    print(f"🚀 Uploaded to S3: {audio_key}")

    # Upload to backup folder
    backup_key = f"08.Audio Backup/{filename}"
    s3.upload_file(audio_path, bucket_name, backup_key)
    print(f"📦 Backup uploaded to S3: {backup_key}")

# ---- Monitor Loop ----

def monitor_s3(interval=60):
    print("👀 Monitoring S3 for new videos... (Ctrl+C to stop)")
    try:
        while True:
            new_videos = list_new_videos()
            for key in new_videos:
                print(f"\n🆕 Found new video: {key}")
                video_path = download_video(key)

                if not validate_video(video_path):
                    print(f"⚠️ Skipping file due to invalid format: {video_path}")
                    continue

                audio_path = convert_to_mp3(video_path)
                if audio_path:
                    upload_audio(audio_path)
                    print(f"🎉 Done processing: {os.path.basename(video_path)}")
                else:
                    print(f"⚠️ Skipping upload because conversion failed: {video_path}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n🛑 Stopped.")

# ---- Start ----

if __name__ == "__main__":
    monitor_s3()
