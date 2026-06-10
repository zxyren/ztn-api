import os
import glob
import json
import shutil
import subprocess

FFMPEG  = shutil.which("ffmpeg")  or "ffmpeg"
FFPROBE = shutil.which("ffprobe") or "ffprobe"

def ffmpeg_dir():
    return os.path.dirname(FFMPEG) if os.path.isabs(FFMPEG) else None

def get_file_metadata(fp, item_format):
    if not os.path.exists(fp): return None
    size_bytes = os.path.getsize(fp)
    if size_bytes < 1024 * 1024: size_str = f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024: size_str = f"{size_bytes / (1024 * 1024):.1f} MB"
    else: size_str = f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"
    
    ext = os.path.splitext(fp)[1].upper().replace(".", "")
    if fp.endswith(".zip"): return f"ZIP \u00b7 {size_str}"

    try:
        cmd = [FFPROBE, "-v", "quiet", "-print_format", "json", "-show_format", "-show_streams", fp]
        out = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL)
        info = json.loads(out)
        
        streams = info.get("streams", [])
        fmt_info = info.get("format", {})
        
        video_stream = next((s for s in streams if s.get("codec_type") == "video"), None)
        audio_stream = next((s for s in streams if s.get("codec_type") == "audio"), None)
        
        def format_duration(d):
            if not d: return ""
            try:
                seconds = int(float(d))
                m, s = divmod(seconds, 60)
                h, m = divmod(m, 60)
                if h > 0: return f"{h}:{m:02d}:{s:02d}"
                return f"{m}:{s:02d}"
            except: return ""

        duration = format_duration(fmt_info.get("duration"))
        
        parts = []
        if item_format == "audio":
            if audio_stream:
                bit_rate = audio_stream.get("bit_rate") or fmt_info.get("bit_rate")
                if bit_rate: parts.append(f"{int(float(bit_rate)) // 1000} kbps")
            if duration: parts.append(duration)
            parts.append(ext)
            parts.append(size_str)
            
        elif item_format == "video":
            if video_stream:
                height = video_stream.get("height")
                width = video_stream.get("width")
                if height:
                    if width and width >= 3800: parts.append("4K Ultra HD")
                    else: parts.append(f"{height}p")
            if duration: parts.append(duration)
            if video_stream:
                codec = video_stream.get("codec_name", "").upper()
                if codec == "H264": codec = "H.264"
                if codec: parts.append(codec)
            elif ext: parts.append(ext)
            parts.append(size_str)
            
        elif item_format == "image":
            if video_stream:
                w, h = video_stream.get("width"), video_stream.get("height")
                if w and h: parts.append(f"{w}x{h}")
            parts.append(ext)
            parts.append(size_str)
            
        else:
            parts = [ext, size_str]
            
        return " \u00b7 ".join([p for p in parts if p])
    except Exception as e:
        return f"{ext} \u00b7 {size_str}"


# ── yt-dlp option builders ─────────────────────────────────────────────────

def build_video_opts(hook, out_dir):
    return {
        "outtmpl": os.path.join(out_dir, "%(title)s.%(ext)s"),
        "progress_hooks": [hook],
        "restrictfilenames": True, "windowsfilenames": True,
        "updatetime": False, "noverifyhttpscert": True,
        "retries": 10, "fragment_retries": 10,
        "socket_timeout": 15,
        "concurrent_fragment_downloads": 4,
        "ffmpeg_location": ffmpeg_dir(),
        "format": "bestvideo+bestaudio/best",
        "merge_output_format": "mp4",
        "postprocessors": [{"key": "FFmpegVideoConvertor", "preferedformat": "mp4"},
                           {"key": "FFmpegMetadata"}],
        "postprocessor_args": {
            "ffmpegvideoconvertor": ["-c:v", "copy", "-c:a", "aac", "-b:a", "192k"],
        },
        "http_headers": {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
    }
    

def build_audio_opts(hook, out_dir):
    return {
        "outtmpl": os.path.join(out_dir, "%(title)s.%(ext)s"),
        "progress_hooks": [hook],
        "restrictfilenames": True, "windowsfilenames": True,
        "updatetime": False, "noverifyhttpscert": True,
        "retries": 10, "fragment_retries": 10,
        "socket_timeout": 15,
        "ffmpeg_location": ffmpeg_dir(),
        "format": "bestaudio/best",
        "postprocessors": [{"key": "FFmpegExtractAudio",
                            "preferredcodec": "mp3", "preferredquality": "192"}],
        "postprocessor_args": {
            "ffmpegextractaudio": ["-vn", "-c:a", "libmp3lame", "-b:a", "192k"],
        },
        "http_headers": {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
    }


def find_output(out_dir, ext):
    cands = [f for f in glob.glob(os.path.join(out_dir, f"*.{ext}"))
             if not f.endswith(".part")]
    if not cands:
        cands = [f for f in glob.glob(os.path.join(out_dir, "*"))
                 if os.path.isfile(f) and not f.endswith(".part")]
    return max(cands, key=os.path.getmtime) if cands else None
