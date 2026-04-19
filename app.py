import os
import threading
import time
import tempfile
import glob
import uuid
from queue import Queue
from flask import Flask, request, jsonify, send_file, Response, session
from flask_cors import CORS
from yt_dlp import YoutubeDL
import shutil
import subprocess
import json
import logging

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(24))
CORS(app)  # Enable CORS for React frontend

# Disable Flask's request logging for the SSE endpoint
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# Use temp directory instead of project folder - files will be served to users and cleaned up
DOWNLOAD_FOLDER = tempfile.mkdtemp(prefix="video_downloader_")
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
print(f"✓ Temporary download folder: {DOWNLOAD_FOLDER}")

# Per-session downloads tracking
user_downloads = {}  # {session_id: {"total": 0, "completed": 0, ...}}
user_next_ids = {}   # {session_id: next_id}
user_task_queues = {}  # {session_id: Queue}
session_lock = threading.Lock()

# SSE client management - map session_id to list of queues
sse_clients = {}  # {session_id: [client_queues]}
sse_lock = threading.Lock()

def get_session_id():
    """Get or create session ID for current request"""
    if 'session_id' not in session:
        session['session_id'] = str(uuid.uuid4())
    return session['session_id']

def ensure_session_initialized(session_id):
    """Ensure session has download tracking initialized"""
    with session_lock:
        if session_id not in user_downloads:
            user_downloads[session_id] = {
                "total": 0,
                "completed": 0,
                "downloading": 0,
                "queue": []
            }
            user_next_ids[session_id] = 1
            user_task_queues[session_id] = Queue()
            sse_clients[session_id] = []

def detect_ffmpeg():
    """Check if ffmpeg exists in PATH"""
    ffmpeg_path = shutil.which("ffmpeg")
    if ffmpeg_path:
        print(f"✓ FFmpeg detected at {ffmpeg_path}")
        return ffmpeg_path
    
    # fallback: try running ffmpeg
    try:
        result = subprocess.run(
            ["ffmpeg", "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if result.returncode == 0:
            return "ffmpeg"
    except Exception:
        pass

    print("⚠ FFmpeg not found!")
    return None

FFMPEG_PATH = detect_ffmpeg()
MAX_CONCURRENT = 1

def broadcast_update(session_id):
    """Send update to all connected SSE clients for this session"""
    with session_lock:
        data = json.dumps(user_downloads.get(session_id, {}))
    
    with sse_lock:
        clients = sse_clients.get(session_id, [])
        for client_queue in clients[:]:
            try:
                client_queue.put(data)
            except:
                clients.remove(client_queue)

def ydl_options(progress_cb):
    opts = {
        'outtmpl': os.path.join(DOWNLOAD_FOLDER, '%(title)s.%(ext)s'),
        'progress_hooks': [progress_cb],
        'restrictfilenames': True,
        'windowsfilenames': True,
        'updatetime': False,
        'noverifyhttpscert': True,
        'buffersize': 1024 * 64,
        'continuedl': True,
        '--no-check-certificate': True,
        'http_headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        },
    }
    
    if FFMPEG_PATH:
        opts['format'] = 'bestvideo+bestaudio/best'
        opts['merge_output_format'] = 'mp4'
    else:
        # Fallback to single format if FFmpeg not available
        print("⚠ FFmpeg not available - downloading single format only")
        opts['format'] = 'best'  # Download best single format (no merging needed)

    return opts


def download_one(item, session_id):
    downloaded_filename = None
    
    def progress_hook(d):
        if d['status'] == 'downloading':
            total = d.get('total_bytes') or d.get('total_bytes_estimate')
            if total:
                pct = d.get('downloaded_bytes', 0) * 100.0 / total
                with session_lock:
                    item['progress'] = max(0.0, min(100.0, pct))
                    item['status'] = 'Downloading'
                broadcast_update(session_id)  # Send update to this user only
        elif d['status'] == 'finished':
            with session_lock:
                item['progress'] = 100.0
                item['status'] = 'Merging'
            broadcast_update(session_id)  # Send update to this user only
            # Capture the filename when download finishes
            if 'filename' in d:
                nonlocal downloaded_filename
                downloaded_filename = d['filename']

    opts = ydl_options(progress_hook)
    try:
        with YoutubeDL(opts) as ydl:
            # Extract info first to get the expected filename
            info = ydl.extract_info(item['url'], download=False)
            expected_filename = ydl.prepare_filename(info)
            ydl.download([item['url']])
        
        # Use the expected filename, or find the most recently modified file as fallback
        if os.path.exists(expected_filename):
            downloaded_filename = expected_filename
        else:
            # Fallback: Get the most recently modified file in download folder
            files = glob.glob(os.path.join(DOWNLOAD_FOLDER, '*'))
            if files:
                downloaded_filename = max(files, key=os.path.getmtime)
        
        with session_lock:
            item['status'] = 'Completed'
            item['filename'] = os.path.basename(downloaded_filename) if downloaded_filename else None
            item['filepath'] = downloaded_filename if downloaded_filename else None
            user_downloads[session_id]['completed'] += 1
        broadcast_update(session_id)  # Send update to this user only
    except Exception as e:
        print(f"--- DOWNLOAD FAILED: {str(e)} ---")
        with session_lock:
            item['status'] = 'Error'
            item['error'] = str(e)
        broadcast_update(session_id)  # Send update to this user only

def worker_loop(session_id):
    task_queue = user_task_queues[session_id]
    while True:
        url = task_queue.get()
        if url is None:
            break
        with session_lock:
            user_downloads[session_id]['downloading'] += 1
            item = next((x for x in user_downloads[session_id]['queue'] if x['url']==url and x['status']=='Queued'), None)
            if item:
                item['status'] = 'Starting'
        broadcast_update(session_id)  # Send update to this user only
        if item:
            download_one(item, session_id)
        time.sleep(0.2)
        with session_lock:
            user_downloads[session_id]['downloading'] -= 1
        broadcast_update(session_id)  # Send update to this user only
        task_queue.task_done()

def start_workers():
    # Workers are started per-session on demand
    pass

# API Routes
@app.route("/api/queue", methods=["POST"])
def queue_download():
    session_id = get_session_id()
    ensure_session_initialized(session_id)
    
    data = request.get_json(force=True, silent=True) or {}
    urls = data.get("urls", [])
    with session_lock:
        for raw_url in urls:
            url = (raw_url or "").strip()
            if not url:
                continue
            item = {"id": user_next_ids[session_id], "url": url, "status": "Queued", "progress": 0.0}
            user_next_ids[session_id] += 1
            user_downloads[session_id]['queue'].append(item)
            user_downloads[session_id]['total'] += 1
            user_task_queues[session_id].put(url)
        
        # Start worker thread for this session if not already running
        if not hasattr(user_downloads[session_id], '_worker_started'):
            for _ in range(MAX_CONCURRENT):
                threading.Thread(target=worker_loop, args=(session_id,), daemon=True).start()
            user_downloads[session_id]['_worker_started'] = True
    
    broadcast_update(session_id)  # Send update to this user only
    return jsonify(user_downloads[session_id])

@app.route("/api/upload", methods=["POST"])
def upload_file():
    session_id = get_session_id()
    ensure_session_initialized(session_id)
    
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "No file"}), 400
    lines = [ln.strip() for ln in f.read().decode("utf-8", errors="ignore").splitlines() if ln.strip()]
    with session_lock:
        for url in lines:
            item = {"id": user_next_ids[session_id], "url": url, "status": "Queued", "progress": 0.0}
            user_next_ids[session_id] += 1
            user_downloads[session_id]['queue'].append(item)
            user_downloads[session_id]['total'] += 1
            user_task_queues[session_id].put(url)
        
        # Start worker thread for this session if not already running
        if not hasattr(user_downloads[session_id], '_worker_started'):
            for _ in range(MAX_CONCURRENT):
                threading.Thread(target=worker_loop, args=(session_id,), daemon=True).start()
            user_downloads[session_id]['_worker_started'] = True
    
    broadcast_update(session_id)  # Send update to this user only
    return jsonify(user_downloads[session_id])

@app.route("/api/status")
def status():
    session_id = get_session_id()
    ensure_session_initialized(session_id)
    return jsonify(user_downloads[session_id])

@app.route("/api/events")
def events():
    """Server-Sent Events endpoint for real-time updates - session isolated"""
    session_id = get_session_id()
    ensure_session_initialized(session_id)
    
    def event_stream():
        client_queue = Queue()
        with sse_lock:
            sse_clients[session_id].append(client_queue)
        
        try:
            # Send initial state for this session
            with session_lock:
                data = json.dumps(user_downloads[session_id])
            yield f"data: {data}\n\n"
            
            # Send updates as they occur
            while True:
                data = client_queue.get()
                yield f"data: {data}\n\n"
        except GeneratorExit:
            with sse_lock:
                if client_queue in sse_clients[session_id]:
                    sse_clients[session_id].remove(client_queue)
    
    return Response(
        event_stream(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no"
        }
    )


@app.route("/api/download/<int:item_id>")
def download_file(item_id):
    """Serve the downloaded file to trigger browser download - session isolated"""
    session_id = get_session_id()
    ensure_session_initialized(session_id)
    
    with session_lock:
        item = next((x for x in user_downloads[session_id]['queue'] if x['id'] == item_id), None)
    
    if not item:
        return jsonify({"error": "Item not found"}), 404
    
    if item['status'] != 'Completed':
        return jsonify({"error": "File not ready"}), 400
    
    filepath = item.get('filepath')
    if not filepath or not os.path.exists(filepath):
        return jsonify({"error": "File not found"}), 404
    
    filename = item.get('filename', 'video.mp4')
    return send_file(filepath, as_attachment=True, download_name=filename)

@app.route("/api/clear", methods=["POST"])
def clear_downloads():
    session_id = get_session_id()
    ensure_session_initialized(session_id)
    
    with session_lock:
        # Clean up old files before clearing
        for item in user_downloads[session_id]['queue']:
            filepath = item.get('filepath')
            if filepath and os.path.exists(filepath):
                try:
                    os.remove(filepath)
                except:
                    pass
        
        user_downloads[session_id]['total'] = 0
        user_downloads[session_id]['completed'] = 0
        user_downloads[session_id]['downloading'] = 0
        user_downloads[session_id]['queue'] = []
        user_next_ids[session_id] = 1
        # Clear the task queue
        task_queue = user_task_queues[session_id]
        while not task_queue.empty():
            try:
                task_queue.get_nowait()
            except:
                break
    
    broadcast_update(session_id)  # Send update to this user only
    return jsonify(user_downloads[session_id])

# Add a root route for testing
@app.route("/")
def index():
    try:
        with open(os.path.join(os.path.dirname(__file__), 'index.html'), 'r') as f:
            return f.read()
    except:
        return jsonify({
            "message": "Video Downloader API",
            "endpoints": [
                "/api/status",
                "/api/events (SSE)",
                "/api/queue",
                "/api/upload",
                "/api/download/<id>",
                "/api/clear"
            ]
        })

# thumbnail extraction route
@app.route("/api/thumbnail", methods=["POST"])
def get_thumbnail():
    session_id = get_session_id()
    ensure_session_initialized(session_id)
    
    url = (request.get_json(force=True, silent=True) or {}).get("url", "").strip()
    if not url:
        return jsonify({"error": "No URL provided"}), 400
    try:
        opts = {
            'quiet': True,
            'skip_download': True,
            'socket_timeout': 10,
            'http_headers': {'User-Agent': 'Mozilla/5.0'}
        }
        
        info = YoutubeDL(opts).extract_info(url, download=False)
        if not info:
            return jsonify({"error": "No info"}), 404
        
        # Get low quality thumbnail (240-640px)
        thumbnail = None
        thumbs = [t for t in info.get('thumbnails', []) if t.get('url')]
        if thumbs:
            low_quality = [t for t in thumbs if 240 <= t.get('width', 0) <= 640]
            thumbnail = (min(low_quality, key=lambda x: abs(x.get('width', 0) - 480))['url'] 
                        if low_quality else min(thumbs, key=lambda x: x.get('width', 999999))['url'])
        thumbnail = thumbnail or info.get('thumbnail')
        if thumbnail:
            return jsonify({"thumbnail": thumbnail, "title": info.get('title', '')})
        return jsonify({"error": "No thumbnail"}), 404
    except Exception as e:
        print(f"Error: {url} - {e}")
        return jsonify({"error": str(e)}), 500
    
# Start workers for Gunicorn & production
start_workers()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)