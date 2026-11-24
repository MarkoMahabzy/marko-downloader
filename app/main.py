"""
MARKO – Universal Media Downloader
FastAPI backend: yt-dlp + ffmpeg, job queue + workers, real SSE progress.
No DRM / login / bot-protection bypass. Blocked or private links return honest errors.
"""
import asyncio
import ipaddress
import json
import os
import queue
import re
import socket
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse, unquote

import requests
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DATA_DIR = Path(os.getenv("DATA_DIR", str(BASE_DIR.parent / "data" / "tmp")))
DATA_DIR.mkdir(parents=True, exist_ok=True)

MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE_MB", "2048")) * 1024 * 1024  # default 2 GB, configurable
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "2"))
RETENTION_MINUTES = int(os.getenv("FILE_RETENTION_MINUTES", "60"))
PORT = int(os.getenv("PORT", "8000"))

app = FastAPI(title="MARKO", docs_url=None, redoc_url=None, openapi_url=None)

# ---------------- Rate limiting (simple in-memory, fair) ----------------
_rate_lock = threading.Lock()
_rate_hits: dict[str, list[float]] = {}

def rate_limited(ip: str, limit: int, window_sec: int) -> bool:
    now = time.time()
    with _rate_lock:
        hits = [t for t in _rate_hits.get(ip, []) if now - t < window_sec]
        if len(hits) >= limit:
            _rate_hits[ip] = hits
            return True
        hits.append(now)
        _rate_hits[ip] = hits
        return False

# ---------------- SSRF / URL validation ----------------
def validate_url(url: str):
    """Returns (ok, error_ar). Blocks non-http(s), private/internal/reserved IPs."""
    if not url or len(url) > 2048:
        return False, "الرابط غير صالح"
    p = urlparse(url.strip())
    if p.scheme not in ("http", "https") or not p.hostname:
        return False, "الرابط غير صالح — لازم يبدأ بـ http أو https"
    host = p.hostname
    if host in ("localhost",):
        return False, "هذا الرابط غير مسموح به"
    try:
        infos = socket.getaddrinfo(host, None)
    except Exception:
        return False, "مش قادر أوصل للموقع ده — اتأكد من الرابط"
    # Explicit internal networks only. (Python's ip.is_private also flags ranges like
    # 198.18.0.0/15 benchmarking, which some DNS resolvers legitimately return for
    # public hosts via fake-IP proxies — blocking it would reject every public URL.)
    blocked_nets = [ipaddress.ip_network(n) for n in (
        "0.0.0.0/8", "10.0.0.0/8", "127.0.0.0/8", "169.254.0.0/16",
        "172.16.0.0/12", "192.168.0.0/16", "224.0.0.0/4",
        "::1/128", "fe80::/10", "fc00::/7", "::/128")]
    for info in infos:
        ip_str = info[4][0]
        try:
            ip = ipaddress.ip_address(ip_str)
        except ValueError:
            continue
        if any(ip in net for net in blocked_nets):
            return False, "هذا الرابط غير مسموح به (عنوان داخلي)"
    return True, ""

DIRECT_EXTS = {".mp4", ".mp3", ".m4a", ".wav", ".ogg", ".webm", ".mkv", ".mov",
               ".zip", ".rar", ".pdf", ".apk", ".jpg", ".jpeg", ".png", ".gif"}

def is_direct_file(url: str) -> bool:
    path = unquote(urlparse(url).path).lower()
    return any(path.endswith(e) for e in DIRECT_EXTS)

# ---------------- Jobs ----------------
jobs: dict[str, dict] = {}
jobs_lock = threading.Lock()
job_queue: "queue.Queue[str]" = queue.Queue()

def new_job(url, kind, quality):
    jid = uuid.uuid4().hex[:12]
    job = {
        "id": jid, "url": url, "kind": kind, "quality": quality,
        "status": "queued", "percent": 0.0, "downloaded_bytes": 0,
        "total_bytes": 0, "speed_bps": 0.0, "eta_sec": None,
        "filename": None, "filepath": None, "filesize": 0,
        "title": None, "error": None, "created_at": time.time(),
    }
    with jobs_lock:
        jobs[jid] = job
    return job

def update_job(jid, **kw):
    with jobs_lock:
        if jid in jobs:
            jobs[jid].update(kw)

def get_job(jid):
    with jobs_lock:
        j = jobs.get(jid)
        return dict(j) if j else None

def public_job(j):
    if not j:
        return None
    return {k: j[k] for k in ("id", "status", "percent", "downloaded_bytes", "total_bytes",
                              "speed_bps", "eta_sec", "filename", "filesize", "title", "error")}

# ---------------- yt-dlp helpers ----------------
def ydl_base_opts():
    return {
        "quiet": True, "no_warnings": True, "noplaylist": True,
        "socket_timeout": 30, "retries": 10, "fragment_retries": 10,
        # Test-only escape hatch: some sandboxed/proxied test environments MITM TLS with
        # a private CA that yt-dlp's bundled certifi store cannot verify. Production
        # default stays False (full certificate verification). Enable ONLY for local
        # testing behind such a proxy: YTDLP_NO_CHECK_CERTIFICATE=1
        "nocheckcertificate": os.getenv("YTDLP_NO_CHECK_CERTIFICATE") == "1",
        # YouTube sometimes blocks the default client on datacenter IPs; the android
        # client is a normal official client variant, not a DRM/login bypass.
        "extractor_args": {"youtube": {"player_client": ["default", "android"]}},
    }

def analyze_url(url: str):
    """Return info dict with ONLY real formats."""
    if is_direct_file(url):
        filename = unquote(Path(urlparse(url).path).name) or "file"
        total = 0
        ctype = ""
        try:
            r = requests.head(url, allow_redirects=True, timeout=15,
                              headers={"User-Agent": "Mozilla/5.0 MARKO"})
            total = int(r.headers.get("Content-Length") or 0)
            ctype = r.headers.get("Content-Type", "")
        except Exception:
            pass
        return {"title": filename, "duration": None, "thumbnail": None,
                "is_direct": True, "filename": filename,
                "video_qualities": [], "audio_formats": [],
                "direct_size": total, "content_type": ctype}
    import yt_dlp
    info = None
    last_exc = None
    # Platforms throttle datacenter IPs in bursts; a plain retry of the same normal
    # request often succeeds a few seconds later. This is NOT a protection bypass:
    # if the platform keeps blocking, we surface its honest error to the user.
    for attempt in range(3):
        try:
            with yt_dlp.YoutubeDL(ydl_base_opts()) as ydl:
                info = ydl.extract_info(url, download=False)
            break
        except Exception as e:
            last_exc = e
            if attempt < 2:
                time.sleep(5)
    if info is None and last_exc:
        raise last_exc
    if not info:
        raise ValueError("unsupported")
    formats = info.get("formats") or []
    if not formats and info.get("url"):
        # Generic extractor resolved a direct media stream (e.g. a bare file URL
        # whose extension is not in DIRECT_EXTS) — treat it as a direct file.
        ext = info.get("ext") or ""
        fname = (info.get("title") or "file") + (f".{ext}" if ext else "")
        return {"title": fname, "duration": info.get("duration"), "thumbnail": None,
                "is_direct": True, "filename": fname, "video_qualities": [],
                "audio_formats": [], "direct_size": info.get("filesize") or 0,
                "content_type": ""}
    vq = {}
    for f in formats:
        h = f.get("height")
        if h and f.get("vcodec") not in (None, "none"):
            cur = vq.get(h)
            size = f.get("filesize") or f.get("filesize_approx") or 0
            if cur is None or size > cur["filesize"]:
                vq[h] = {"height": h, "ext": f.get("ext") or "mp4", "filesize": size}
    video_qualities = sorted(vq.values(), key=lambda x: -x["height"])
    audio = []
    seen_abr = set()
    for f in formats:
        if f.get("acodec") not in (None, "none") and f.get("vcodec") in (None, "none"):
            abr = int(f.get("abr") or 0)
            if abr and abr not in seen_abr:
                seen_abr.add(abr)
                audio.append({"abr": abr, "ext": f.get("ext") or "mp3",
                              "filesize": f.get("filesize") or f.get("filesize_approx") or 0})
    audio.sort(key=lambda x: -x["abr"])
    return {"title": info.get("title") or "بدون عنوان",
            "duration": info.get("duration"), "thumbnail": info.get("thumbnail"),
            "is_direct": False, "video_qualities": video_qualities,
            "audio_formats": audio, "webpage_url": info.get("webpage_url") or url}

# ---------------- Download workers ----------------
def _progress_hook(jid):
    def hook(d):
        if d["status"] == "downloading":
            downloaded = d.get("downloaded_bytes") or 0
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            if total and downloaded > MAX_FILE_SIZE:
                raise Exception("حجم الملف أكبر من الحد المسموح به")
            pct = (downloaded / total * 100.0) if total else 0.0
            update_job(jid, status="downloading", downloaded_bytes=downloaded,
                       total_bytes=total, percent=round(pct, 2),
                       speed_bps=d.get("speed") or 0.0, eta_sec=d.get("eta"))
        elif d["status"] == "finished":
            update_job(jid, status="processing", percent=99.0)
    return hook

def download_direct(jid, url, outdir: Path):
    job = get_job(jid)
    filename = unquote(Path(urlparse(url).path).name) or f"marko-{jid}"
    dest = outdir / filename
    update_job(jid, status="downloading", title=filename)
    with requests.get(url, stream=True, timeout=60,
                      headers={"User-Agent": "Mozilla/5.0 MARKO"}) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length") or 0)
        if total and total > MAX_FILE_SIZE:
            raise Exception("حجم الملف أكبر من الحد المسموح به")
        downloaded = 0
        start = time.time()
        with open(dest, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if not chunk:
                    continue
                fh.write(chunk)
                downloaded += len(chunk)
                if downloaded > MAX_FILE_SIZE:
                    raise Exception("حجم الملف أكبر من الحد المسموح به")
                elapsed = max(time.time() - start, 0.001)
                speed = downloaded / elapsed
                pct = (downloaded / total * 100.0) if total else 0.0
                eta = int((total - downloaded) / speed) if total and speed else None
                update_job(jid, downloaded_bytes=downloaded, total_bytes=total,
                           percent=round(pct, 2), speed_bps=speed, eta_sec=eta)
    return dest

def download_media(jid, url, kind, quality, outdir: Path):
    import yt_dlp
    opts = ydl_base_opts()
    opts["outtmpl"] = str(outdir / "%(title).80s.%(ext)s")
    opts["progress_hooks"] = [_progress_hook(jid)]
    opts["merge_output_format"] = "mp4"
    if kind == "audio":
        opts["format"] = "bestaudio/best"
        opts["postprocessors"] = [{"key": "FFmpegExtractAudio",
                                   "preferredcodec": "mp3", "preferredquality": "192"}]
    else:
        if quality:
            h = int(quality)
            opts["format"] = f"bv*[height<={h}]+ba/b[height<={h}]/bv*+ba/b"
        else:
            opts["format"] = "bv*+ba/b"
    update_job(jid, status="downloading")
    last_exc = None
    for attempt in range(6):  # plain retries on transient platform throttling
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=True)
                if info:
                    update_job(jid, title=info.get("title"))
            last_exc = None
            break
        except Exception as e:
            last_exc = e
            # yt-dlp resumes from .part files (continuedl), so retrying after a
            # mid-download cut continues where it stopped instead of restarting.
            time.sleep(15)
    if last_exc:
        raise last_exc
    # find largest produced file
    files = [p for p in outdir.iterdir() if p.is_file()]
    if not files:
        raise Exception("الملف ما اتجهزش — حاول تاني")
    dest = max(files, key=lambda p: p.stat().st_size)
    return dest

def worker_loop():
    while True:
        jid = job_queue.get()
        job = get_job(jid)
        if not job:
            job_queue.task_done()
            continue
        outdir = DATA_DIR / jid
        outdir.mkdir(parents=True, exist_ok=True)
        try:
            update_job(jid, status="preparing", percent=0.0)
            if is_direct_file(job["url"]):
                dest = download_direct(jid, job["url"], outdir)
            else:
                dest = download_media(jid, job["url"], job["kind"], job["quality"], outdir)
            size = dest.stat().st_size
            update_job(jid, status="done", percent=100.0, downloaded_bytes=size,
                       total_bytes=size, filename=dest.name, filepath=str(dest),
                       filesize=size, speed_bps=0.0, eta_sec=0)
        except Exception as e:
            msg = str(e)
            low = msg.lower()
            if "private" in low or "login" in low or "sign in" in low:
                msg = "المحتوى ده خاص أو محتاج تسجيل دخول — مش هينفع ينزل"
            elif "unsupported" in low or "no video" in low:
                msg = "الموقع أو الرابط ده غير مدعوم"
            elif "أكبر من الحد" not in msg:
                msg = "حصلت مشكلة أثناء التحميل — ممكن الموقع يكون حاجب التحميل أو الرابط مش شغال"
            update_job(jid, status="error", error=msg)
            try:
                for p in outdir.iterdir():
                    p.unlink(missing_ok=True)
            except Exception:
                pass
        finally:
            job_queue.task_done()

for _ in range(MAX_CONCURRENT):
    threading.Thread(target=worker_loop, daemon=True).start()

# ---------------- Cleanup ----------------
def cleanup_loop():
    while True:
        time.sleep(300)
        cutoff = time.time() - RETENTION_MINUTES * 60
        with jobs_lock:
            old = [jid for jid, j in jobs.items()
                   if j["created_at"] < cutoff and j["status"] in ("done", "error")]
        for jid in old:
            d = DATA_DIR / jid
            if d.exists():
                for p in d.iterdir():
                    p.unlink(missing_ok=True)
                d.rmdir()
            with jobs_lock:
                jobs.pop(jid, None)

threading.Thread(target=cleanup_loop, daemon=True).start()

# ---------------- API ----------------
class AnalyzeIn(BaseModel):
    url: str

class DownloadIn(BaseModel):
    url: str
    kind: str = "video"   # video | audio (plain words in UI: فيديو / صوت بس)
    quality: int | None = None

@app.post("/api/analyze")
def api_analyze(body: AnalyzeIn, request: Request):
    ip = request.client.host if request.client else "?"
    if rate_limited(f"a:{ip}", 20, 60):
        return JSONResponse({"ok": False, "error": "طلبات كتير بسرعة — استنى ثواني وحاول تاني"}, status_code=429)
    ok, err = validate_url(body.url)
    if not ok:
        return JSONResponse({"ok": False, "error": err}, status_code=400)
    try:
        info = analyze_url(body.url.strip())
        return {"ok": True, "info": info}
    except Exception:
        return JSONResponse({"ok": False, "error": "الرابط ده مش مدعوم أو المحتوى خاص/محجوب"}, status_code=422)

@app.post("/api/download")
def api_download(body: DownloadIn, request: Request):
    ip = request.client.host if request.client else "?"
    if rate_limited(f"d:{ip}", 10, 600):
        return JSONResponse({"ok": False, "error": "وصلت لحد التحميلات دلوقتي — استنى شوية"}, status_code=429)
    if body.kind not in ("video", "audio"):
        return JSONResponse({"ok": False, "error": "اختار فيديو أو صوت بس"}, status_code=400)
    ok, err = validate_url(body.url)
    if not ok:
        return JSONResponse({"ok": False, "error": err}, status_code=400)
    job = new_job(body.url.strip(), body.kind, body.quality)
    job_queue.put(job["id"])
    return {"ok": True, "job_id": job["id"], "position": job_queue.qsize()}

@app.get("/api/jobs/{jid}")
def api_job(jid: str):
    j = get_job(jid)
    if not j:
        return JSONResponse({"ok": False, "error": "العملية مش موجودة"}, status_code=404)
    return {"ok": True, "job": public_job(j)}

@app.get("/api/jobs/{jid}/events")
async def api_events(jid: str):
    if not get_job(jid):
        return JSONResponse({"ok": False, "error": "العملية مش موجودة"}, status_code=404)
    async def gen():
        last = None
        while True:
            j = get_job(jid)
            if not j:
                yield "event: error\ndata: {}\n\n"
                return
            snap = json.dumps(public_job(j), ensure_ascii=False)
            if snap != last:
                last = snap
                yield f"data: {snap}\n\n"
            if j["status"] in ("done", "error"):
                return
            await asyncio.sleep(0.5)
    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.get("/api/files/{jid}")
def api_file(jid: str):
    j = get_job(jid)
    if not j or j["status"] != "done" or not j.get("filepath"):
        return JSONResponse({"ok": False, "error": "الملف مش جاهز"}, status_code=404)
    path = Path(j["filepath"])
    if not path.exists():
        return JSONResponse({"ok": False, "error": "الملف اتمسح — نزّله تاني"}, status_code=404)
    # FileResponse supports HTTP Range and we force attachment download to the device.
    return FileResponse(path, filename=path.name, content_disposition_type="attachment")

# ---------------- Health check (Render) ----------------
@app.get("/healthz")
def healthz():
    return {"ok": True}

# ---------------- Pages ----------------
@app.get("/")
def home():
    return FileResponse(STATIC_DIR / "index.html")

@app.get("/how")
def how():
    return FileResponse(STATIC_DIR / "how.html")

@app.get("/about")
def about():
    return FileResponse(STATIC_DIR / "about.html")

@app.get("/privacy")
def privacy():
    return FileResponse(STATIC_DIR / "privacy.html")

@app.get("/terms")
def terms():
    return FileResponse(STATIC_DIR / "terms.html")

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

@app.exception_handler(404)
def not_found(request: Request, exc):
    if request.url.path.startswith("/api/"):
        return JSONResponse({"ok": False, "error": "مش موجود"}, status_code=404)
    return FileResponse(STATIC_DIR / "404.html", status_code=404)
