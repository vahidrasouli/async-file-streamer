import asyncio
import aiohttp
import time
import os
import base64
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from dataclasses import dataclass
from enum import Enum
from rubpy import Client

# ==========================================
# 🌟 امنیت داکر: تزریق متغیرهای محیطی
# ==========================================
session_env = os.environ.get("RUBIKA_SESSION_BASE64")
if session_env:
    try:
        with open("time_sessions.rp", "wb") as f:
            f.write(base64.b64decode(session_env))
        print("✅ [سیستم] فایل سشن استخراج شد.")
    except Exception as e:
        sys.exit(1)

target_guid_env = os.environ.get("TARGET_CHAT_GUID_BASE64")
if target_guid_env:
    try:
        TARGET_CHAT_GUID = base64.b64decode(target_guid_env).decode("utf-8")
    except Exception as e:
        sys.exit(1)
else:
    TARGET_CHAT_GUID = "u0JuWpO08150d3e4de8e3b77a5ef7488"

# ==========================================
# ۱. تنظیمات و متغیرهای ایزوله ربات
# ==========================================
AUTH_PREFIX = "#dl_"

class BotStatus(Enum):
    IDLE = "idle"
    WAITING = "waiting_for_confirm"
    PROCESSING = "processing"
    COOLDOWN = "cooldown" 

@dataclass(slots=True)
class AppState:
    status: BotStatus
    pending_url: str | None
    pending_size_bytes: int
    last_processed_id: int
    active_task: asyncio.Task | None  

state = AppState(status=BotStatus.IDLE, pending_url=None, pending_size_bytes=0, last_processed_id=0, active_task=None)
message_queue = asyncio.Queue()

# ==========================================
# ۲. سرور فِیک برای عبور از Health Check (Thread کاملاً مجزا)
# ==========================================
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"Bot is Alive!")
    def log_message(self, format, *args):
        pass 

def start_health_server():
    try:
        server = HTTPServer(('0.0.0.0', 8080), HealthCheckHandler)
        print("🌐 [سیستم] قلب تپنده در Thread مجزا روی پورت 8080 روشن شد.")
        server.serve_forever()
    except Exception as e:
        pass

threading.Thread(target=start_health_server, daemon=True).start()

# ==========================================
# ۳. ابزارهای شبکه و روبیکا
# ==========================================
async def get_remote_file_size(url: str, session: aiohttp.ClientSession) -> int:
    try:
        custom_timeout = aiohttp.ClientTimeout(total=5, connect=2)
        async with session.head(url, allow_redirects=True, timeout=custom_timeout, headers={"Connection": "keep-alive"}) as resp:
            if resp.status in (200, 206): return int(resp.headers.get("Content-Length", 0))
    except Exception: pass
    return 0

def extract_message_id(msg_res) -> int:
    if isinstance(msg_res, dict): return int(msg_res.get("data", {}).get("message_update", {}).get("message_id") or msg_res.get("data", {}).get("message", {}).get("message_id") or 0)
    msg_update = getattr(msg_res, 'message_update', None)
    if msg_update: return int(getattr(msg_update, 'message_id', 0) or 0)
    msg_data = getattr(msg_res, 'message', None)
    if msg_data: return int(getattr(msg_data, 'message_id', 0) or 0)
    return 0

async def _safe_update_ui(client: Client, chat_guid: str, msg_id: int, text: str):
    try: await client.edit_message(chat_guid, str(msg_id), text)
    except Exception: pass 

async def clear_chat_history_task(client: Client, chat_guid: str):
    try:
        await asyncio.sleep(1.0) 
        deleted_count = 0
        while True:
            res = None
            try: res = await client.get_messages(chat_guid, None, 20)
            except Exception: break
            messages = res.get("data", {}).get("messages", []) if isinstance(res, dict) else getattr(res, 'messages', [])
            if not messages: break 
            
            msg_ids = []
            for msg in messages:
                mid = msg.get("message_id") if isinstance(msg, dict) else getattr(msg, "message_id", None)
                if mid: msg_ids.append(str(mid))
                
            if not msg_ids: break
            await client.delete_messages(chat_guid, msg_ids)
            deleted_count += len(msg_ids)
            await asyncio.sleep(2.0)
        await client.send_message(chat_guid, f"✨ پاکسازی کامل شد! ({deleted_count} پیام حذف شد)")
    except Exception as e:
        await client.send_message(chat_guid, f"❌ خطا در پاکسازی: {e}")
    finally:
        state.status = BotStatus.IDLE; state.active_task = None

# ==========================================
# ۴. هسته استریم و آپلود
# ==========================================
class DynamicChunker:
    def __init__(self): self.buffer = bytearray()
    def add_data(self, data: bytes): self.buffer.extend(data)
    def extract_ready_chunks(self, target_size: int):
        while len(self.buffer) >= target_size:
            chunk = bytes(self.buffer[:target_size])
            del self.buffer[:target_size]
            yield chunk
    def extract_remaining(self) -> bytes:
        data, self.buffer = bytes(self.buffer), bytearray()
        return data

async def stream_generator(file_url, shared_session, chunk_size=64*1024):
    async with shared_session.get(file_url, timeout=aiohttp.ClientTimeout(total=0, sock_read=30)) as resp:
        resp.raise_for_status()
        async for chunk in resp.content.iter_chunked(chunk_size): yield chunk 

# 🌟 تغییر ۱: افزایش تعداد تلاش مجدد به ۵ بار برای مقابله با Connection Reset
async def upload_memory_part(client, session, target_guid, part_data: bytes, part_name: str, max_retries=5):
    size, mime = len(part_data), part_name.split(".")[-1]
    res = await client.request_send_file(part_name, size, mime)
    file_id = getattr(res, 'id', getattr(res, 'file_id', res.get('id') if isinstance(res, dict) else None))
    upload_url = getattr(res, 'upload_url', res.get('upload_url') if isinstance(res, dict) else None)
    access_hash_send = getattr(res, 'access_hash_send', res.get('access_hash_send') if isinstance(res, dict) else None)
    dc_id = getattr(res, 'dc_id', res.get('dc_id') if isinstance(res, dict) else None)
    
    if not upload_url: raise Exception("لینک آپلود دریافت نشد.")

    for attempt in range(max_retries):
        try:
            # 🌟 تغییر ۲: افزایش زمان تایم‌اوت به ۱۲۰ ثانیه برای پارت‌های بزرگ
            async with session.post(
                url=upload_url, headers={"auth": client.auth, "file-id": str(file_id), "total-part": "1", "part-number": "1", "chunk-size": str(size), "access-hash-send": str(access_hash_send)},
                data=part_data, timeout=aiohttp.ClientTimeout(total=120)
            ) as response:
                
                # 🌟 تغییر ۳: بررسی وضعیت HTTP قبل از تبدیل به JSON (جلوگیری از باگ 413 HTML)
                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(f"HTTP {response.status}: {error_text[:50]}")
                    
                upload_result = await response.json()
                
            if upload_result.get("status") == "OK":
                access_hash_rec = upload_result.get("data", {}).get("access_hash_rec", "")
                await client.send_message(target_guid, text=f"✅ {part_name} ارسال شد.", file_inline={"mime": mime, "size": size, "dc_id": str(dc_id), "file_id": str(file_id), "file_name": part_name, "access_hash_rec": access_hash_rec, "type": "File"})
                return True
            else: raise Exception(f"روبیکا: {upload_result}")
        except asyncio.CancelledError: raise
        except Exception as e:
            if attempt < max_retries - 1: 
                await asyncio.sleep(2 ** attempt) # مکث نمایی برای وصل شدن مجدد به شبکه
            else: raise Exception(f"آپلود شکست خورد: {e}")

async def dynamic_memory_streaming(client, shared_session, file_url, target_guid, total_size_bytes, progress_callback):
    # 🌟 تغییر ۴: سقف حجم هر پارت به ۸ مگابایت محدود شد تا به لیمیت سرور روبیکا (413) نخوریم
    MIN_SIZE, MAX_SIZE, TARGET_UPLOAD_TIME = 1024 * 1024, 8 * 1024 * 1024, 4.0            
    current_target_size = 2 * 1024 * 1024   
    chunker, part_index, uploaded_bytes = DynamicChunker(), 1, 0
    total_mb = total_size_bytes / (1024 * 1024) if total_size_bytes else 0
    
    async for incoming_chunk in stream_generator(file_url, shared_session):
        chunker.add_data(incoming_chunk)
        for data_to_upload in chunker.extract_ready_chunks(int(current_target_size)):
            part_name = f"part_{part_index}.mp4"
            start_time = time.time()
            try:
                await upload_memory_part(client, shared_session, target_guid, data_to_upload, part_name)
                uploaded_bytes += len(data_to_upload)
                speed_bps = len(data_to_upload) / (time.time() - start_time)
                current_target_size = max(MIN_SIZE, min(MAX_SIZE, speed_bps * TARGET_UPLOAD_TIME))
                
                percent = min(100, int((uploaded_bytes / total_size_bytes) * 100)) if total_size_bytes else 0
                bar = '█' * int(15 * percent // 100) + '░' * (15 - int(15 * percent // 100))
                await progress_callback(f"🚀 **عملیات استریم فایل**\n━━━━━━━━━━━━━━━\n📊 پیشرفت: {percent}% [{bar}]\n📦 ارسال: {uploaded_bytes/(1024*1024):.2f} از {total_mb:.2f} MB\n⚡ سرعت لحظه‌ای: {(speed_bps/(1024*1024)):.2f} MB/s\n🧩 سایز پارت: {(len(data_to_upload)/(1024*1024)):.2f} MB")
                part_index += 1
            except asyncio.CancelledError: raise
            except Exception as e: raise Exception(f"متوقف در پارت {part_index}: {e}") 
                
    remaining_data = chunker.extract_remaining()
    if remaining_data: await upload_memory_part(client, shared_session, target_guid, remaining_data, f"part_{part_index}.mp4")

# ==========================================
# ۵. مدیریت تسک‌های پس‌زمینه ربات
# ==========================================
async def background_upload_task(client: Client, shared_session: aiohttp.ClientSession, target_url: str, reply_to_id: int, total_size_bytes: int):
    try:
        progress_msg = await client.send_message(TARGET_CHAT_GUID, "⏳ در حال اتصال...", reply_to_message_id=str(reply_to_id))
        progress_msg_id = extract_message_id(progress_msg)
        async def update_dashboard(text: str):
            if progress_msg_id: asyncio.create_task(_safe_update_ui(client, TARGET_CHAT_GUID, progress_msg_id, text))
        await dynamic_memory_streaming(client, shared_session, target_url, TARGET_CHAT_GUID, total_size_bytes, update_dashboard)
        if progress_msg_id: await client.edit_message(TARGET_CHAT_GUID, str(progress_msg_id), f"✅ آپلود پایان یافت.\n🔗 {target_url}")
    except asyncio.CancelledError:
        if progress_msg_id: await _safe_update_ui(client, TARGET_CHAT_GUID, progress_msg_id, "🛑 **عملیات لغو شد.**")
    except Exception as e:
        await client.send_message(TARGET_CHAT_GUID, f"❌ خطا: {e}", reply_to_message_id=str(reply_to_id))
    finally:
        if state.status != BotStatus.COOLDOWN: state.status = BotStatus.IDLE
        state.pending_url = None; state.active_task = None

async def message_fetcher(client: Client):
    MIN_SLEEP, MAX_SLEEP, current_sleep = 1.5, 15.0, 1.5 
    while True:
        await asyncio.sleep(current_sleep)
        try:
            res = None
            try: res = await client.get_messages(TARGET_CHAT_GUID, None, 10)
            except Exception: res = await client.get_messages(TARGET_CHAT_GUID, state.last_processed_id + 100, 10)
            messages = res.get("data", {}).get("messages", []) if isinstance(res, dict) else getattr(res, 'messages', [])
            if not messages: current_sleep = min(MAX_SLEEP, current_sleep * 1.5); continue
            has_new = False
            for msg in reversed(messages):
                msg_id = int(msg.get("message_id", 0) if isinstance(msg, dict) else getattr(msg, "message_id", 0))
                if msg_id > state.last_processed_id:
                    has_new = True; state.last_processed_id = msg_id
                    text = msg.get("text", "") if isinstance(msg, dict) else getattr(msg, "text", "")
                    if text: await message_queue.put({"id": msg_id, "text": str(text).strip()})
            current_sleep = MIN_SLEEP if has_new else min(MAX_SLEEP, current_sleep * 1.5)
        except Exception: 
            current_sleep = MAX_SLEEP 

async def command_processor(client: Client, shared_session: aiohttp.ClientSession):
    while True:
        msg_data = await message_queue.get()
        msg_id, text = msg_data["id"], msg_data["text"]
        try:
            if state.status == BotStatus.COOLDOWN:
                await client.send_message(TARGET_CHAT_GUID, "⏳ سیستم در حال استراحت است...", reply_to_message_id=str(msg_id)); continue
            if text.startswith(AUTH_PREFIX):
                if state.status == BotStatus.PROCESSING:
                    await client.send_message(TARGET_CHAT_GUID, "⏳ ربات درگیر است. دستور توقف بدهید.", reply_to_message_id=str(msg_id)); continue 
                url = text.replace(AUTH_PREFIX, "").strip()
                state.status = BotStatus.IDLE 
                await client.send_message(TARGET_CHAT_GUID, "🔄 آنالیز سرور مبدا...", reply_to_message_id=str(msg_id))
                await asyncio.sleep(0.5)
                size = await get_remote_file_size(url, shared_session)
                if size == 0: await client.send_message(TARGET_CHAT_GUID, "❌ سرور حجم را برنگرداند.", reply_to_message_id=str(msg_id))
                else:
                    state.status, state.pending_url, state.pending_size_bytes = BotStatus.WAITING, url, size
                    await client.send_message(TARGET_CHAT_GUID, f"📦 استریم:\n🔗 {url}\n⚖️ {(size/(1024*1024)):.2f} MB\nدستور؟ (تایید / لغو)", reply_to_message_id=str(msg_id))
            elif text == "تایید" and state.status == BotStatus.WAITING:
                state.status = BotStatus.PROCESSING
                state.active_task = asyncio.create_task(background_upload_task(client, shared_session, state.pending_url, msg_id, state.pending_size_bytes))
            elif text in ["لغو", "توقف"]:
                if state.status == BotStatus.WAITING: state.status, state.pending_url = BotStatus.IDLE, None; await client.send_message(TARGET_CHAT_GUID, "🛑 لغو شد.", reply_to_message_id=str(msg_id))
                elif state.status == BotStatus.PROCESSING:
                    if state.active_task and not state.active_task.done():
                        state.status = BotStatus.COOLDOWN; state.active_task.cancel()
                        await client.send_message(TARGET_CHAT_GUID, "🛑 سیگنال توقف ارسال شد...", reply_to_message_id=str(msg_id))
                        await asyncio.sleep(3.0) 
                        state.status = BotStatus.IDLE; await client.send_message(TARGET_CHAT_GUID, "✅ ارتباط قطع شد.", reply_to_message_id=str(msg_id))
            elif text == "#clear_chat" and state.status != BotStatus.PROCESSING:
                state.status = BotStatus.PROCESSING
                await client.send_message(TARGET_CHAT_GUID, "🧹 شروع پاکسازی...", reply_to_message_id=str(msg_id))
                state.active_task = asyncio.create_task(clear_chat_history_task(client, TARGET_CHAT_GUID))
        finally: message_queue.task_done()

# ==========================================
# ۶. اجرای اصلی
# ==========================================
async def main():
    connector = aiohttp.TCPConnector(limit=10)
    async with Client('time_sessions') as client, aiohttp.ClientSession(connector=connector) as shared_session:
        init_msg = await client.send_message(TARGET_CHAT_GUID, "🤖 سیستمِ یکپارچه ابری آماده دریافت فرامین است...")
        state.last_processed_id = extract_message_id(init_msg)
        
        await asyncio.gather(
            asyncio.create_task(message_fetcher(client)), 
            asyncio.create_task(command_processor(client, shared_session))
        )

if __name__ == "__main__":
    asyncio.run(main())