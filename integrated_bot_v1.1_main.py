import asyncio
import aiohttp
import time
import os
import base64
import sys
import threading
import math
import urllib.parse
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
MAX_ALLOWED_SIZE = 1950 * 1024 * 1024 

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
# ۲. سرور فِیک برای عبور از Health Check
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
    except Exception:
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
# ۴. هسته استریم یکپارچه (Multipart Upload - TURBO MAX 🚀🚀)
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

async def stream_generator(file_url, shared_session, chunk_size=64*1024, max_retries=10):
    downloaded_bytes = 0
    retries = 0
    while retries < max_retries:
        headers = {}
        if downloaded_bytes > 0: headers["Range"] = f"bytes={downloaded_bytes}-"
        try:
            timeout = aiohttp.ClientTimeout(total=0, sock_read=60)
            async with shared_session.get(file_url, headers=headers, timeout=timeout) as resp:
                resp.raise_for_status()
                if downloaded_bytes > 0 and resp.status != 206: raise Exception("سرور مبدا Resume ندارد.")
                async for chunk in resp.content.iter_chunked(chunk_size):
                    yield chunk
                    downloaded_bytes += len(chunk)
                return 
        except asyncio.CancelledError: raise
        except Exception as e:
            retries += 1
            if retries >= max_retries: raise Exception(f"قطعی مبدا: {e}")
            await asyncio.sleep(3) 

async def _upload_multipart_chunk(session, auth, upload_url, file_id, total_parts, part_index, chunk_data, access_hash_send, max_retries=5):
    chunk_size = len(chunk_data)
    for attempt in range(max_retries):
        try:
            headers = {
                "auth": auth, "file-id": str(file_id), "total-part": str(total_parts),
                "part-number": str(part_index), "chunk-size": str(chunk_size), "access-hash-send": str(access_hash_send)
            }
            async with session.post(upload_url, headers=headers, data=chunk_data, timeout=aiohttp.ClientTimeout(total=120)) as response:
                if response.status != 200: raise Exception(f"HTTP {response.status}")
                res_json = await response.json()
                
            if res_json.get("status") == "OK":
                data_block = res_json.get("data")
                if data_block and isinstance(data_block, dict):
                    return data_block.get("access_hash_rec", "")
                return ""
            else: raise Exception(f"خطا: {res_json}")
        except asyncio.CancelledError: raise
        except Exception as e:
            if attempt < max_retries - 1: await asyncio.sleep(2 ** attempt)
            else: raise Exception(f"تزریق پارت {part_index} شکست خورد: {e}")

async def upload_entire_file_stream(client, shared_session, file_url, target_guid, total_size_bytes, progress_callback):
    parsed_url = urllib.parse.urlparse(file_url)
    file_name = os.path.basename(parsed_url.path)
    if not file_name or "." not in file_name: file_name = "stream_file.zip"
    mime = file_name.split(".")[-1]

    # 🌟 ارتقاء حجم پارت‌ها به ۴ مگابایت (زیر خط قرمز روبیکا)
    CHUNK_SIZE = 4 * 1024 * 1024 
    total_parts = math.ceil(total_size_bytes / CHUNK_SIZE) if total_size_bytes else 1

    # 🌟 افزایش تعداد تلاش‌های اولیه به ۸ بار برای فایل‌های سنگین
    res = None
    max_init_retries = 8
    for attempt in range(max_init_retries):
        try:
            res = await client.request_send_file(file_name, total_size_bytes, mime)
            break
        except Exception as e:
            if attempt < max_init_retries - 1:
                wait_time = 5 * (attempt + 1)
                await progress_callback(f"⚠️ سرور روبیکا برای تخصیص فضا شلوغ است.\nتلاش مجدد ({attempt+1}/{max_init_retries}) در {wait_time} ثانیه دیگر...")
                await asyncio.sleep(wait_time)
            else:
                raise Exception(f"ارتباط اولیه با روبیکا برقرار نشد: {e}")

    file_id = getattr(res, 'id', getattr(res, 'file_id', res.get('id') if isinstance(res, dict) else None))
    upload_url = getattr(res, 'upload_url', res.get('upload_url') if isinstance(res, dict) else None)
    access_hash_send = getattr(res, 'access_hash_send', res.get('access_hash_send') if isinstance(res, dict) else None)
    dc_id = getattr(res, 'dc_id', res.get('dc_id') if isinstance(res, dict) else None)
    
    if not upload_url: raise Exception("روبیکا لینک آپلود یکپارچه نداد.")

    chunker = DynamicChunker()
    part_index = 1
    uploaded_bytes = 0
    total_mb = total_size_bytes / (1024 * 1024) if total_size_bytes else 0
    final_access_hash = ""
    
    last_update_time = time.time()
    last_percent = -1
    
    tasks = []
    start_time = time.time()

    async def upload_task_wrapper(d, p_idx):
        rec = await _upload_multipart_chunk(shared_session, client.auth, upload_url, file_id, total_parts, p_idx, d, access_hash_send)
        return (p_idx, rec, len(d))

    async for incoming_chunk in stream_generator(file_url, shared_session):
        chunker.add_data(incoming_chunk)
        for data_to_upload in chunker.extract_ready_chunks(CHUNK_SIZE):
            
            task = asyncio.create_task(upload_task_wrapper(data_to_upload, part_index))
            tasks.append(task)
            part_index += 1
            
            # 🌟 ارتقاء موازی‌سازی: ارسال ۵ پارت به صورت همزمان (۲۰ مگابایت در هر شلیک)
            if len(tasks) >= 5:
                results = await asyncio.gather(*tasks)
                for p_idx, rec_hash, length in results:
                    if rec_hash: final_access_hash = rec_hash
                    uploaded_bytes += length
                tasks.clear()
                
                speed_bps = uploaded_bytes / (time.time() - start_time)
                percent = min(100, int((uploaded_bytes / total_size_bytes) * 100)) if total_size_bytes else 0
                now = time.time()
                if now - last_update_time > 4.0 and percent > last_percent:
                    bar = '█' * int(15 * percent // 100) + '░' * (15 - int(15 * percent // 100))
                    await progress_callback(f"🚀 **تزریق موازی (حالت توربو مکس)**\n━━━━━━━━━━━━━━━\n📊 پیشرفت: {percent}% [{bar}]\n📦 پارت‌ها: {part_index-1} از {total_parts} (حجم {CHUNK_SIZE/(1024*1024):.1f}MB)\n⚡ سرعت: {(speed_bps/(1024*1024)):.2f} MB/s\n📥 ارسال: {uploaded_bytes/(1024*1024):.2f} از {total_mb:.2f} MB")
                    last_update_time, last_percent = now, percent
                
    remaining_data = chunker.extract_remaining()
    if remaining_data: 
        task = asyncio.create_task(upload_task_wrapper(remaining_data, part_index))
        tasks.append(task)

    if tasks:
        results = await asyncio.gather(*tasks)
        for p_idx, rec_hash, length in results:
            if rec_hash: final_access_hash = rec_hash
            uploaded_bytes += length
        tasks.clear()

    await progress_callback("✅ تزریق پارت‌ها پایان یافت. در حال همگام‌سازی نهایی سرور...")
    
    # 🌟 افزایش تلاش‌های ثبت نهایی به ۶ بار
    for attempt in range(6):
        try:
            await client.send_message(
                target_guid, 
                text=f"✅ دانلود مستقیم از سرور ابری انجام شد.\n📂 نام: {file_name}",
                file_inline={"mime": mime, "size": total_size_bytes, "dc_id": str(dc_id), "file_id": str(file_id), "file_name": file_name, "access_hash_rec": final_access_hash, "type": "File"}
            )
            break 
        except Exception as e:
            if attempt < 5:
                await progress_callback(f"⚠️ سرور روبیکا در حال پردازش نهایی است. تلاش مجدد ({attempt+1}/5)...")
                await asyncio.sleep(5)
            else:
                raise Exception(f"خطا در ایجاد پیام نهایی: {e}")

# ==========================================
# ۵. مدیریت تسک‌های پس‌زمینه ربات
# ==========================================
async def background_upload_task(client: Client, shared_session: aiohttp.ClientSession, target_url: str, reply_to_id: int, total_size_bytes: int):
    try:
        progress_msg = await client.send_message(TARGET_CHAT_GUID, "⏳ در حال آنالیز ساختار فایل...", reply_to_message_id=str(reply_to_id))
        progress_msg_id = extract_message_id(progress_msg)
        async def update_dashboard(text: str):
            if progress_msg_id: asyncio.create_task(_safe_update_ui(client, TARGET_CHAT_GUID, progress_msg_id, text))
            
        await upload_entire_file_stream(client, shared_session, target_url, TARGET_CHAT_GUID, total_size_bytes, update_dashboard)
        if progress_msg_id: await client.delete_messages(TARGET_CHAT_GUID, [str(progress_msg_id)]) 
    except asyncio.CancelledError:
        if progress_msg_id: await _safe_update_ui(client, TARGET_CHAT_GUID, progress_msg_id, "🛑 **عملیات توسط کاربر لغو شد.**")
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
                
                if size == 0: 
                    await client.send_message(TARGET_CHAT_GUID, "❌ سرور مبدا حجم را برنگرداند.", reply_to_message_id=str(msg_id))
                elif size > MAX_ALLOWED_SIZE:
                    await client.send_message(TARGET_CHAT_GUID, f"❌ **رد درخواست:**\nحجم فایل شما ({(size/(1024*1024*1024)):.2f} GB) بیشتر از سقف مجاز سرورهای روبیکا (حدود ۲ گیگابایت) است.", reply_to_message_id=str(msg_id))
                else:
                    state.status, state.pending_url, state.pending_size_bytes = BotStatus.WAITING, url, size
                    await client.send_message(TARGET_CHAT_GUID, f"📦 استریم یکپارچه:\n🔗 {url}\n⚖️ {(size/(1024*1024)):.2f} MB\nدستور؟ (تایید / لغو)", reply_to_message_id=str(msg_id))
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
    connector = aiohttp.TCPConnector(limit=30)
    async with Client('time_sessions') as client, aiohttp.ClientSession(connector=connector) as shared_session:
        init_msg = await client.send_message(TARGET_CHAT_GUID, "🤖 سیستم آپلود موازی (Turbo Max) روشن شد...")
        state.last_processed_id = extract_message_id(init_msg)
        
        await asyncio.gather(
            asyncio.create_task(message_fetcher(client)), 
            asyncio.create_task(command_processor(client, shared_session))
        )

if __name__ == "__main__":
    asyncio.run(main())