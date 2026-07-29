import asyncio
import aiohttp
import time
import os
import base64
import sys
from dataclasses import dataclass
from enum import Enum
from rubpy import Client

# ==========================================
# 🌟 امنیت داکر: تزریق سشن از متغیرهای محیطی
# ==========================================
session_env = os.environ.get("RUBIKA_SESSION_BASE64")
if session_env:
    try:
        with open("time_sessions.rp", "wb") as f:
            f.write(base64.b64decode(session_env))
        print("✅ [سیستم] فایل سشن با موفقیت استخراج شد.")
    except Exception as e:
        print(f"❌ [سیستم] ارور در دیکد کردن سشن: {e}")
        sys.exit(1)

# ==========================================
# ۱. تنظیمات و متغیرهای ایزوله ربات
# ==========================================
TARGET_CHAT_GUID = "u0JuWpO08150d3e4de8e3b77a5ef7488" 
AUTH_PREFIX = "#dl_"

class BotStatus(Enum):
    IDLE = "idle"
    WAITING = "waiting_for_confirm"
    PROCESSING = "processing"
    COOLDOWN = "cooldown" # 🌟 وضعیت جدید برای استراحت شبکه

@dataclass(slots=True)
class AppState:
    status: BotStatus
    pending_url: str | None
    pending_size_bytes: int
    last_processed_id: int
    active_task: asyncio.Task | None  

state = AppState(
    status=BotStatus.IDLE,
    pending_url=None,
    pending_size_bytes=0,
    last_processed_id=0,
    active_task=None
)

message_queue = asyncio.Queue()

# ==========================================
# ۲. ابزارهای شبکه و میکرو‌سرویس‌های تعاملی
# ==========================================
async def get_remote_file_size(url: str, session: aiohttp.ClientSession) -> int:
    try:
        custom_timeout = aiohttp.ClientTimeout(total=5, connect=2)
        async with session.head(url, allow_redirects=True, timeout=custom_timeout, headers={"Connection": "keep-alive"}) as resp:
            if resp.status in (200, 206):
                return int(resp.headers.get("Content-Length", 0))
    except asyncio.TimeoutError:
        pass
    except Exception:
        pass
    return 0

def extract_message_id(msg_res) -> int:
    if isinstance(msg_res, dict):
        return int(msg_res.get("data", {}).get("message_update", {}).get("message_id") or 
                   msg_res.get("data", {}).get("message", {}).get("message_id") or 0)
    msg_update = getattr(msg_res, 'message_update', None)
    if msg_update: return int(getattr(msg_update, 'message_id', 0) or 0)
    msg_data = getattr(msg_res, 'message', None)
    if msg_data: return int(getattr(msg_data, 'message_id', 0) or 0)
    return 0

async def _safe_update_ui(client: Client, chat_guid: str, msg_id: int, text: str):
    try:
        await client.edit_message(chat_guid, str(msg_id), text)
    except Exception:
        pass 

async def clear_chat_history_task(client: Client, chat_guid: str):
    try:
        # مکث اولیه برای اطمینان از پایداری شبکه قبل از بمباران درخواست‌ها
        await asyncio.sleep(1.0) 
        deleted_count = 0
        while True:
            res = None
            try:
                res = await client.get_messages(chat_guid, None, 20)
            except Exception:
                break
            
            messages = res.get("data", {}).get("messages", []) if isinstance(res, dict) else getattr(res, 'messages', [])
            if not messages: 
                break 
            
            msg_ids = []
            for msg in messages:
                mid = msg.get("message_id") if isinstance(msg, dict) else getattr(msg, "message_id", 0)
                if mid:
                    msg_ids.append(str(mid))
                    
            if not msg_ids: 
                break
            
            await client.delete_messages(chat_guid, msg_ids)
            deleted_count += len(msg_ids)
            await asyncio.sleep(2.0)
            
        await client.send_message(chat_guid, f"✨ پاکسازی کامل شد! ({deleted_count} پیام حذف شد)")
    except Exception as e:
        await client.send_message(chat_guid, f"❌ خطا در پاکسازی: {e}")
    finally:
        state.status = BotStatus.IDLE
        state.pending_url = None
        state.active_task = None

# ==========================================
# ۳. هسته استریم و آپلود
# ==========================================
class DynamicChunker:
    def __init__(self):
        self.buffer = bytearray()
        
    def add_data(self, data: bytes):
        self.buffer.extend(data)
        
    def extract_ready_chunks(self, target_size: int):
        while len(self.buffer) >= target_size:
            chunk = bytes(self.buffer[:target_size])
            del self.buffer[:target_size]
            yield chunk
            
    def extract_remaining(self) -> bytes:
        if self.buffer:
            data = bytes(self.buffer)
            self.buffer.clear()
            return data
        return b""

async def stream_generator(file_url, shared_session, chunk_size=64*1024):
    try:
        async with shared_session.get(file_url, timeout=aiohttp.ClientTimeout(total=0, sock_read=30)) as resp:
            resp.raise_for_status()
            async for chunk in resp.content.iter_chunked(chunk_size):
                yield chunk 
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"❌ خطای قطعی در دانلودر: {e}")
        raise

async def upload_memory_part(client, session, target_guid, part_data: bytes, part_name: str, max_retries=3):
    size = len(part_data)
    mime = part_name.split(".")[-1]
    
    res = await client.request_send_file(part_name, size, mime)
    file_id = getattr(res, 'id', getattr(res, 'file_id', res.get('id') if isinstance(res, dict) else None))
    upload_url = getattr(res, 'upload_url', res.get('upload_url') if isinstance(res, dict) else None)
    access_hash_send = getattr(res, 'access_hash_send', res.get('access_hash_send') if isinstance(res, dict) else None)
    dc_id = getattr(res, 'dc_id', res.get('dc_id') if isinstance(res, dict) else None)
    
    if not upload_url: raise Exception("لینک آپلود دریافت نشد.")

    backoff = 1.0
    for attempt in range(max_retries):
        try:
            async with session.post(
                url=upload_url,
                headers={"auth": client.auth, "file-id": str(file_id), "total-part": "1", "part-number": "1", "chunk-size": str(size), "access-hash-send": str(access_hash_send)},
                data=part_data,
                timeout=aiohttp.ClientTimeout(total=45)
            ) as response:
                upload_result = await response.json()
                
            if upload_result.get("status") == "OK":
                access_hash_rec = upload_result.get("data", {}).get("access_hash_rec", "")
                file_inline = {"mime": mime, "size": size, "dc_id": str(dc_id), "file_id": str(file_id), "file_name": part_name, "access_hash_rec": access_hash_rec, "type": "File"}
                
                await client.send_message(target_guid, text=f"✅ {part_name} ارسال شد.", file_inline=file_inline)
                return True
            else:
                raise Exception(f"خطای روبیکا: {upload_result}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(backoff * (2 ** attempt))
            else:
                raise Exception(f"آپلود شکست خورد: {e}")

async def dynamic_memory_streaming(client, shared_session, file_url, target_guid, total_size_bytes, progress_callback):
    MIN_SIZE = 256 * 1024               
    MAX_SIZE = 25 * 1024 * 1024         
    TARGET_UPLOAD_TIME = 3.0            
    current_target_size = 1024 * 1024   
    
    chunker = DynamicChunker()
    part_index = 1
    uploaded_bytes = 0
    total_mb = total_size_bytes / (1024 * 1024) if total_size_bytes else 0
    
    async for incoming_chunk in stream_generator(file_url, shared_session):
        chunker.add_data(incoming_chunk)
        
        for data_to_upload in chunker.extract_ready_chunks(int(current_target_size)):
            part_name = f"part_{part_index}.mp4"
            start_time = time.time()
            
            try:
                await upload_memory_part(client, shared_session, target_guid, data_to_upload, part_name)
                
                elapsed = time.time() - start_time
                uploaded_bytes += len(data_to_upload)
                
                speed_bps = len(data_to_upload) / elapsed
                speed_mbps = speed_bps / (1024 * 1024)
                current_mb = len(data_to_upload) / (1024 * 1024)
                uploaded_mb = uploaded_bytes / (1024 * 1024)
                
                calculated_next_size = speed_bps * TARGET_UPLOAD_TIME
                current_target_size = max(MIN_SIZE, min(MAX_SIZE, calculated_next_size))
                
                percent = min(100, int((uploaded_bytes / total_size_bytes) * 100)) if total_size_bytes else 0
                filled = int(15 * percent // 100)
                bar = '█' * filled + '░' * (15 - filled)
                
                dashboard_text = (
                    f"🚀 **عملیات استریم فایل**\n"
                    f"━━━━━━━━━━━━━━━\n"
                    f"📊 پیشرفت: {percent}% [{bar}]\n"
                    f"📦 ارسال شده: {uploaded_mb:.2f} از {total_mb:.2f} MB\n"
                    f"━━━━━━━━━━━━━━━\n"
                    f"⚡ سرعت لحظه‌ای: {speed_mbps:.2f} MB/s\n"
                    f"🧩 سایز پارت فعلی: {current_mb:.2f} MB\n"
                )
                await progress_callback(dashboard_text)
                
                part_index += 1
                
            except asyncio.CancelledError:
                raise
            except Exception as e:
                chunker.add_data(data_to_upload)
                current_target_size = max(MIN_SIZE, current_target_size / 2)
                await asyncio.sleep(2)
                
    remaining_data = chunker.extract_remaining()
    if remaining_data:
        part_name = f"part_{part_index}.mp4"
        await upload_memory_part(client, shared_session, target_guid, remaining_data, part_name)

# ==========================================
# ۴. مدیریت تسک‌های پس‌زمینه ربات
# ==========================================
async def background_upload_task(client: Client, shared_session: aiohttp.ClientSession, target_url: str, reply_to_id: int, total_size_bytes: int):
    try:
        progress_msg = await client.send_message(TARGET_CHAT_GUID, "⏳ در حال استخراج لاین استریم...", reply_to_message_id=str(reply_to_id))
        progress_msg_id = extract_message_id(progress_msg)
        
        async def update_dashboard(text: str):
            if progress_msg_id:
                asyncio.create_task(_safe_update_ui(client, TARGET_CHAT_GUID, progress_msg_id, text))

        await dynamic_memory_streaming(client, shared_session, target_url, TARGET_CHAT_GUID, total_size_bytes, update_dashboard)
                
        final_text = f"✅ آپلود با موفقیت پایان یافت.\n🔗 لینک استریم شده:\n{target_url}"
        if progress_msg_id:
            await client.edit_message(TARGET_CHAT_GUID, str(progress_msg_id), final_text)
            
    except asyncio.CancelledError:
        print("⚠️ [سیستم] عملیات با دستور توقف اورژانسی (Kill Switch) متوقف شد.")
        if progress_msg_id:
            await _safe_update_ui(client, TARGET_CHAT_GUID, progress_msg_id, "🛑 **عملیات استریم توسط کاربر متوقف (Kill) شد.**\nربات در حال بستن کانکشن‌ها...")
    except Exception as e:
        await client.send_message(TARGET_CHAT_GUID, f"❌ خطا: {e}", reply_to_message_id=str(reply_to_id))
    finally:
        if state.status != BotStatus.COOLDOWN:
            state.status = BotStatus.IDLE
        state.pending_url = None
        state.active_task = None

async def message_fetcher(client: Client):
    print("📡 رادار هوشمند شبکه فعال شد...")
    MIN_SLEEP, MAX_SLEEP = 1.5, 15.0 
    current_sleep = MIN_SLEEP
    
    while True:
        await asyncio.sleep(current_sleep)
        try:
            res = None
            try:
                res = await client.get_messages(TARGET_CHAT_GUID, None, 10)
            except Exception:
                res = await client.get_messages(TARGET_CHAT_GUID, state.last_processed_id + 100, 10)
            
            messages = res.get("data", {}).get("messages", []) if isinstance(res, dict) else getattr(res, 'messages', [])
            if not messages:
                current_sleep = min(MAX_SLEEP, current_sleep * 1.5)
                continue
                
            has_new_message = False
            for msg in reversed(messages):
                msg_id = int(msg.get("message_id") if isinstance(msg, dict) else getattr(msg, "message_id", 0))
                
                if msg_id > state.last_processed_id:
                    has_new_message = True
                    state.last_processed_id = msg_id
                    text = msg.get("text", "") if isinstance(msg, dict) else getattr(msg, "text", "")
                    if text: await message_queue.put({"id": msg_id, "text": str(text).strip()})
            
            current_sleep = MIN_SLEEP if has_new_message else min(MAX_SLEEP, current_sleep * 1.5)
        except Exception:
            current_sleep = MAX_SLEEP 

async def command_processor(client: Client, shared_session: aiohttp.ClientSession):
    print("⚙️ هسته پردازشی روشن شد...")
    while True:
        msg_data = await message_queue.get()
        msg_id, text = msg_data["id"], msg_data["text"]
        
        try:
            # بررسی حالت Cooldown
            if state.status == BotStatus.COOLDOWN:
                await client.send_message(TARGET_CHAT_GUID, "⏳ سیستم در حال استراحت و تخلیه کانکشن‌هاست. لطفاً چند ثانیه دیگر تلاش کنید...", reply_to_message_id=str(msg_id))
                continue

            if text.startswith(AUTH_PREFIX):
                if state.status == BotStatus.PROCESSING:
                    await client.send_message(TARGET_CHAT_GUID, "⏳ ربات درگیر پردازش است. جهت توقف استریم کلمه «توقف» را ارسال کنید.", reply_to_message_id=str(msg_id))
                    continue 
                if state.status == BotStatus.WAITING:
                    await client.send_message(TARGET_CHAT_GUID, "⚠️ لینک قبلی خودکار لغو شد.", reply_to_message_id=str(msg_id))
                
                url = text.replace(AUTH_PREFIX, "").strip()
                state.status = BotStatus.IDLE 
                
                if state.status == BotStatus.IDLE:
                    await client.send_message(TARGET_CHAT_GUID, "🔄 آنالیز سرور مبدا...", reply_to_message_id=str(msg_id))
                    await asyncio.sleep(0.5)
                    
                    size = await get_remote_file_size(url, shared_session)
                    if size == 0:
                        await client.send_message(TARGET_CHAT_GUID, "❌ سرور مبدا حجم را برنگرداند.", reply_to_message_id=str(msg_id))
                    else:
                        state.status = BotStatus.WAITING
                        state.pending_url = url
                        state.pending_size_bytes = size
                        await client.send_message(TARGET_CHAT_GUID, f"📦 جزئیات استریم:\n🔗 {url}\n⚖️ حجم: {(size / (1024*1024)):.2f} MB\n\nدستور؟ (تایید / لغو)", reply_to_message_id=str(msg_id))

            elif text == "تایید" and state.status == BotStatus.WAITING:
                state.status = BotStatus.PROCESSING
                state.active_task = asyncio.create_task(background_upload_task(client, shared_session, state.pending_url, msg_id, state.pending_size_bytes))

            # سیستم قطع ارتباط ایمن (Graceful Shutdown)
            elif text in ["لغو", "توقف"]:
                if state.status == BotStatus.WAITING:
                    state.status = BotStatus.IDLE
                    state.pending_url = None
                    await client.send_message(TARGET_CHAT_GUID, "🛑 عملیات لغو شد.", reply_to_message_id=str(msg_id))
                elif state.status == BotStatus.PROCESSING:
                    if state.active_task and not state.active_task.done():
                        state.status = BotStatus.COOLDOWN
                        state.active_task.cancel()
                        await client.send_message(TARGET_CHAT_GUID, "🛑 سیگنال توقف ارسال شد. در حال تخلیه بافر شبکه (جلوگیری از خطای سرور)...", reply_to_message_id=str(msg_id))
                        
                        # 🌟 خواب ۳ ثانیه‌ای برای بسته شدن کانکشن‌های TCP بدون آسیب به سرور روبیکا
                        await asyncio.sleep(3.0) 
                        state.status = BotStatus.IDLE
                        await client.send_message(TARGET_CHAT_GUID, "✅ ارتباط با سرور با موفقیت قطع شد. سیستم آماده است.", reply_to_message_id=str(msg_id))
                    else:
                        await client.send_message(TARGET_CHAT_GUID, "⚠️ هیچ عملیات فعالی برای توقف یافت نشد.", reply_to_message_id=str(msg_id))
                
            elif text == "#clear_chat":
                if state.status == BotStatus.PROCESSING:
                    await client.send_message(TARGET_CHAT_GUID, "⏳ ربات درگیر است.", reply_to_message_id=str(msg_id))
                else:
                    state.status = BotStatus.PROCESSING
                    await client.send_message(TARGET_CHAT_GUID, "🧹 شروع پاکسازی...", reply_to_message_id=str(msg_id))
                    state.active_task = asyncio.create_task(clear_chat_history_task(client, TARGET_CHAT_GUID))
        finally:
            message_queue.task_done()

# ==========================================
# ۵. ارکستراتور و اجرای همزمان
# ==========================================
async def main():
    connector = aiohttp.TCPConnector(limit=10)
    async with Client('time_sessions') as client, aiohttp.ClientSession(connector=connector) as shared_session:
        init_msg = await client.send_message(TARGET_CHAT_GUID, "🤖 سیستمِ یکپارچه ابری آماده دریافت فرامین است...")
        state.last_processed_id = extract_message_id(init_msg)
        
        fetch_task = asyncio.create_task(message_fetcher(client))
        process_task = asyncio.create_task(command_processor(client, shared_session))
        
        await asyncio.gather(fetch_task, process_task)

if __name__ == "__main__":
    asyncio.run(main())