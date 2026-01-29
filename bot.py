import os
import asyncio
import logging
import pymongo
import base64
from aiohttp import web
from pyrogram import Client, filters, enums, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait

# --- CONFIGURATION ---
try:
    API_ID = int(os.environ.get("API_ID"))
    API_HASH = os.environ.get("API_HASH")
    BOT_TOKEN = os.environ.get("BOT_TOKEN")
    MAIN_CHANNEL_ID = int(os.environ.get("MAIN_CHANNEL_ID")) 
    DB_CHANNEL_ID = int(os.environ.get("DB_CHANNEL_ID"))   
    MONGO_URL = os.environ.get("MONGO_URL")
    
    if not all([API_ID, API_HASH, BOT_TOKEN, MAIN_CHANNEL_ID, DB_CHANNEL_ID, MONGO_URL]):
        raise ValueError("Missing Variables")
except Exception as e:
    print(f"❌ Config Error: {e}", flush=True)
    raise SystemExit

# --- DATABASE ---
mongo_client = pymongo.MongoClient(MONGO_URL)
db = mongo_client["TitanFactoryBot"]
post_queue = db["post_queue"]     
active_posts = db["active_posts"] 

app = Client("render_manager", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# --- HELPERS ---
def encode_ids(file_ids):
    if not file_ids: return "0"
    if len(file_ids) == 1: return str(file_ids[0])
    sorted_ids = sorted(file_ids)
    if sorted_ids[-1] - sorted_ids[0] == len(sorted_ids) - 1:
        return f"{sorted_ids[0]}-{sorted_ids[-1]}"
    return ".".join(map(str, sorted_ids))

async def decode_ids(client, code):
    ids = []
    if "-" in code:
        start, end = map(int, code.split("-"))
        ids = list(range(start, end + 1))
    elif "." in code:
        ids = list(map(int, code.split(".")))
    else:
        ids = [int(code)]
    
    messages = []
    for i in range(0, len(ids), 200):
        try:
            batch = await client.get_messages(DB_CHANNEL_ID, ids[i:i+200])
            messages.extend([m for m in batch if m])
        except FloodWait as e:
            await asyncio.sleep(e.value)
            batch = await client.get_messages(DB_CHANNEL_ID, ids[i:i+200])
            messages.extend([m for m in batch if m])
    return messages

def str_to_b64(text):
    return base64.urlsafe_b64encode(text.encode()).decode().strip("=")

def b64_to_str(text):
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4)).decode()

# --- WATCHER ---
async def queue_watcher():
    print("👀 Watcher Started... Waiting for jobs.", flush=True)
    
    # 🔍 DIAGNOSTIC: Check if items exist right now
    pending_count = post_queue.count_documents({"status": "pending_post"})
    print(f"📊 DEBUG: Pending Jobs in DB right now: {pending_count}", flush=True)

    while True:
        job = post_queue.find_one({"status": "pending_post"})
        if job:
            print(f"⚡ Found Job: {job.get('anime')} ({job.get('resolution')}p)", flush=True)
            try:
                anime_name = job["anime"]
                res = job["resolution"]
                file_ids = job["file_ids"]

                payload_raw = f"get-{encode_ids(file_ids)}"
                payload_hash = str_to_b64(payload_raw)
                bot_username = app.me.username
                link = f"https://t.me/{bot_username}?start={payload_hash}"
                
                existing_post = active_posts.find_one({"anime": anime_name})
                new_button = InlineKeyboardButton(f"{res}p", url=link)

                if existing_post:
                    print(f"✏️ Editing existing post for {anime_name}", flush=True)
                    msg_id = existing_post["message_id"]
                    current_markup = existing_post.get("buttons", [])
                    
                    exists = False
                    for row in current_markup:
                        for btn in row:
                            if btn['text'] == f"{res}p": exists = True
                    
                    if not exists:
                        if not current_markup: current_markup = [[new_button]]
                        else: current_markup[0].append(dict(text=f"{res}p", url=link))
                        
                        def sort_key(b):
                            t = b['text'].replace("p", "")
                            return int(t) if t.isdigit() else 9999
                        current_markup[0].sort(key=sort_key)

                        final_kb = []
                        for row in current_markup:
                            r = [InlineKeyboardButton(b['text'], url=b['url']) for b in row]
                            final_kb.append(r)

                        try:
                            await app.edit_message_reply_markup(MAIN_CHANNEL_ID, msg_id, reply_markup=InlineKeyboardMarkup(final_kb))
                            active_posts.update_one({"_id": existing_post["_id"]}, {"$set": {"buttons": current_markup}})
                            print("✅ Post Updated Successfully.", flush=True)
                        except Exception as e:
                            print(f"⚠️ Edit Failed: {e}", flush=True)
                else:
                    print(f"🆕 Creating NEW post for {anime_name}", flush=True)
                    caption = (
                        f"**{anime_name}**\n\n"
                        f"**🎭 Genres:** {job['genres']}\n"
                        f"**⭐ Score:** {job['score']}  |  **Type:** {job['type']}\n"
                        f"**📖 Synopsis:**\n__{job['synopsis']}__\n\n"
                        f"**Join:** @YourChannelLink"
                    )
                    keyboard = InlineKeyboardMarkup([[new_button]])
                    try:
                        if job['poster']:
                            sent = await app.send_photo(MAIN_CHANNEL_ID, job['poster'], caption=caption, reply_markup=keyboard)
                        else:
                            sent = await app.send_message(MAIN_CHANNEL_ID, caption, reply_markup=keyboard)
                        
                        active_posts.insert_one({
                            "anime": anime_name,
                            "message_id": sent.id,
                            "buttons": [[dict(text=f"{res}p", url=link)]]
                        })
                        print("✅ New Post Created Successfully.", flush=True)
                    except Exception as e:
                        print(f"❌ Post Failed (Check Permissions/ID): {e}", flush=True)

                post_queue.update_one({"_id": job["_id"]}, {"$set": {"status": "done"}})
            except Exception as e:
                print(f"❌ CRITICAL Worker Error: {e}", flush=True)
                post_queue.update_one({"_id": job["_id"]}, {"$set": {"status": "error", "error": str(e)}})
        
        await asyncio.sleep(5)

# --- USER BOT ---
@app.on_message(filters.command("start") & filters.private)
async def start_handler(client, message):
    if len(message.command) < 2: return await message.reply("Bot Online.")
    try:
        payload = message.command[1]
        decoded = b64_to_str(payload)
        if decoded.startswith("get-"):
            code = decoded.split("get-")[1]
            status = await message.reply("📂 **Fetching Files...**")
            msgs = await decode_ids(client, code)
            if not msgs: return await status.edit("❌ Files removed.")
            await status.delete()
            for m in msgs:
                try: await m.copy(message.chat.id, caption=m.caption, protect_content=False)
                except FloodWait as e: await asyncio.sleep(e.value); await m.copy(message.chat.id, caption=m.caption)
                except: pass
    except Exception as e: await message.reply(f"❌ Error: {e}")

# --- WEB SERVER ---
async def web_server():
    async def handle(request): return web.Response(text="Bot Alive")
    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"🌐 Web Server on {port}", flush=True)

if __name__ == "__main__":
    app.start()
    print("🤖 Render Bot Online. Starting loops...", flush=True)
    loop = asyncio.get_event_loop()
    loop.create_task(queue_watcher())
    loop.create_task(web_server())
    idle()
    app.stop()
