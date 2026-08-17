import discord
from discord.ext import commands
import aiohttp
import asyncio
import random
import string
import os
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime
from dotenv import load_dotenv
import re
import feedparser

# ==============================
# 🔧 設定 ✅ nitter.net 正規URL
# ==============================
RSS_FEEDS = [
    {"name": "X", "url": "https://nitter.net/search?f=tweets&q=discord.gg&rss=1"},
]
RSS_SCAN_INTERVAL = 70

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))
CHECK_DELAY = float(os.getenv("CHECK_DELAY", "1.5"))

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
TARGET_CHANNEL_ID = 1538692769168625674

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

DISCORD_RE = re.compile(r"discord\.gg/([A-Za-z0-9_\-]+)", re.IGNORECASE)

print(f"=== 最終版｜nitter.net 正規URL ===")
print(f"監視先: {[f['url'] for f in RSS_FEEDS]}")

# ==============================
# 🔧 キープアライブ
# ==============================
class _KeepAliveHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot is alive!")
    def log_message(self, format, *args):
        pass

def start_keep_alive():
    try:
        port = int(os.environ.get("PORT", 8080))
        server = HTTPServer(("0.0.0.0", port), _KeepAliveHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
    except Exception as e:
        print(f"キープアライブエラー: {e}")

# ==============================
# 🤖 本体
# ==============================
class InviteScanner:
    def __init__(self):
        self.session = None
        self.rss_running = False
        self.brute_running = False
        self.found_codes = set()
        self.rss_entries = {feed["url"]: set() for feed in RSS_FEEDS}
        self.result_queue = asyncio.Queue()
        self._sender_task = None
        self.rss_tasks = []
        self.brute_tasks = []
        self.lock = asyncio.Lock()

    async def init(self):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/rss+xml, application/xml, text/plain, */*",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        }
        connector = aiohttp.TCPConnector(limit=10, force_close=True)
        self.session = aiohttp.ClientSession(headers=headers, connector=connector)

    async def close(self):
        self.rss_running = False
        self.brute_running = False
        for t in self.rss_tasks + self.brute_tasks:
            t.cancel()
        if self.session and not self.session.closed:
            await self.session.close()

    # ======================================
    # 📰 RSS監視
    # ======================================
    async def rss_poller_single(self, feed_info):
        name, url = feed_info["name"], feed_info["url"]
        await asyncio.sleep(RSS_SCAN_INTERVAL * RSS_FEEDS.index(feed_info))

        while self.rss_running:
            try:
                print(f"📰 [{name}] RSS確認中…")
                async with self.session.get(url, timeout=20) as resp:
                    if resp.status in [503, 502, 500]:
                        print(f"⚠️ [{name}] サーバー一時停止({resp.status}) → 60秒待機")
                        await asyncio.sleep(60)
                        continue
                    if resp.status == 403:
                        print(f"⚠️ [{name}] 403 → 60秒待機")
                        await asyncio.sleep(60)
                        continue
                    if resp.status != 200:
                        print(f"⚠️ [{name}] 状態{resp.status} → 30秒待機")
                        await asyncio.sleep(30)
                        continue
                    xml = await resp.text()

                raw_codes = list(set(DISCORD_RE.findall(xml)))
                if raw_codes:
                    print(f"🔥 [{name}] 生XMLから直接発見！: {raw_codes[:10]}")

                feed = feedparser.parse(xml)
                new_posts = 0
                extracted = 0
                valid = 0

                for entry in feed.entries:
                    entry_id = entry.get("id", entry.get("link", ""))
                    if entry_id in self.rss_entries[url]:
                        continue
                    self.rss_entries[url].add(entry_id)
                    new_posts += 1

                    title = entry.get("title", "")
                    summary = entry.get("summary", "")
                    content = entry.get("content", [{}])[0].get("value", "")
                    text_all = f"{title} {summary} {content}"

                    codes = DISCORD_RE.findall(text_all)
                    if codes:
                        print(f"🔍 [{name}] 投稿から発見: {codes[:10]}")

                    for code in codes:
                        code = code.lower()
                        if code in self.found_codes:
                            continue
                        info = await self.check_code(code)
                        if info:
                            info["source"] = f"RSS:{name}"
                            valid += 1
                            await self.result_queue.put(info)
                            print(f"✅ [{name}] 有効: discord.gg/{code} → {info['guild']}")
                    extracted += len(codes)

                print(f"📊 [{name}] 新規{new_posts}件 / 抽出{extracted}件 / 有効{valid}件")

            except Exception as e:
                print(f"❌ [{name}] エラー: {e}")
                await asyncio.sleep(30)

            await asyncio.sleep(RSS_SCAN_INTERVAL)

    # ======================================
    # 🔍 コード確認
    # ======================================
    def generate_code(self):
        chars = string.ascii_letters + string.digits
        length = random.choice([7, 8, 9])
        return ''.join(random.choices(chars, k=length))

    async def check_code(self, code: str):
        async with self.lock:
            code = code.lower()
            if code in self.found_codes:
                return None
            url = f"https://discord.com/api/v10/invites/{code}?with_counts=true"
            try:
                async with self.session.get(url, timeout=15) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self.found_codes.add(code)
                        return {
                            "code": code,
                            "guild": data.get("guild", {}).get("name", "Unknown"),
                            "members": data.get("approximate_member_count", 0),
                            "online": data.get("approximate_presence_count", 0),
                        }
                    elif resp.status == 403:
                        print(f"⚠️ 403: discord.gg/{code}")
                        await asyncio.sleep(5)
                    elif resp.status == 429:
                        retry = float(resp.headers.get("Retry-After", 10))
                        print(f"⚠️ 制限: {retry}秒待機")
                        await asyncio.sleep(retry + 3)
            except Exception as e:
                print(f"確認エラー: {e}")
        return None

    async def brute_worker(self):
        while self.brute_running:
            code = self.generate_code()
            info = await self.check_code(code)
            if info:
                info["source"] = "総当たり"
                await self.result_queue.put(info)
                print(f"🔍 総当たり発見: {code}")
            await asyncio.sleep(CHECK_DELAY)

    # ======================================
    # 📤 通知
    # ======================================
    async def sender_task(self, channel):
        print(f"📤 送信タスク起動: {channel.name}")
        while (self.rss_running or self.brute_running) or not self.result_queue.empty():
            try:
                info = await asyncio.wait_for(self.result_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            try:
                invite_url = f"https://discord.gg/{info['code']}"
                embed = discord.Embed(
                    title="🎉 有効な招待コード発見！",
                    color=0x2ecc71,
                    timestamp=datetime.now()
                )
                embed.add_field(name="サーバー名", value=info["guild"], inline=True)
                embed.add_field(name="メンバー数", value=str(info["members"]), inline=True)
                embed.add_field(name="オンライン", value=str(info["online"]), inline=True)
                embed.add_field(name="📥 取得元", value=info.get("source", "不明"), inline=False)

                await channel.send(embed=embed)
                await channel.send(f"👉 **{invite_url}**")
                print(f"📤 送信完了: {invite_url}")
            except Exception as e:
                print(f"❌ 送信エラー: {e}")
        print("📤 送信タスク終了")

    # ======================================
    # 🚀 起動/停止
    # ======================================
    async def start_rss_all(self, channel):
        if self.rss_running:
            return False
        self.rss_running = True
        self._sender_task = asyncio.create_task(self.sender_task(channel))
        self.rss_tasks = [asyncio.create_task(self.rss_poller_single(feed)) for feed in RSS_FEEDS]
        return True

    async def start_brute(self, channel):
        if self.brute_running:
            return False
        self.brute_running = True
        if not self._sender_task or self._sender_task.done():
            self._sender_task = asyncio.create_task(self.sender_task(channel))
        self.brute_tasks = [asyncio.create_task(self.brute_worker()) for _ in range(MAX_WORKERS)]
        return True

    async def stop_all(self):
        self.rss_running = self.brute_running = False
        for t in self.rss_tasks + self.brute_tasks:
            t.cancel()
        await asyncio.sleep(1)

scanner = InviteScanner()

@bot.event
async def on_ready():
    print(f"✅ Bot起動完了: {bot.user}")
    await scanner.init()

# ==================== 🎮 コマンド ====================
@bot.command(name="rss_start")
@commands.has_role("TISN管理者")
async def rss_start(ctx):
    target = bot.get_channel(TARGET_CHANNEL_ID) or ctx.channel
    if await scanner.start_rss_all(target):
        names = " / ".join([f["name"] for f in RSS_FEEDS])
        await ctx.send(f"✅ RSS監視開始！\n監視先: {names}\n間隔: {RSS_SCAN_INTERVAL}秒")
    else:
        await ctx.send("❌ 既に実行中です")

@bot.command(name="brute_start")
@commands.has_role("TISN管理者")
async def brute_start(ctx):
    target = bot.get_channel(TARGET_CHANNEL_ID) or ctx.channel
    if await scanner.start_brute(target):
        await ctx.send(f"✅ 総当たり開始！（並行:{MAX_WORKERS}）")
    else:
        await ctx.send("❌ 既に実行中です")

@bot.command(name="scan_start_all")
@commands.has_role("TISN管理者")
async def scan_start_all(ctx):
    target = bot.get_channel(TARGET_CHANNEL_ID) or ctx.channel
    await scanner.start_rss_all(target)
    await scanner.start_brute(target)
    await ctx.send("🚀 RSS＋総当たり 一斉開始！")

@bot.command(name="scan_stop")
@commands.has_role("TISN管理者")
async def scan_stop(ctx):
    await scanner.stop_all()
    await ctx.send("⏹️ 全て停止しました")

@bot.command(name="scan_status")
@commands.has_role("TISN管理者")
async def scan_status(ctx):
    total_entries = sum(len(v) for v in scanner.rss_entries.values())
    await ctx.send(
        f"RSS実行中: {'✅ はい' if scanner.rss_running else '❌ いいえ'}\n"
        f"総当たり中: {'✅ はい' if scanner.brute_running else '❌ いいえ'}\n"
        f"RSS取得済: {total_entries}件\n"
        f"発見済コード: {len(scanner.found_codes)}件"
    )

@bot.event
async def on_disconnect():
    await scanner.close()

if __name__ == "__main__":
    if not BOT_TOKEN:
        print("❌ BOT_TOKEN が設定されていません")
        exit(1)
    start_keep_alive()
    bot.run(BOT_TOKEN)