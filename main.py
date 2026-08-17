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

# ==============================
# ✅ X検索ページを直接取得 + aiohttp制限を1MBに緩和
# ==============================
SEARCH_URLS = [
    {"name": "X-検索1", "url": "https://x.com/search?q=discord+invite&f=live"},
    {"name": "X-検索2", "url": "https://x.com/search?q=discord.gg&f=live"},
    {"name": "X-検索3", "url": "https://x.com/search?q=discord+server&f=live"},
]
SCAN_INTERVAL = 180
MAX_RETRY = 3

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))
CHECK_DELAY = float(os.getenv("CHECK_DELAY", "1.5"))

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
TARGET_CHANNEL_ID = 1538692769168625674

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

DISCORD_RE = re.compile(r"discord\.gg/([A-Za-z0-9_\-]+)", re.IGNORECASE)

print(f"=== ✅ aiohttp制限を1MBに緩和・X直接取得 ===")
for u in SEARCH_URLS:
    print(f"  {u['name']}: {u['url']}")

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
        self.web_running = False
        self.brute_running = False
        self.found_codes = set()
        self.seen_links = {u["url"]: set() for u in SEARCH_URLS}
        self.result_queue = asyncio.Queue()
        self._sender_task = None
        self.current_url_index = 0
        self.fail_count = 0

    async def init(self):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Cache-Control": "max-age=0",
        }
        connector = aiohttp.TCPConnector(limit=5, force_close=False)
        # ======================================
        # ✅ 核心修正：ヘッダーサイズ制限を1MBに引き上げ！
        # ======================================
        self.session = aiohttp.ClientSession(
            headers=headers,
            connector=connector,
            max_line_size=1024*1024,   # ✅ 1MB
            max_field_size=1024*1024,  # ✅ 1MB
        )

    async def close(self):
        self.web_running = False
        self.brute_running = False
        if self.session and not self.session.closed:
            await self.session.close()

    # ======================================
    # 📰 直接取得タスク
    # ======================================
    async def web_poller(self):
        while self.web_running:
            info = SEARCH_URLS[self.current_url_index]
            name, url = info["name"], info["url"]

            try:
                print(f"📰 [{name}] ページ取得中…")
                async with self.session.get(url, timeout=30, allow_redirects=True) as resp:
                    print(f"📡 [{name}] ステータス: {resp.status}")

                    if resp.status >= 500:
                        self.fail_count += 1
                        print(f"⚠️ [{name}] サーバーエラー → {self.fail_count}/{MAX_RETRY}")
                        if self.fail_count >= MAX_RETRY:
                            self.fail_count = 0
                            self.current_url_index = (self.current_url_index + 1) % len(SEARCH_URLS)
                            print(f"🔄 切り替え → {SEARCH_URLS[self.current_url_index]['name']}")
                            await asyncio.sleep(10)
                            continue
                        await asyncio.sleep(30)
                        continue

                    if resp.status in [401, 403, 429]:
                        print(f"⚠️ [{name}] アクセス制限")
                        self.current_url_index = (self.current_url_index + 1) % len(SEARCH_URLS)
                        await asyncio.sleep(60)
                        continue
                    if resp.status != 200:
                        print(f"⚠️ [{name}] 状態: {resp.status}")
                        await asyncio.sleep(30)
                        continue

                    self.fail_count = 0
                    html = await resp.text()
                    size = len(html)
                    print(f"✅ [{name}] 取得成功！ サイズ: {size}文字")

                    if size < 1000:
                        print(f"⚠️ [{name}] 内容が少ない → 次へ")
                        self.current_url_index = (self.current_url_index + 1) % len(SEARCH_URLS)
                        await asyncio.sleep(5)
                        continue

                codes = list(set(DISCORD_RE.findall(html)))
                if codes:
                    print(f"🔥 [{name}] ページから発見: {codes[:15]}")

                new_count = 0
                valid_count = 0
                for code in codes:
                    code = code.lower()
                    if code in self.seen_links[url]:
                        continue
                    self.seen_links[url].add(code)
                    new_count += 1

                    if code in self.found_codes:
                        continue
                    info = await self.check_code(code)
                    if info:
                        info["source"] = f"Web:{name}"
                        valid_count += 1
                        await self.result_queue.put(info)
                        print(f"✅ 有効: discord.gg/{code} → {info['guild']}")

                print(f"📊 [{name}] 新規{new_count}件 / 有効{valid_count}件")

            except Exception as e:
                self.fail_count += 1
                print(f"❌ [{name}] エラー: {type(e).__name__}: {e}")
                if self.fail_count >= MAX_RETRY:
                    self.fail_count = 0
                    self.current_url_index = (self.current_url_index + 1) % len(SEARCH_URLS)
                    print(f"🔄 切り替え → {SEARCH_URLS[self.current_url_index]['name']}")
                await asyncio.sleep(10)

            await asyncio.sleep(SCAN_INTERVAL)

    # ======================================
    # 🔍 コード確認
    # ======================================
    def generate_code(self):
        chars = string.ascii_letters + string.digits
        length = random.choice([7, 8, 9])
        return ''.join(random.choices(chars, k=length))

    async def check_code(self, code: str):
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
                elif resp.status == 429:
                    retry = float(resp.headers.get("Retry-After", 10))
                    print(f"⚠️ API制限: {retry}秒")
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
        print(f"📤 送信タスク起動")
        while (self.web_running or self.brute_running) or not self.result_queue.empty():
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
    async def start_web_monitor(self, channel):
        if self.web_running:
            return False
        self.web_running = True
        self._sender_task = asyncio.create_task(self.sender_task(channel))
        asyncio.create_task(self.web_poller())
        return True

    async def start_brute(self, channel):
        if self.brute_running:
            return False
        self.brute_running = True
        if not self._sender_task or self._sender_task.done():
            self._sender_task = asyncio.create_task(self.sender_task(channel))
        for _ in range(MAX_WORKERS):
            asyncio.create_task(self.brute_worker())
        return True

    async def stop_all(self):
        self.web_running = False
        self.brute_running = False
        await asyncio.sleep(1)

scanner = InviteScanner()

@bot.event
async def on_ready():
    print(f"✅ Bot起動: {bot.user}")
    await scanner.init()

# ==================== 🎮 コマンド ====================
@bot.command(name="web_start")
@commands.has_role("TISN管理者")
async def web_start(ctx):
    target = bot.get_channel(TARGET_CHANNEL_ID) or ctx.channel
    if await scanner.start_web_monitor(target):
        await ctx.send("✅ X検索直接監視開始！（制限緩和済）")
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
    await scanner.start_web_monitor(target)
    await scanner.start_brute(target)
    await ctx.send("🚀 検索監視＋総当たり 一斉開始！")

@bot.command(name="scan_stop")
@commands.has_role("TISN管理者")
async def scan_stop(ctx):
    await scanner.stop_all()
    await ctx.send("⏹️ 全て停止")

@bot.command(name="scan_status")
@commands.has_role("TISN管理者")
async def scan_status(ctx):
    total_seen = sum(len(v) for v in scanner.seen_links.values())
    await ctx.send(
        f"検索監視: {'✅ はい' if scanner.web_running else '❌ いいえ'}\n"
        f"現在: {SEARCH_URLS[scanner.current_url_index]['name']}\n"
        f"連続失敗: {scanner.fail_count}/{MAX_RETRY}\n"
        f"抽出済コード: {total_seen}件\n"
        f"有効確認済: {len(scanner.found_codes)}件"
    )

@bot.event
async def on_disconnect():
    await scanner.close()

if __name__ == "__main__":
    if not BOT_TOKEN:
        print("❌ BOT_TOKENなし")
        exit(1)
    start_keep_alive()
    bot.run(BOT_TOKEN)