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
# 🔧 設定（ここにRSSのURLを貼るだけ！）
# ==============================
RSS_FEED_URL = "https://rss.app/feeds/62Hpu1p8rfEpnRX2.xml"  # ← ★ここを書き換え！
RSS_SCAN_INTERVAL = 150  # RSSを確認する間隔（秒 推奨180以上）

# 総当たり設定
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "40"))
CHECK_DELAY = float(os.getenv("CHECK_DELAY", "0.3"))

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
TARGET_CHANNEL_ID = int(os.getenv("TARGET_CHANNEL_ID", "1538692769168625674"))

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# 正規表現：discord.gg/コード を抽出
DISCORD_RE = re.compile(r"(?:https?://)?discord\.gg/([A-Za-z0-9_\-]+)")

print("=== RSS + 総当たり 招待コード探索機 ===")
print(f"RSS: {RSS_FEED_URL} / 間隔: {RSS_SCAN_INTERVAL}秒")
print(f"総当たり: {MAX_WORKERS}並行 / {CHECK_DELAY}秒遅延")

# ==============================
# キープアライブ
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
        print(f"エラー: {e}")

# ==============================
# 本体
# ==============================
class InviteScanner:
    def __init__(self):
        self.session = None
        self.running = False
        self.found_codes = set()  # 重複除外
        self.rss_entries = set()  # RSSで取得済み投稿
        self.result_queue = asyncio.Queue()
        self._sender_task = None
        self.rss_task = None

    async def init(self):
        connector = aiohttp.TCPConnector(limit=MAX_WORKERS, force_close=True)
        self.session = aiohttp.ClientSession(
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            connector=connector
        )

    async def close(self):
        self.running = False
        if self.rss_task:
            self.rss_task.cancel()
        if self.session and not self.session.closed:
            await self.session.close()

    # ✅ RSS取得 → 新規投稿からコード抽出
    async def rss_poller(self, channel):
        await asyncio.sleep(5)
        while self.running:
            try:
                print(f"📰 RSS確認中… {RSS_FEED_URL}")
                async with self.session.get(RSS_FEED_URL, timeout=15) as resp:
                    if resp.status != 200:
                        print(f"⚠️ RSS取得失敗: 状態{resp.status}")
                        await asyncio.sleep(60)
                        continue
                    xml = await resp.text()

                feed = feedparser.parse(xml)
                new_count = 0
                found_count = 0

                for entry in feed.entries:
                    entry_id = entry.get("id", entry.get("link", ""))
                    if entry_id in self.rss_entries:
                        continue
                    self.rss_entries.add(entry_id)
                    new_count += 1

                    text = entry.get("title", "") + " " + entry.get("summary", "")
                    codes = DISCORD_RE.findall(text)
                    for code in codes:
                        if code in self.found_codes:
                            continue
                        self.found_codes.add(code)
                        found_count += 1
                        # 存在確認してキューへ
                        info = await self.check_code(code)
                        if info:
                            await self.result_queue.put(info)
                            print(f"📰 RSS発見: {code} → {info['guild']}")

                print(f"✅ RSS: 新規{new_count}件 / コード発見{found_count}件")

            except Exception as e:
                print(f"❌ RSSエラー: {e}")

            # 次回まで待機
            await asyncio.sleep(RSS_SCAN_INTERVAL)

    # ✅ ランダムコード生成
    def generate_code(self):
        chars = string.ascii_letters + string.digits
        length = random.choice([7, 8, 9, 10])
        return ''.join(random.choices(chars, k=length))

    # ✅ Discord APIで存在確認
    async def check_code(self, code: str):
        if code in self.found_codes:
            return None
        url = f"https://discord.com/api/v10/invites/{code}?with_counts=true"
        try:
            async with self.session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.found_codes.add(code)
                    return {
                        "code": code,
                        "guild": data.get("guild", {}).get("name", "Unknown"),
                        "members": data.get("approximate_member_count", 0),
                        "online": data.get("approximate_presence_count", 0),
                        "source": "RSS/総当たり"
                    }
                elif resp.status == 429:
                    retry = float(resp.headers.get("Retry-After", 3))
                    print(f"⚠️ API制限: {retry}秒待機")
                    await asyncio.sleep(retry)
        except Exception as e:
            print(f"check_code {code}: {e}")
        return None

    # ✅ 総当たりワーカー
    async def brute_worker(self):
        while self.running:
            code = self.generate_code()
            info = await self.check_code(code)
            if info:
                await self.result_queue.put(info)
                print(f"🔍 総当たり発見: {code} → {info['guild']}")
            await asyncio.sleep(CHECK_DELAY)

    # ✅ 送信タスク
    async def sender_task(self, channel):
        while self.running or not self.result_queue.empty():
            try:
                info = await asyncio.wait_for(self.result_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            try:
                embed = discord.Embed(
                    title="🎉 有効な招待コード発見！",
                    url=f"https://discord.gg/{info['code']}",
                    color=0x2ecc71,
                    timestamp=datetime.now()
                )
                embed.add_field(name="コード", value=f"```https://discord.gg/{info['code']}```", inline=False)
                embed.add_field(name="サーバー名", value=info["guild"], inline=True)
                embed.add_field(name="メンバー数", value=str(info["members"]), inline=True)
                embed.add_field(name="オンライン", value=str(info["online"]), inline=True)
                embed.add_field(name="取得元", value=info.get("source", "不明"), inline=False)
                await channel.send(embed=embed)
            except Exception as e:
                print(f"送信エラー: {e}")

    # ✅ 一括開始
    async def start_all(self, channel):
        self.running = True
        self._sender_task = asyncio.create_task(self.sender_task(channel))
        self.rss_task = asyncio.create_task(self.rss_poller(channel))
        workers = [asyncio.create_task(self.brute_worker()) for _ in range(MAX_WORKERS)]
        await asyncio.gather(*workers, return_exceptions=True)

scanner = InviteScanner()

@bot.event
async def on_ready():
    print(f"✅ Bot起動: {bot.user}")
    await scanner.init()

# ==================== コマンド ====================
@bot.command(name="scan_start")
@commands.has_role("TISN管理者")
async def scan_start(ctx):
    """🚀 RSS監視＋総当たり 一括開始"""
    if scanner.running:
        await ctx.send("❌ 既に実行中です")
        return
    target = bot.get_channel(TARGET_CHANNEL_ID) or ctx.channel
    if not isinstance(target, discord.TextChannel):
        await ctx.send("❌ テキストチャンネルのみ")
        return

    await ctx.send(
        f"🚀 **RSS監視＋総当たり 開始**\n"
        f"・RSS確認: {RSS_SCAN_INTERVAL}秒毎\n"
        f"・総当たり: {MAX_WORKERS}並行\n"
        f"・送信先: {target.mention}"
    )
    await scanner.start_all(target)

@bot.command(name="scan_stop")
@commands.has_role("TISN管理者")
async def scan_stop(ctx):
    """⏹️ 停止"""
    if not scanner.running:
        await ctx.send("❌ 実行されていません")
        return
    scanner.running = False
    await ctx.send("⏹️ 停止しました")

@bot.command(name="scan_status")
@commands.has_role("TISN管理者")
async def scan_status(ctx):
    """📊 状態確認"""
    await ctx.send(
        f"実行中: {'✅ はい' if scanner.running else '❌ いいえ'}\n"
        f"発見済コード: {len(scanner.found_codes)}件\n"
        f"RSS取得済: {len(scanner.rss_entries)}件"
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