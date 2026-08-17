# 技術的学習用 - 実際の使用は推奨されません
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
import time

# ==============================
# キープアライブサーバー（Railway用）
# ==============================
class _KeepAliveHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot is alive!")

    def log_message(self, format, *args):
        pass  # ログを出力しない

def start_keep_alive():
    """Railwayでスリープ防止するためのHTTPサーバー"""
    try:
        port = int(os.environ.get("PORT", 8080))
        server = HTTPServer(("0.0.0.0", port), _KeepAliveHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        print(f"キープアライブサーバー起動: ポート {port}")
    except Exception as e:
        print(f"Keep-alive サーバー起動失敗: {e}")

# ==============================
# Bot設定
# ==============================
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
TARGET_CHANNEL_ID = int(os.getenv("TARGET_CHANNEL_ID", "1538692769168625674"))

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "50"))
CHECK_DELAY = float(os.getenv("CHECK_DELAY", "0.5"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "1000"))

NITTER_DEFAULT_INSTANCES = [
    "https://nitter.net",
    "https://nitter.kavin.rocks",
]
# allow underscore in invite codes too
NITTER_INVITE_RE = re.compile(r"(?:https?://)?discord\.gg/([A-Za-z0-9_\-]+)")

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

print("Starting main.py")
print(f"BOT_TOKEN set: {'yes' if BOT_TOKEN else 'no'}")
print(f"CHECK_DELAY={CHECK_DELAY}, MAX_WORKERS={MAX_WORKERS}, MAX_ATTEMPTS={MAX_ATTEMPTS}")

class InviteScanner:
    def __init__(self):
        self.session = None
        self.found = []
        self.checked = 0
        self.running = False
        # asyncio.Lock can be created outside loop in modern Python
        self.lock = asyncio.Lock()
        self.forever = False
        self.result_queue = asyncio.Queue()
        self._sender_task = None
        # Nitter-related
        self.nitter_running = False
        self.nitter_task = None
        self.nitter_seen = {}
        
    async def init(self):
        # create a shared aiohttp session
        self.session = aiohttp.ClientSession(
            headers={"User-Agent": "DiscordBot (1.0)"}
        )
    
    async def close(self):
        self.running = False
        self.nitter_running = False
        # wait for sender to finish flushing
        if self._sender_task:
            try:
                await asyncio.wait_for(self.result_queue.join(), timeout=10)
            except Exception:
                pass
            try:
                await asyncio.wait_for(self._sender_task, timeout=10)
            except Exception:
                pass
        if self.nitter_task:
            try:
                await asyncio.wait_for(self.nitter_task, timeout=5)
            except Exception:
                pass
        if self.session:
            await self.session.close()
    
    def generate_code(self):
        chars = string.ascii_lowercase + string.digits
        length = random.choice([7, 8, 9, 10])
        return ''.join(random.choices(chars, k=length))
    
    async def _sleep_interruptible(self, seconds: float):
        # Sleep in small increments so we can stop early if either running flag is cleared
        remaining = seconds
        chunk = 1.0
        # check both running flags so nitter poller and workers can be interrupted
        while remaining > 0 and (getattr(self, 'running', False) or getattr(self, 'nitter_running', False)):
            await asyncio.sleep(min(chunk, remaining))
            remaining -= chunk
            # loop will exit early if flags turned False
    
    async def check(self, code: str):
        if not self.session:
            # session not initialized
            return None
        url = f"https://discord.com/api/v10/invites/{code}?with_counts=true"
        try:
            async with self.session.get(url, timeout=10) as resp:
                async with self.lock:
                    self.checked += 1
                
                if resp.status == 200:
                    data = await resp.json()
                    return {
                        "code": code,
                        "guild": data.get("guild", {}).get("name", "Unknown"),
                        "guild_id": data.get("guild", {}).get("id"),
                        "members": data.get("approximate_member_count", 0),
                        "online": data.get("approximate_presence_count", 0)
                    }
                elif resp.status == 429:
                    retry = float(resp.headers.get("Retry-After", 60))
                    print(f"レート制限: {retry}秒待機")
                    # wait but allow early exit if scanner is stopped
                    await self._sleep_interruptible(retry)
        except Exception as e:
            # log exception for debugging
            print(f"check() exception for {code}: {e}")
        return None
    
    async def _sender(self, channel: discord.TextChannel):
        # Drain results from the queue and send them. Keep running while scanner is running or queue not empty
        while (getattr(self, 'running', False) or not self.result_queue.empty() or getattr(self, 'nitter_running', False)):
            result = None
            try:
                # wait up to 1s for a result so we can re-check flags periodically
                result = await asyncio.wait_for(self.result_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            try:
                embed = discord.Embed(
                    title="🔍 招待コード発見",
                    url=f"https://discord.gg/{result['code']}",
                    color=0x00ff00,
                    timestamp=datetime.now()
                )
                embed.add_field(name="コード", value=f"`{result['code']}`", inline=False)
                embed.add_field(name="サーバー", value=result.get('guild','Unknown'), inline=True)
                embed.add_field(name="メンバー", value=result.get('members',0), inline=True)

                try:
                    await channel.send(embed=embed)
                except discord.Forbidden:
                    print(f"送信権限なし: {channel.id}")
                    # stop everything if we cannot send
                    self.running = False
                    break
                except Exception as e:
                    print(f"送信エラー: {e}")
                    # on error, wait a bit and continue; the item is considered handled to avoid infinite retry
                    await asyncio.sleep(1)
                
                print(f"[SENT] {result['code']} -> {result.get('guild','Unknown')}")
            finally:
                if result is not None:
                    try:
                        self.result_queue.task_done()
                    except Exception:
                        pass

    async def worker(self, channel: discord.TextChannel):
        while getattr(self, 'running', False) and (self.forever or self.checked < MAX_ATTEMPTS):
            code = self.generate_code()
            result = await self.check(code)
            
            if result:
                self.found.append(result)
                # Enqueue result for sender task instead of sending directly
                try:
                    await self.result_queue.put(result)
                except Exception as e:
                    print(f"キュー格納エラー: {e}")
                
                print(f"[FOUND-ENQUEUED] {result['code']} -> {result['guild']}")
            
            await asyncio.sleep(CHECK_DELAY)
            
            if self.checked % 50 == 0:
                print(f"進捗: {self.checked}件チェック済み / 発見: {len(self.found)}件")

    # ------------------------- Nitter poller -------------------------
    async def _fetch_nitter_html(self, base_url: str, path: str = "/search?f=tweets&q=discord.gg"):
        url = base_url.rstrip("/") + path
        try:
            async with self.session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    return await resp.text()
                else:
                    print(f"Nitter fetch failed: {base_url} status={resp.status}")
        except Exception as e:
            print(f"Nitter fetch error {base_url}: {e}")
        return ""

    async def nitter_poller(self, instances: list, interval: float = 30.0, seen_ttl: int = 60*60*12, verify: bool = True):
        """
        instances: list of Nitter base URLs (e.g. https://nitter.net)
        interval: how long to wait between polls (seconds)
        seen_ttl: how long to keep a seen code in seconds
        verify: if True, call self.check(code) to validate via Discord API before enqueuing
        """
        self.nitter_running = True
        self.nitter_seen = getattr(self, "nitter_seen", {})
        while self.nitter_running:
            # cleanup old seen entries
            now = time.time()
            for k, t in list(self.nitter_seen.items()):
                if now - t > seen_ttl:
                    del self.nitter_seen[k]

            # rotate instances to spread load
            random.shuffle(instances)
            for inst in instances:
                if not self.nitter_running:
                    break
                html = await self._fetch_nitter_html(inst)
                if not html:
                    await asyncio.sleep(1)
                    continue

                codes = set(NITTER_INVITE_RE.findall(html))
                for code in codes:
                    if not self.nitter_running:
                        break
                    if code in self.nitter_seen:
                        continue
                    # mark seen immediately to avoid duplicates across instances
                    self.nitter_seen[code] = now
                    if verify:
                        # validate via existing check() which handles rate limits
                        try:
                            res = await self.check(code)
                        except Exception as e:
                            print(f"nitter check error for {code}: {e}")
                            res = None
                        if res:
                            await self.result_queue.put(res)
                            self.found.append(res)
                            print(f"[NITTER->ENQUEUE] {code} -> {res.get('guild')}")
                    else:
                        # minimal enqueue without verification (less API calls)
                        res = {"code": code, "guild": "Unknown (from Nitter)", "members": 0, "online": 0}
                        await self.result_queue.put(res)
                        self.found.append(res)
                        print(f"[NITTER->ENQUEUE(NO-VERIFY)] {code}")

                # small delay between instances to be gentle
                await asyncio.sleep(1)

            # wait interval before next full poll, but allow early stop
            slept = 0.0
            chunk = 1.0
            while slept < interval and self.nitter_running:
                await asyncio.sleep(min(chunk, interval - slept))
                slept += chunk

        # poller exiting
        print("Nitter poller stopped")

scanner = InviteScanner()

@bot.event
async def on_ready():
    print(f"Bot起動: {bot.user}")
    print(f"TARGET_CHANNEL_ID: {TARGET_CHANNEL_ID}")
    await scanner.init()

# ------------------------- Commands -------------------------
@bot.command(name="nitter_scan_start", aliases=["twitter_scan_start"])
@commands.has_role("TISN管理者")
async def nitter_scan_start(ctx, interval: int = None):
    """Nitter（Twitter代替）ポーリング開始"""
    if scanner.nitter_task and not scanner.nitter_task.done():
        await ctx.send("❌ 既に Nitter ポーラーが実行中です。")
        return

    instances_env = os.getenv("NITTER_INSTANCES", "")
    if instances_env:
        instances = [s.strip() for s in instances_env.split(",") if s.strip()]
    else:
        instances = NITTER_DEFAULT_INSTANCES

    poll_interval = interval if interval is not None else int(os.getenv("NITTER_POLL_INTERVAL", "30"))
    scanner.nitter_task = asyncio.create_task(scanner.nitter_poller(instances, interval=poll_interval))
    await ctx.send(f"🔎 Nitter ポーリング開始（interval={poll_interval}s）")

@bot.command(name="nitter_scan_stop", aliases=["twitter_scan_stop"])
@commands.has_role("TISN管理者")
async def nitter_scan_stop(ctx):
    """Nitter（Twitter代替）ポーリング停止"""
    if not scanner.nitter_task or scanner.nitter_task.done():
        await ctx.send("実行していません。")
        return
    scanner.nitter_running = False
    try:
        await scanner.nitter_task
    except Exception:
        pass
    scanner.nitter_task = None
    await ctx.send("⏹️ Nitter ポーリング停止しました。")

@bot.command()
@commands.has_role("TISN管理者")
async def scan(ctx, duration: int = 60):
    """招待コードスキャン開始"""
    if scanner.running:
        await ctx.send("❌ 既に実行中です。")
        return

    target = bot.get_channel(TARGET_CHANNEL_ID)

    if target is None:
        target = ctx.channel
        await ctx.send(f"⚠️ 設定チャンネル({TARGET_CHANNEL_ID})が見つかりません。現在のチャンネル({ctx.channel.mention})を使用します。")

    if not isinstance(target, discord.TextChannel):
        await ctx.send("❌ 指定されたチャンネルはテキストチャンネルではありません。")
        return

    bot_member = target.guild.me
    if not target.permissions_for(bot_member).send_messages:
        await ctx.send(f"❌ Botは{target.mention}にメッセージを送信する権限がありません。")
        return

    scanner.running = True
    scanner.checked = 0
    scanner.forever = False
    # start sender task
    scanner._sender_task = asyncio.create_task(scanner._sender(target))

    await ctx.send(f"🔍 スキャン開始（{duration}秒間）\n対象チャンネル: {target.mention}")

    workers = [asyncio.create_task(scanner.worker(target)) for _ in range(MAX_WORKERS)]
    await asyncio.sleep(duration)
    scanner.running = False
    await asyncio.gather(*workers, return_exceptions=True)
    # wait for queued results to be sent
    try:
        await scanner.result_queue.join()
        if scanner._sender_task:
            await scanner._sender_task
    except Exception:
        pass

    await ctx.send(f"# ✅ スキャン完了\nチェック数: {scanner.checked}\n発見数: {len(scanner.found)}")

@bot.command()
@commands.has_role("TISN管理者")
async def scan_forever(ctx):
    """停止コマンドで止めるまで永続的にスキャンを行います"""
    if scanner.running:
        await ctx.send("❌ 既に実行中です。")
        return

    target = bot.get_channel(TARGET_CHANNEL_ID)
    
    if target is None:
        target = ctx.channel
        await ctx.send(f"⚠️ 設定チャンネル({TARGET_CHANNEL_ID})が見つかりません。現在のチャンネル({ctx.channel.mention})を使用します。")
    
    if not isinstance(target, discord.TextChannel):
        await ctx.send("❌ 指定されたチャンネルはテキストチャンネルではありません。")
        return
    
    bot_member = target.guild.me
    if not target.permissions_for(bot_member).send_messages:
        await ctx.send(f"❌ Botは{target.mention}にメッセージを送信する権限がありません。")
        return

    scanner.running = True
{