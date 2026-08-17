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
# キープアライブサーバー
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
        print(f"キープアライブサーバー起動: ポート {port}")
    except Exception as e:
        print(f"Keep-alive エラー: {e}")

# ==============================
# Bot設定
# ==============================
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
TARGET_CHANNEL_ID = int(os.getenv("TARGET_CHANNEL_ID", "1538692769168625674"))

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "30"))
CHECK_DELAY = float(os.getenv("CHECK_DELAY", "0.8"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "1000"))

# ✅ 最も安定が確認できたインスタンスのみ
# ✅ 2026年8月現在 稼働中のインスタンスのみを使用
NITTER_DEFAULT_INSTANCES = [
    "https://xcancel.com",
    "https://nitter.poast.org",
    "https://nitter.privacyredirect.com",
    "https://nitter.space",
    "https://nitter.tiekoetter.com",
    "https://nt.vern.cc",
    "https://lightbrd.com",
]

# ✅ 並行検索の設定
MAX_PARALLEL_INSTANCES = 4       # ✅ 同時に検索するインスタンス数（安全値）
MAX_PAGES_TO_SCAN = 5            # ✅ 遡る過去ページ数（1=最新のみ、5=過去5ページ分）
PAGE_SCAN_DELAY = 2.5            # ✅ ページ間の待機時間（秒）

NITTER_INVITE_RE = re.compile(r"(?:https?://)?discord\.gg/([A-Za-z0-9_\-]+)")

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

print("Starting main.py (高速並行版)")
print(f"並行数:{MAX_PARALLEL_INSTANCES} 遡り:{MAX_PAGES_TO_SCAN}ページ")

class InviteScanner:
    def __init__(self):
        self.session = None
        self.found = []
        self.checked = 0
        self.running = False
        self.lock = asyncio.Lock()
        self.forever = False
        self.result_queue = asyncio.Queue()
        self._sender_task = None
        self.nitter_running = False
        self.nitter_task = None
        self.nitter_seen = {}
        self.bad_instances = set()
        self.session_refresh_interval = 300
        # ✅ 同時実行制御用セマフォ
        self.semaphore = asyncio.Semaphore(MAX_PARALLEL_INSTANCES)

    async def init(self):
        await self._recreate_session()

    async def _recreate_session(self):
        if self.session and not self.session.closed:
            await self.session.close()
        connector = aiohttp.TCPConnector(limit=MAX_PARALLEL_INSTANCES, force_close=True)
        self.session = aiohttp.ClientSession(
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            connector=connector
        )
        print("✅ HTTPセッション再作成")

    async def close(self):
        self.running = False
        self.nitter_running = False
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
        if self.session and not self.session.closed:
            await self.session.close()

    def generate_code(self):
        chars = string.ascii_lowercase + string.digits
        length = random.choice([7, 8, 9, 10])
        return ''.join(random.choices(chars, k=length))

    async def _sleep_interruptible(self, seconds: float):
        remaining = seconds
        chunk = 1.0
        while remaining > 0 and (getattr(self, 'running', False) or getattr(self, 'nitter_running', False)):
            await asyncio.sleep(min(chunk, remaining))
            remaining -= chunk

    async def check(self, code: str):
        if not self.session or self.session.closed:
            await self._recreate_session()
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
                    print(f"⚠️ Discordレート制限: {retry}秒待機")
                    await self._sleep_interruptible(retry)
        except Exception as e:
            print(f"check() エラー {code}: {e}")
        return None

    async def _sender(self, channel: discord.TextChannel):
        while (getattr(self, 'running', False) or not self.result_queue.empty() or getattr(self, 'nitter_running', False)):
            result = None
            try:
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
                    print(f"❌ 送信権限なし: {channel.id}")
                    self.running = False
                    break
                except Exception as e:
                    print(f"送信エラー: {e}")
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
                try:
                    await self.result_queue.put(result)
                except Exception as e:
                    print(f"キュー格納エラー: {e}")
                print(f"[FOUND] {result['code']} -> {result['guild']}")
            await asyncio.sleep(CHECK_DELAY)
            if self.checked % 50 == 0:
                print(f"進捗: {self.checked}件チェック / 発見:{len(self.found)}件")

    async def _maintain_workers(self, channel: discord.TextChannel, desired_count: int):
        tasks = {asyncio.create_task(self.worker(channel)) for _ in range(desired_count)}
        try:
            while getattr(self, 'running', False):
                if not tasks:
                    tasks = {asyncio.create_task(self.worker(channel)) for _ in range(desired_count)}
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for t in done:
                    tasks.discard(t)
                    try:
                        exc = t.exception()
                        if exc:
                            print(f"⚠️ Worker終了: {exc}")
                    except asyncio.CancelledError:
                        pass
                    if getattr(self, 'running', False):
                        await asyncio.sleep(random.uniform(0.5, 1.5))
                        tasks.add(asyncio.create_task(self.worker(channel)))
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            print(f"_maintain_workers エラー: {e}")

    # ------------------------- ✅ 改良版：並行＋過去遡り -------------------------
    async def _scan_single_instance(self, base_url: str, interval: float, verify: bool):
        """1つのインスタンスを複数ページ遡ってスキャン"""
        if base_url in self.bad_instances:
            return []

        found_codes = []
        now = time.time()

        # ✅ ページを遡って取得: p=1 → p=MAX_PAGES_TO_SCAN
        for page_num in range(1, MAX_PAGES_TO_SCAN + 1):
            if not self.nitter_running:
                break

            path = f"/search?f=tweets&q=discord.gg&p={page_num}" if page_num > 1 else "/search?f=tweets&q=discord.gg"
            url = base_url.rstrip("/") + path

            html = None
            try:
                if not self.session or self.session.closed:
                    await self._recreate_session()

                async with self.semaphore:  # ✅ 同時実行数を制限
                    async with self.session.get(url, timeout=15) as resp:
                        if resp.status == 200:
                            html = await resp.text()
                            if base_url in self.bad_instances:
                                self.bad_instances.discard(base_url)
                        elif resp.status in [429, 500, 502, 503, 403]:
                            wait = min(2 ** min(page_num, 3), 10)
                            print(f"⚠️ {base_url} ページ{page_num}: 状態{resp.status} → {wait}秒待機")
                            await asyncio.sleep(wait)
                            continue
                        else:
                            print(f"⚠️ {base_url}: 状態{resp.status}")
                            await asyncio.sleep(1)
                            continue

            except Exception as e:
                print(f"❌ {base_url} ページ{page_num}: {e}")
                await asyncio.sleep(2)
                continue

            if not html:
                continue

            codes = set(NITTER_INVITE_RE.findall(html))
            print(f"  {base_url} ページ{page_num}: {len(codes)}件発見")

            for code in codes:
                if not self.nitter_running:
                    break
                if code in self.nitter_seen:
                    continue

                self.nitter_seen[code] = now
                if verify:
                    res = await self.check(code)
                    if res:
                        found_codes.append(res)
                        await self.result_queue.put(res)
                        print(f"[NITTER] {code} → {res.get('guild')}")
                else:
                    res = {"code": code, "guild": "Unknown", "members": 0, "online": 0}
                    found_codes.append(res)
                    await self.result_queue.put(res)

            # ✅ ページ間で待機（負荷軽減）
            if page_num < MAX_PAGES_TO_SCAN:
                await asyncio.sleep(PAGE_SCAN_DELAY)

        return found_codes

    async def nitter_poller(self, instances: list, interval: float = 180.0, seen_ttl: int = 60*60*12, verify: bool = True):
        """✅ 並行版ポーラー：複数インスタンスを同時にスキャン"""
        self.nitter_running = True
        self.nitter_seen = getattr(self, "nitter_seen", {})
        last_refresh = time.time()

        while self.nitter_running:
            now = time.time()

            # 定期的にセッションと不良リストをリセット
            if now - last_refresh > self.session_refresh_interval:
                await self._recreate_session()
                self.bad_instances.clear()
                last_refresh = now

            # 古いエントリ削除
            for k, t in list(self.nitter_seen.items()):
                if now - t > seen_ttl:
                    del self.nitter_seen[k]

            active_instances = [i for i in instances if i not in self.bad_instances]
            if not active_instances:
                print("⚠️ 有効インスタンスなし。待機…")
                await asyncio.sleep(interval)
                continue

            print(f"🔍 並行スキャン開始: {len(active_instances)}インスタンス / {MAX_PAGES_TO_SCAN}ページ遡り")

            # ✅ 複数インスタンスを並行実行
            tasks = [self._scan_single_instance(inst, interval, verify) for inst in active_instances]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            total_found = sum(len(r) for r in results if isinstance(r, list))
            print(f"✅ スキャン完了: 計{total_found}件 新規発見")

            # 次回まで待機
            slept = 0.0
            chunk = 5.0
            while slept < interval and self.nitter_running:
                await asyncio.sleep(min(chunk, interval - slept))
                slept += chunk

        print("⏹️ Nitterポーラー停止")

scanner = InviteScanner()

@bot.event
async def on_ready():
    print(f"✅ Bot起動: {bot.user}")
    print(f"対象チャンネル: {TARGET_CHANNEL_ID}")
    await scanner.init()

# ------------------------- コマンド -------------------------
@bot.command(name="nitter_scan_start", aliases=["twitter_scan_start"])
@commands.has_role("TISN管理者")
async def nitter_scan_start(ctx, interval: int = None):
    """Nitter並行スキャン開始（既定:180秒 推奨:180秒以上）"""
    if scanner.nitter_task and not scanner.nitter_task.done():
        await ctx.send("❌ 既に実行中です。")
        return

    instances_env = os.getenv("NITTER_INSTANCES", "")
    instances = [s.strip() for s in instances_env.split(",") if s.strip()] if instances_env else NITTER_DEFAULT_INSTANCES

    poll_interval = interval if interval is not None else int(os.getenv("NITTER_POLL_INTERVAL", "180"))
    scanner.nitter_task = asyncio.create_task(scanner.nitter_poller(instances, interval=poll_interval))
    await ctx.send(
        f"🚀 並行スキャン開始\n"
        f"・インスタンス: {len(instances)}台 並行{MAX_PARALLEL_INSTANCES}\n"
        f"・遡り: 過去{MAX_PAGES_TO_SCAN}ページ\n"
        f"・間隔: {poll_interval}秒"
    )

@bot.command(name="nitter_scan_stop", aliases=["twitter_scan_stop"])
@commands.has_role("TISN管理者")
async def nitter_scan_stop(ctx):
    if not scanner.nitter_task or scanner.nitter_task.done():
        await ctx.send("実行していません。")
        return
    scanner.nitter_running = False
    try:
        await scanner.nitter_task
    except Exception:
        pass
    scanner.nitter_task = None
    await ctx.send("⏹️ 停止しました。")

@bot.command()
@commands.has_role("TISN管理者")
async def scan(ctx, duration: int = 60):
    if scanner.running:
        await ctx.send("❌ 既に実行中です。")
        return
    target = bot.get_channel(TARGET_CHANNEL_ID) or ctx.channel
    if not isinstance(target, discord.TextChannel):
        await ctx.send("❌ テキストチャンネルのみ対応。")
        return
    if not target.permissions_for(target.guild.me).send_messages:
        await ctx.send(f"❌ {target.mention}に送信権限がありません。")
        return

    scanner.running = True
    scanner.checked = 0
    scanner.forever = False
    scanner._sender_task = asyncio.create_task(scanner._sender(target))
    await ctx.send(f"🔍 スキャン開始（{duration}秒）: {target.mention}")

    workers = [asyncio.create_task(scanner.worker(target)) for _ in range(MAX_WORKERS)]
    await asyncio.sleep(duration)
    scanner.running = False
    await asyncio.gather(*workers, return_exceptions=True)
    try:
        await scanner.result_queue.join()
        if scanner._sender_task:
            await scanner._sender_task
    except Exception:
        pass
    await ctx.send(f"✅ 完了: チェック{scanner.checked}件 / 発見{len(scanner.found)}件")

@bot.command()
@commands.has_role("TISN管理者")
async def scan_forever(ctx):
    if scanner.running:
        await ctx.send("❌ 既に実行中です。")
        return
    target = bot.get_channel(TARGET_CHANNEL_ID) or ctx.channel
    if not isinstance(target, discord.TextChannel):
        await ctx.send("❌ テキストチャンネルのみ対応。")
        return
    if not target.permissions_for(target.guild.me).send_messages:
        await ctx.send(f"❌ {target.mention}に送信権限がありません。")
        return

    scanner.running = True
    scanner.checked = 0
    scanner.forever = True
    scanner._sender_task = asyncio.create_task(scanner._sender(target))
    await ctx.send(f"🔁 永続スキャン開始: {target.mention}")
    await scanner._maintain_workers(target, MAX_WORKERS)
    try:
        await scanner.result_queue.join()
        if scanner._sender_task:
            await scanner._sender_task
    except Exception:
        pass
    await ctx.send(f"✅ 停止: チェック{scanner.checked}件 / 発見{len(scanner.found)}件")

@bot.command()
@commands.has_role("TISN管理者")
async def stop(ctx):
    if not scanner.running:
        await ctx.send("実行していません。")
        return
    scanner.running = scanner.forever = False
    try:
        await scanner.result_queue.join()
        if scanner._sender_task:
            await scanner._sender_task
    except Exception:
        pass
    await ctx.send("⏹️ 停止しました。")

@bot.command()
@commands.has_role("TISN管理者")
async def status(ctx):
    await ctx.send(
        f"実行中: {'はい' if scanner.running else 'いいえ'}\n"
        f"永続: {'オン' if scanner.forever else 'オフ'}\n"
        f"チェック済: {scanner.checked}\n"
        f"発見: {len(scanner.found)}件\n"
        f"不良インスタンス: {len(scanner.bad_instances)}件\n"
        f"並行制限: {MAX_PARALLEL_INSTANCES} / 遡り: {MAX_PAGES_TO_SCAN}ページ"
    )

@bot.event
async def on_disconnect():
    await scanner.close()

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.NotOwner):
        await ctx.send("❌ 管理者権限が必要です。")
    else:
        print(f"コマンドエラー: {error}")

# ==============================
# 起動
# ==============================
if __name__ == "__main__":
    if not BOT_TOKEN:
        print("❌ BOT_TOKEN が設定されていません")
        exit(1)
    start_keep_alive()
    try:
        bot.run(BOT_TOKEN)
    except Exception as e:
        print(f"bot.run() 例外: {e}")