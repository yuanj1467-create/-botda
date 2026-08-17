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

# ✅ 改善: インスタンスを複数追加・冗長化
NITTER_DEFAULT_INSTANCES = [
    "https://nitter.net",
    "https://nitter.kavin.rocks",
    "https://nitter.poast.org",
    "https://nitter.pussthecat.org",
    "https://nitter.moomoo.me",
    "https://nitter.420labs.com",
]

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
        self.lock = asyncio.Lock()
        self.forever = False
        self.result_queue = asyncio.Queue()
        self._sender_task = None
        # Nitter関連
        self.nitter_running = False
        self.nitter_task = None
        self.nitter_seen = {}
        # ✅ 追加: インスタンスの状態管理
        self.bad_instances = set()
        self.session_refresh_interval = 300  # 5分ごとにセッション再作成
    
    async def init(self):
        """aiohttpセッションを初期化"""
        await self._recreate_session()

    async def _recreate_session(self):
        """✅ 追加: セッションを再作成（接続エラー時の回復用）"""
        if self.session and not self.session.closed:
            await self.session.close()
        connector = aiohttp.TCPConnector(limit=10, force_close=True)
        self.session = aiohttp.ClientSession(
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            connector=connector
        )
        print("✅ HTTPセッションを再作成しました")
    
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
                    print(f"⚠️ レート制限: {retry}秒待機")
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
                
                print(f"[FOUND-ENQUEUED] {result['code']} -> {result['guild']}")
            
            await asyncio.sleep(CHECK_DELAY)
            
            if self.checked % 50 == 0:
                print(f"進捗: {self.checked}件チェック済み / 発見: {len(self.found)}件")

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
                            print(f"⚠️ Worker 終了: {exc}")
                    except asyncio.CancelledError:
                        print("Worker キャンセル")
                    if getattr(self, 'running', False):
                        await asyncio.sleep(random.uniform(0.5, 1.5))
                        tasks.add(asyncio.create_task(self.worker(channel)))
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            print(f"_maintain_workers エラー: {e}")
            for t in tasks:
                try:
                    t.cancel()
                except Exception:
                    pass
            await asyncio.gather(*tasks, return_exceptions=True)

    # ------------------------- Nitterポーラー（修正版） -------------------------
    async def _fetch_nitter_html(self, base_url: str, path: str = "/search?f=tweets&q=discord.gg", retries: int = 3):
        """✅ 改善: リトライ処理追加 + セッションエラー時の再接続"""
        # ✅ 状態の悪いインスタンスはスキップ
        if base_url in self.bad_instances:
            return ""

        url = base_url.rstrip("/") + path

        for attempt in range(retries):
            try:
                if not self.session or self.session.closed:
                    await self._recreate_session()

                async with self.session.get(url, timeout=15) as resp:
                    if resp.status == 200:
                        # 正常応答 → 不良リストから削除
                        if base_url in self.bad_instances:
                            self.bad_instances.discard(base_url)
                        return await resp.text()
                    elif resp.status in [429, 500, 502, 503, 504]:
                        wait = min(2 ** attempt, 10)
                        print(f"⚠️ Nitter {base_url} 状態コード: {resp.status} → {wait}秒待機（試行{attempt+1}/{retries}）")
                        await asyncio.sleep(wait)
                    else:
                        print(f"⚠️ Nitter {base_url} 状態コード: {resp.status}")
                        await asyncio.sleep(1)
            except aiohttp.ClientConnectionError as e:
                print(f"❌ Nitter接続エラー {base_url}: {e} → セッション再作成")
                await self._recreate_session()
                await asyncio.sleep(2)
            except Exception as e:
                print(f"❌ Nitter取得エラー {base_url}: {e}")
                await asyncio.sleep(1)

        # ✅ 3回失敗 → 不良インスタンスとしてマーク
        print(f"🚫 インスタンス {base_url} を一時的に無効化")
        self.bad_instances.add(base_url)
        return ""

    async def nitter_poller(self, instances: list, interval: float = 60.0, seen_ttl: int = 60*60*12, verify: bool = True):
        """
        Nitterからdiscord.ggリンクを定期的に収集
        interval: ポール間隔（秒）→ 長めに推奨（30→60秒以上）
        """
        self.nitter_running = True
        self.nitter_seen = getattr(self, "nitter_seen", {})
        last_refresh = time.time()

        while self.nitter_running:
            now = time.time()

            # ✅ 定期的にセッション再作成
            if now - last_refresh > self.session_refresh_interval:
                await self._recreate_session()
                self.bad_instances.clear()  # 5分ごとに一時無効リストをリセット
                last_refresh = now

            # 古いエントリを削除
            for k, t in list(self.nitter_seen.items()):
                if now - t > seen_ttl:
                    del self.nitter_seen[k]

            # ✅ 生きているインスタンスだけを使用
            active_instances = [i for i in instances if i not in self.bad_instances]
            if not active_instances:
                print("⚠️ 有効なNitterインスタンスがありません。待機中…")
                await asyncio.sleep(interval)
                continue

            random.shuffle(active_instances)
            print(f"🔍 Nitterスキャン実行: {len(active_instances)}インスタンス使用")

            for inst in active_instances:
                if not self.nitter_running:
                    break

                html = await self._fetch_nitter_html(inst)
                if not html:
                    await asyncio.sleep(2)
                    continue

                codes = set(NITTER_INVITE_RE.findall(html))
                print(f"  {inst}: {len(codes)}件のコードを発見")

                for code in codes:
                    if not self.nitter_running:
                        break
                    if code in self.nitter_seen:
                        continue

                    self.nitter_seen[code] = now
                    if verify:
                        try:
                            res = await self.check(code)
                        except Exception as e:
                            print(f"コード確認エラー {code}: {e}")
                            res = None
                        if res:
                            await self.result_queue.put(res)
                            self.found.append(res)
                            print(f"[NITTER→ENQUEUE] {code} → {res.get('guild')}")
                    else:
                        res = {"code": code, "guild": "Unknown (from Nitter)", "members": 0, "online": 0}
                        await self.result_queue.put(res)
                        self.found.append(res)
                        print(f"[NITTER→ENQUEUE(未確認)] {code}")

                await asyncio.sleep(random.uniform(2, 4))  # ✅ インスタンス間隔を長めに

            # 次回ポールまで待機
            slept = 0.0
            chunk = 2.0
            while slept < interval and self.nitter_running:
                await asyncio.sleep(min(chunk, interval - slept))
                slept += chunk

        print("⏹️ Nitterポーラー停止")

scanner = InviteScanner()

@bot.event
async def on_ready():
    print(f"✅ Bot起動: {bot.user}")
    print(f"対象チャンネルID: {TARGET_CHANNEL_ID}")
    await scanner.init()

# ------------------------- コマンド -------------------------
@bot.command(name="nitter_scan_start", aliases=["twitter_scan_start"])
@commands.has_role("TISN管理者")
async def nitter_scan_start(ctx, interval: int = None):
    """Nitterポーリング開始（推奨間隔: 60秒以上）"""
    if scanner.nitter_task and not scanner.nitter_task.done():
        await ctx.send("❌ 既に実行中です。")
        return

    instances_env = os.getenv("NITTER_INSTANCES", "")
    if instances_env:
        instances = [s.strip() for s in instances_env.split(",") if s.strip()]
    else:
        instances = NITTER_DEFAULT_INSTANCES

    poll_interval = interval if interval is not None else int(os.getenv("NITTER_POLL_INTERVAL", "60"))
    scanner.nitter_task = asyncio.create_task(scanner.nitter_poller(instances, interval=poll_interval))
    await ctx.send(f"🔎 Nitterスキャン開始（間隔={poll_interval}秒 / {len(instances)}インスタンス）")

@bot.command(name="nitter_scan_stop", aliases=["twitter_scan_stop"])
@commands.has_role("TISN管理者")
async def nitter_scan_stop(ctx):
    """Nitterポーリング停止"""
    if not scanner.nitter_task or scanner.nitter_task.done():
        await ctx.send("実行していません。")
        return
    scanner.nitter_running = False
    try:
        await scanner.nitter_task
    except Exception:
        pass
    scanner.nitter_task = None
    await ctx.send("⏹️ Nitterスキャンを停止しました。")

@bot.command()
@commands.has_role("TISN管理者")
async def scan(ctx, duration: int = 60):
    """招待コード総当たりスキャン"""
    if scanner.running:
        await ctx.send("❌ 既に実行中です。")
        return

    target = bot.get_channel(TARGET_CHANNEL_ID)
    if target is None:
        target = ctx.channel
        await ctx.send(f"⚠️ チャンネルが見つからないため {ctx.channel.mention} を使用")

    if not isinstance(target, discord.TextChannel):
        await ctx.send("❌ テキストチャンネルではありません。")
        return

    bot_member = target.guild.me
    if not target.permissions_for(bot_member).send_messages:
        await ctx.send(f"❌ {target.mention} への送信権限がありません。")
        return

    scanner.running = True
    scanner.checked = 0
    scanner.forever = False
    scanner._sender_task = asyncio.create_task(scanner._sender(target))

    await ctx.send(f"🔍 スキャン開始（{duration}秒）\n対象: {target.mention}")

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

    await ctx.send(f"✅ スキャン完了\nチェック: {scanner.checked}件 / 発見: {len(scanner.found)}件")

@bot.command()
@commands.has_role("TISN管理者")
async def scan_forever(ctx):
    """永続スキャン"""
    if scanner.running:
        await ctx.send("❌ 既に実行中です。")
        return

    target = bot.get_channel(TARGET_CHANNEL_ID)
    if target is None:
        target = ctx.channel
        await ctx.send(f"⚠️ チャンネルが見つからないため {ctx.channel.mention} を使用")

    if not isinstance(target, discord.TextChannel):
        await ctx.send("❌ テキストチャンネルではありません。")
        return

    bot_member = target.guild.me
    if not target.permissions_for(bot_member).send_messages:
        await ctx.send(f"❌ {target.mention} への送信権限がありません。")
        return

    scanner.running = True
    scanner.checked = 0
    scanner.forever = True
    scanner._sender_task = asyncio.create_task(scanner._sender(target))

    await ctx.send(f"🔁 永続スキャン開始\n対象: {target.mention}")
    await scanner._maintain_workers(target, MAX_WORKERS)

    try:
        await scanner.result_queue.join()
        if scanner._sender_task:
            await scanner._sender_task
    except Exception:
        pass

    await ctx.send(f"✅ スキャン停止\nチェック: {scanner.checked}件 / 発見: {len(scanner.found)}件")

@bot.command()
@commands.has_role("TISN管理者")
async def stop(ctx):
    """スキャン停止"""
    if not scanner.running:
        await ctx.send("実行していません。")
        return
    scanner.running = False
    scanner.forever = False
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
    """状態確認"""
    await ctx.send(
        f"実行中: {'はい' if scanner.running else 'いいえ'}\n"
        f"永続モード: {'オン' if scanner.forever else 'オフ'}\n"
        f"チェック済み: {scanner.checked}\n"
        f"発見: {len(scanner.found)}件\n"
        f"Nitter不良インスタンス: {len(scanner.bad_instances)}件"
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
        print("❌ エラー: BOT_TOKEN が設定されていません")
        exit(1)
    
    start_keep_alive()
    try:
        bot.run(BOT_TOKEN)
    except Exception as e:
        print(f"bot.run() 例外: {e}")