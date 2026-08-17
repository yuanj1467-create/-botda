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
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), _KeepAliveHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"キープアライブサーバー起動: ポート {port}")

# ==============================
# Bot設定
# ==============================
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
TARGET_CHANNEL_ID = 1538692769168625674

MAX_WORKERS = 1
CHECK_DELAY = 2.0
MAX_ATTEMPTS = 1000

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

class InviteScanner:
    def __init__(self):
        self.session = None
        self.found = []
        self.checked = 0
        self.running = False
        self.lock = asyncio.Lock()
        
    async def init(self):
        self.session = aiohttp.ClientSession(
            headers={"User-Agent": "DiscordBot (1.0)"}
        )
    
    async def close(self):
        self.running = False
        if self.session:
            await self.session.close()
    
    def generate_code(self):
        chars = string.ascii_lowercase + string.digits
        length = random.choice([7, 8, 9, 10])
        return ''.join(random.choices(chars, k=length))
    
    async def check(self, code: str):
        url = f"https://discord.com/api/v10/invites/{code}?with_counts=true"
        try:
            async with self.session.get(url, timeout=5) as resp:
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
                    await asyncio.sleep(retry)
        except Exception:
            pass
        return None
    
    async def worker(self, channel: discord.TextChannel):
        while self.running and self.checked < MAX_ATTEMPTS:
            code = self.generate_code()
            result = await self.check(code)
            
            if result:
                self.found.append(result)
                embed = discord.Embed(
                    title="🔍 招待コード発見",
                    url=f"https://discord.gg/{result['code']}",
                    color=0x00ff00,
                    timestamp=datetime.now()
                )
                embed.add_field(name="コード", value=f"`{result['code']}`", inline=False)
                embed.add_field(name="サーバー", value=result['guild'], inline=True)
                embed.add_field(name="メンバー", value=result['members'], inline=True)
                
                try:
                    await channel.send(embed=embed)
                except discord.Forbidden:
                    print(f"送信権限なし: {channel.id}")
                    self.running = False
                    break
                except Exception as e:
                    print(f"送信エラー: {e}")
                
                print(f"[FOUND] {result['code']} -> {result['guild']}")
            
            await asyncio.sleep(CHECK_DELAY)
            
            if self.checked % 50 == 0:
                print(f"進捗: {self.checked}件チェック済み / 発見: {len(self.found)}件")

scanner = InviteScanner()

@bot.event
async def on_ready():
    print(f"Bot起動: {bot.user}")
    print(f"TARGET_CHANNEL_ID: {TARGET_CHANNEL_ID}")
    await scanner.init()

@bot.command()
@commands.is_owner()
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
    
    await ctx.send(f"🔍 スキャン開始（{duration}秒間）\n対象チャンネル: {target.mention}")
    
    workers = [asyncio.create_task(scanner.worker(target)) for _ in range(MAX_WORKERS)]
    await asyncio.sleep(duration)
    scanner.running = False
    await asyncio.gather(*workers, return_exceptions=True)
    
    await ctx.send(f"# ✅ スキャン完了\nチェック数: {scanner.checked}\n発見数: {len(scanner.found)}")

@bot.command()
@commands.is_owner()
async def stop(ctx):
    """スキャン停止"""
    if not scanner.running:
        await ctx.send("実行していません。")
        return
    scanner.running = False
    await ctx.send("⏹️ 停止しました。")

@bot.command()
@commands.is_owner()
async def status(ctx):
    """現在の状態"""
    await ctx.send(
        f"実行中: {'はい' if scanner.running else 'いいえ'}\n"
        f"チェック済み: {scanner.checked}\n"
        f"発見: {len(scanner.found)}"
    )

@bot.event
async def on_disconnect():
    await scanner.close()

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.NotOwner):
        await ctx.send("❌ Bot所有者のみ実行可能です。")
    else:
        print(f"コマンドエラー: {error}")

# ==============================
# 起動
# ==============================
if __name__ == "__main__":
    if not BOT_TOKEN:
        print("ERROR: BOT_TOKENが設定されていません")
        exit(1)
    
    start_keep_alive()  # キープアライブサーバー起動
    bot.run(BOT_TOKEN)