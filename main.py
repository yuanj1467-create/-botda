# 技術的学習用 - 実際の使用は推奨されません
import discord
from discord.ext import commands
import aiohttp
import asyncio
import random
import string
import os
from datetime import datetime
from dotenv import load_dotenv

# 環境変数読み込み
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
TARGET_CHANNEL_ID = 1538692769168625674  # あなたのチャンネルID

# 設定
MAX_WORKERS = 1          # ワーカー数（増やすと検出リスク上昇）
CHECK_DELAY = 2.0        # リクエスト間隔（秒）
MAX_ATTEMPTS = 1000      # 最大試行回数

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
            headers={
                "User-Agent": "DiscordBot (https://github.com/example, 1.0)"
            }
        )
    
    async def close(self):
        self.running = False
        if self.session:
            await self.session.close()
    
    def generate_code(self):
        """招待コード生成（7-10文字）"""
        chars = string.ascii_lowercase + string.digits
        length = random.choice([7, 8, 9, 10])
        return ''.join(random.choices(chars, k=length))
    
    async def check(self, code: str):
        """招待コードをチェック"""
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
                    
        except Exception as e:
            pass
        return None
    
    async def worker(self, channel: discord.TextChannel):
        """スキャンワーカー"""
        while self.running and self.checked < MAX_ATTEMPTS:
            code = self.generate_code()
            result = await self.check(code)
            
            if result:
                self.found.append(result)
                
                # 結果をDiscordに送信
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
            
            # レート制限回避
            await asyncio.sleep(CHECK_DELAY)
            
            # 進捗表示
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
    """招待コードスキャン開始（秒数指定）"""
    if scanner.running:
        await ctx.send("❌ 既に実行中です。")
        return
    
    # チャンネル取得（エラーハンドリング付き）
    target = bot.get_channel(TARGET_CHANNEL_ID)
    
    if target is None:
        # 環境変数のチャンネルが見つからない場合は現在のチャンネルを使用
        target = ctx.channel
        await ctx.send(f"⚠️ 設定チャンネル({TARGET_CHANNEL_ID})が見つかりません。現在のチャンネル({ctx.channel.mention})を使用します。")
    
    # 権限チェック
    if not isinstance(target, discord.TextChannel):
        await ctx.send("❌ 指定されたチャンネルはテキストチャンネルではありません。")
        return
    
    # Botの権限確認
    bot_member = target.guild.me
    if not target.permissions_for(bot_member).send_messages:
        await ctx.send(f"❌ Botは{target.mention}にメッセージを送信する権限がありません。\n認証済みロールが付与されているか確認してください。")
        return
    
    if not target.permissions_for(bot_member).embed_links:
        await ctx.send(f"⚠️ embed_links権限がありません。テキストのみで送信します。")
    
    scanner.running = True
    scanner.checked = 0
    
    await ctx.send(f"🔍 スキャン開始（{duration}秒間）\n対象チャンネル: {target.mention}")
    
    # 複数ワーカー起動
    workers = [asyncio.create_task(scanner.worker(target)) for _ in range(MAX_WORKERS)]
    
    # 指定時間待機
    await asyncio.sleep(duration)
    scanner.running = False
    
    # ワーカー終了待ち
    await asyncio.gather(*workers, return_exceptions=True)
    
    # 結果報告
    await ctx.send(
        f"# ✅ スキャン完了\n"
        f"チェック数: {scanner.checked}\n"
        f"発見数: {len(scanner.found)}"
    )

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
        f"発見: {len(scanner.found)}\n"
        f"対象チャンネル: <#{TARGET_CHANNEL_ID}>"
    )

@bot.event
async def on_disconnect():
    await scanner.close()

# エラーハンドリング
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.NotOwner):
        await ctx.send("❌ Bot所有者のみ実行可能です。")
    elif isinstance(error, commands.MissingPermissions):
        await ctx.send("❌ 権限がありません。")
    else:
        print(f"コマンドエラー: {error}")
        await ctx.send(f"❌ エラーが発生しました: {error}")

if __name__ == "__main__":
    if not BOT_TOKEN:
        print("ERROR: BOT_TOKENが設定されていません")
        exit(1)
    start_keep_alive()  # この行があるか確認
    bot.run(BOT_TOKEN)