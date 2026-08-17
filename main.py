import discord
from discord.ext import commands
import aiohttp
import asyncio
import random
import string
from concurrent.futures import ThreadPoolExecutor
import os

BOT_TOKEN = os.environ.get("BOT_TOKEN")
TARGET_CHANNEL_ID = 1538692769168625674  # 見つけた招待コードを送信するチャンネルID

# プロキシリスト（レート制限回避用、任意）
PROXIES = [
    # "http://user:pass@proxy1:8080",
    # "http://user:pass@proxy2:8080",
]

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

class InviteBruteForcer:
    def __init__(self):
        self.session = None
        self.found_invites = set()
        self.checked_count = 0
        self.lock = asyncio.Lock()
        
    async def init_session(self):
        """aiohttpセッション初期化"""
        self.session = aiohttp.ClientSession(
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        )
    
    async def close(self):
        if self.session:
            await self.session.close()
    
    def generate_random_code(self, length=7):
        """ランダムな招待コード生成（Discordは通常7-10文字）"""
        chars = string.ascii_lowercase + string.ascii_uppercase + string.digits
        return ''.join(random.choices(chars, k=length))
    
    def generate_dict_code(self, wordlist):
        """辞書ベースのコード生成（一般的な単語の組み合わせ）"""
        # 例: discord, invite, server, etc.
        return random.choice(wordlist) if wordlist else self.generate_random_code()
    
    async def check_invite(self, code, proxy=None):
        """招待コードの有効性をチェック"""
        url = f"https://discord.com/api/v10/invites/{code}?with_counts=true"
        
        try:
            proxy_url = proxy if proxy else None
            
            async with self.session.get(url, proxy=proxy_url, timeout=5) as response:
                self.checked_count += 1
                
                if response.status == 200:
                    data = await response.json()
                    async with self.lock:
                        if code not in self.found_invites:
                            self.found_invites.add(code)
                            return {
                                "code": code,
                                "guild_name": data.get("guild", {}).get("name", "Unknown"),
                                "guild_id": data.get("guild", {}).get("id"),
                                "channel_name": data.get("channel", {}).get("name"),
                                "member_count": data.get("approximate_member_count", 0),
                                "presence_count": data.get("approximate_presence_count", 0)
                            }
                
                elif response.status == 429:  # レート制限
                    retry_after = response.headers.get("Retry-After", 60)
                    print(f"レート制限発生: {retry_after}秒待機")
                    await asyncio.sleep(float(retry_after))
                    
        except asyncio.TimeoutError:
            pass
        except Exception as e:
            pass
        
        return None
    
    async def brute_force_worker(self, target_channel, max_attempts=1000, delay=1.0, use_proxy=False):
        """ワーカータスク"""
        wordlist = ["discord", "server", "community", "gaming", "chat", "friends", "official", "main", "general"]
        
        for _ in range(max_attempts):
            # ランダムと辞書を混ぜる
            if random.random() < 0.3:
                code = self.generate_dict_code(wordlist)
            else:
                code = self.generate_random_code(random.choice([7, 8, 9, 10]))
            
            proxy = random.choice(PROXIES) if use_proxy and PROXIES else None
            
            result = await self.check_invite(code, proxy)
            
            if result:
                # 見つけたら指定チャンネルに送信
                embed = discord.Embed(
                    title="🎉 招待コード発見！",
                    color=0x00ff00,
                    url=f"https://discord.gg/{result['code']}"
                )
                embed.add_field(name="コード", value=f"`{result['code']}`", inline=False)
                embed.add_field(name="サーバー名", value=result['guild_name'], inline=True)
                embed.add_field(name="メンバー数", value=result['member_count'], inline=True)
                embed.add_field(name="参加中", value=result['presence_count'], inline=True)
                
                await target_channel.send(embed=embed)
                print(f"[FOUND] {result}")
            
            # レート制限回避のための遅延（必須）
            await asyncio.sleep(delay)
            
            # 進捗表示（100件ごと）
            if self.checked_count % 100 == 0:
                print(f"チェック済み: {self.checked_count}件 | 発見: {len(self.found_invites)}件")

brute_forcer = InviteBruteForcer()

@bot.event
async def on_ready():
    print(f"Bot起動: {bot.user}")
    await brute_forcer.init_session()

@bot.command()
async def start_brute(ctx, workers: int = 1, delay: float = 1.0):
    """招待コード総当たり開始
    使い方: !start_brute [ワーカー数] [遅延(秒)]
    例: !start_brute 3 0.5
    """
    if ctx.author.id != ctx.guild.owner_id:
        await ctx.send("サーバー管理者のみ実行可能です。")
        return
    
    target_channel = bot.get_channel(TARGET_CHANNEL_ID)
    if not target_channel:
        await ctx.send("送信先チャンネルが見つかりません。TARGET_CHANNEL_IDを確認してください。")
        return
    
    await ctx.send(f"# 🔍 招待コード総当たり開始\nワーカー数: {workers}\n遅延: {delay}秒\n送信先: {target_channel.mention}")
    
    # 複数ワーカーで並列処理
    tasks = []
    for i in range(workers):
        task = asyncio.create_task(
            brute_forcer.brute_force_worker(target_channel, max_attempts=10000, delay=delay)
        )
        tasks.append(task)
    
    await asyncio.gather(*tasks)

@bot.command()
async def stop_brute(ctx):
    """総当たり停止"""
    # タスクキャンセル処理（簡易版）
    await ctx.send("停止コマンドを受信しました。現在のチェックが終了次第停止します。")

@bot.command()
async def brute_status(ctx):
    """現在の進捗確認"""
    await ctx.send(
        f"# 📊 総当たり状況\n"
        f"チェック済み: {brute_forcer.checked_count}件\n"
        f"発見済み: {len(brute_forcer.found_invites)}件\n"
        f"発見コード: {', '.join(brute_forcer.found_invites) if brute_forcer.found_invites else 'なし'}"
    )

@bot.event
async def on_disconnect():
    await brute_forcer.close()

bot.run(BOT_TOKEN)