import discord
from discord.ext import commands

# ========== ✅ 設定 ==========
TOKEN = "ここにBotのトークンを入力"

# 監視するサーバーIDとチャンネルID
MONITOR_GUILD_ID = 1216303889599565875        # 監視対象サーバーのID
MONITOR_CHANNEL_ID = 1509359281949118605      # 監視対象チャンネルのID

# 報告先（転送先）チャンネルID
REPORT_CHANNEL_ID = 1538692769168625674
# ============================

intents = discord.Intents.default()
intents.message_content = True  # メッセージ内容を読み取るため必須
intents.messages = True

bot = commands.Bot(command_prefix="!", intents=intents)


@bot.event
async def on_message(message: discord.Message):
    # Bot自身のメッセージは無視
    if message.author == bot.user:
        return

    # 指定されたサーバー＆チャンネルだけを監視
    if (
        message.guild
        and message.guild.id == MONITOR_GUILD_ID
        and message.channel.id == MONITOR_CHANNEL_ID
    ):
        report_ch = bot.get_channel(REPORT_CHANNEL_ID)
        if report_ch:
            # メッセージ内容をそのまま転送
            if message.content:
                await report_ch.send(message.content)
            # 画像やファイルが添付されていた場合も一緒に転送
            if message.attachments:
                for att in message.attachments:
                    await report_ch.send(att.url)


@bot.event
async def on_ready():
    print(f"✅ ログイン完了: {bot.user}")
    print(f"📋 監視サーバー: {MONITOR_GUILD_ID}")
    print(f"👁️ 監視チャンネル: {MONITOR_CHANNEL_ID}")
    print(f"📤 転送先チャンネル: {REPORT_CHANNEL_ID}")


if __name__ == "__main__":
    if not TOKEN or TOKEN == "ここにBotのトークンを入力":
        print("❌ TOKENを設定してください")
    else:
        bot.run(TOKEN)