#  @MrMNTG @MusammilN
#please give credits https://github.com/MN-BOTS/ShobanaFilterBot
import logging
import logging.config
import os
import sys
import asyncio
from datetime import date, datetime
import pytz
import aiohttp

# Get logging configurations
logging.config.fileConfig('logging.conf')
logging.getLogger().setLevel(logging.INFO)
logging.getLogger("pyrogram").setLevel(logging.ERROR)
logging.getLogger("imdbpy").setLevel(logging.ERROR)
logging.getLogger("asyncio").setLevel(logging.CRITICAL - 1)

import tgcrypto
from pyrogram import Client, __version__
from pyrogram.types import BotCommand, CallbackQuery
from pyrogram.raw.all import layer
from pyrogram.errors import QueryIdInvalid
from database.ia_filterdb import Media, preload_media_cache, set_index_mode
from database.users_chats_db import db
from info import SESSION, API_ID, API_HASH, BOT_TOKEN, LOG_STR, LOG_CHANNEL, KEEP_ALIVE_URL, DEFAULT_AUTH_CHANNELS
from utils import temp
from typing import Union, Optional, AsyncGenerator
from pyrogram import types

_ORIGINAL_CALLBACK_ANSWER = CallbackQuery.answer


async def _safe_callback_answer(self, *args, **kwargs):
    try:
        return await _ORIGINAL_CALLBACK_ANSWER(self, *args, **kwargs)
    except QueryIdInvalid:
        logging.debug("Ignoring expired callback query answer")
        return None


CallbackQuery.answer = _safe_callback_answer
from Script import script
from os import environ
from aiohttp import web as webserver

# Peer ID invalid fix
from pyrogram import utils as pyroutils
pyroutils.MIN_CHAT_ID = -999999999999
pyroutils.MIN_CHANNEL_ID = -100999999999999

from plugins.webcode import bot_run
from plugins.new_updates import load_runtime_update_config, run_daily_summary

PORT_CODE = environ.get("PORT", "8080")


BOT_COMMANDS = [
    BotCommand("start", "Start the bot"),
    BotCommand("movies", "Latest added movies"),
    BotCommand("series", "Latest added series"),
    BotCommand("connect", "Connect group to PM"),
    BotCommand("disconnect", "Disconnect active chat"),
    BotCommand("connections", "Show your connections"),
    BotCommand("settings", "Open group settings"),
    BotCommand("filter", "Create manual filter"),
    BotCommand("filters", "List filters"),
    BotCommand("imdb", "Search movie/series info"),
    BotCommand("mnsearch", "Search movie/series info"),
    BotCommand("bug", "Send bug report / feedback"),
    BotCommand("search", "Search from external sources"),
    BotCommand("deletefiles", "Bulk delete indexed files"),
    BotCommand("deleteduplicates", "Delete duplicate indexed files"),
    BotCommand("stats", "Show database statistics"),
    BotCommand("ping", "Check bot ping")
]



# ✅ Add this block
async def preload_auth_channels():
    if not await db.get_auth_channels():
        await db.set_auth_channels(DEFAULT_AUTH_CHANNELS)
        logging.info("Set default AUTH_CHANNELs in DB.")

async def startup_warmups():
    """Run non-critical DB/cache warmups after the health server is online."""
    try:
        results = await asyncio.gather(Media.ensure_indexes(), db.ensure_indexes(), return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logging.warning("Startup index warmup failed: %s", result)

        await load_runtime_update_config()
        saved_index_mode = await db.get_config_value('index_mode', None)
        if saved_index_mode:
            set_index_mode(saved_index_mode)

        await preload_auth_channels()
        await preload_media_cache()
    except Exception as e:
        logging.exception("Startup warmup failed: %s", e)

async def keep_alive():
    """Send a request every 111 seconds to keep the bot alive (if required)."""
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await session.get(KEEP_ALIVE_URL)
                logging.info("Sent keep-alive request.")
            except Exception as e:
                logging.error(f"Keep-alive request failed: {e}")
            await asyncio.sleep(111)


class Bot(Client):

    def __init__(self):
        super().__init__(
            name=SESSION,
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            workers=int(environ.get("BOT_WORKERS", "10")),
            plugins={"root": "plugins"},
            sleep_threshold=5,
        )

    async def kulasthree(self):
        while True:
            await asyncio.sleep(24 * 60 * 60)
            logging.info("🔄 Bot is restarting")
            await self.send_message(chat_id=LOG_CHANNEL, text="🔄 Bot is restarting ...")
            os.execl(sys.executable, sys.executable, *sys.argv)

    async def start(self, **kwargs):
        b_users, b_chats = await db.get_banned()
        temp.BANNED_USERS = b_users
        temp.BANNED_CHATS = b_chats
        await super().start()

        client = webserver.AppRunner(await bot_run())
        await client.setup()
        bind_address = "0.0.0.0"
        await webserver.TCPSite(client, bind_address, PORT_CODE).start()
        logging.info("Health check server started on port %s.", PORT_CODE)

        me = await self.get_me()
        temp.ME = me.id
        temp.U_NAME = me.username
        temp.B_NAME = me.first_name
        self.username = '@' + me.username

        logging.info(f"{me.first_name} running on Pyrogram v{__version__} (Layer {layer}) started on {me.username}.")
        logging.info(LOG_STR)
        await self.set_bot_commands(BOT_COMMANDS)
        logging.info("Bot commands synced successfully.")
        await self.send_message(chat_id=LOG_CHANNEL, text=script.RESTART_TXT)

        print("mntg4u</>")

        tz = pytz.timezone('Asia/Kolkata')
        today = date.today()
        now = datetime.now(tz)
        time = now.strftime("%H:%M:%S %p")
        await self.send_message(chat_id=LOG_CHANNEL, text=script.RESTART_GC_TXT.format(today, time))

        asyncio.create_task(self.kulasthree())
        asyncio.create_task(keep_alive())
        asyncio.create_task(run_daily_summary(self))
        asyncio.create_task(startup_warmups())

    async def stop(self, *args):
        await super().stop()
        logging.info("Bot stopped. Bye.")

    async def iter_messages(
        self,
        chat_id: Union[int, str],
        limit: int,
        offset: int = 0,
    ) -> Optional[AsyncGenerator["types.Message", None]]:
        current = offset
        while True:
            new_diff = min(200, limit - current)
            if new_diff <= 0:
                return
            messages = await self.get_messages(chat_id, list(range(current, current + new_diff + 1)))
            for message in messages:
                yield message
                current += 1


app = Bot()
app.run()
