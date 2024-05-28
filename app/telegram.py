import asyncio
import logging
import json
import os
from aiogram import Bot, Dispatcher
from aiogram import F 
from aiogram.types import Message
from aiogram.filters.command import Command
from db import maindb

logging.basicConfig(level=logging.INFO)
bot = Bot(token=os.environ['BOT_TOKEN'])
dp = Dispatcher()

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer("Hello!")

@dp.message(F.text)
async def querry(message: Message):
    try:
        messageJson = json.loads(message.text)
        querrry = await maindb(messageJson)

        await message.answer(str(querrry))
    except Exception as e:
        await message.answer(
            "Невалидный запос. Пример запроса:\n"
            '"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00", "group_type": "month"')

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
