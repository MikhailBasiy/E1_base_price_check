from aiogram import Bot, Dispatcher, html
import asyncio
from dotenv import load_dotenv

from os import getenv
from aiogram.filters import CommandStart
from aiogram.types import Message

from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode


dp = Dispatcher()


@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    await message.answer(f"Hello, {html.bold(message.from_user.full_name)}!")


async def main() -> None:
    load_dotenv()
    TOKEN = getenv("TOKEN")
    bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())