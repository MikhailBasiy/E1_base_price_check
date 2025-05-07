import asyncio
from os import getenv

from aiogram import Bot, Dispatcher, Router, html
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message
from dotenv import load_dotenv
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

dp = Dispatcher()
router = Router()
dp.include_router(router=router)


async def show_menu(message: Message):
    inline_menu = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Провести сверку базовых цен", callback_data="start")],
        [InlineKeyboardButton(text="Провести сверку цен с промо", callback_data="help")]
    ])
    await message.answer("Выбери, что нужно сделать:", reply_markup=inline_menu)


@router.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    await message.answer(f"Привет, {html.bold(message.from_user.first_name)}! Начнем?")
    await show_menu(message)


@router.message()
async def any_text_handler(message: Message) -> None:
    # await message.answer(f"")
    await show_menu(message)


async def main() -> None:
    load_dotenv()
    TOKEN = getenv("TOKEN")
    bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
