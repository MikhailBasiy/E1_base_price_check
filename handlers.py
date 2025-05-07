import asyncio
from io import BytesIO

from aiogram import F, Router, html
from aiogram.filters import CommandStart
from aiogram.types import (BufferedInputFile, CallbackQuery,
                           InlineKeyboardButton, InlineKeyboardMarkup, Message)

from core.func import check_base_prices, update_base_prices_in_db

router = Router()


async def show_menu(message: Message):
    inline_menu = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Провести сбор и сверку базовых цен",
                    callback_data="check_base_prices",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Обновить базовые цены в БД",
                    callback_data="update_base_prices_in_db",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Провести сверку цен с промо", callback_data="help"
                )
            ],
        ]
    )
    await message.answer("Выбери, что нужно сделать:", reply_markup=inline_menu)


@router.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    await message.answer(f"Привет, {html.bold(message.from_user.first_name)}! Начнем?")
    await show_menu(message)


@router.message()
async def any_text_handler(message: Message) -> None:
    # await message.answer(f"")
    await show_menu(message)


@router.callback_query(F.data == "check_base_prices")
async def check_base_prices_handler(callback=CallbackQuery):
    await callback.answer(
        "🔄 Сбор и сравнение цен запущены. Это может занять несколько минут..."
    )
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, check_base_prices)

    buffer = BytesIO()
    result.to_excel(buffer, index=False, engine="xlsxwriter")
    buffer.seek(0)

    file = BufferedInputFile(
        buffer.read(), filename="Результат проверки базовых цен.xlsx"
    )
    await callback.message.answer_document(file)


@router.callback_query(F.data == "update_base_prices_in_db")
async def update_base_prices_in_db_handler(callback=CallbackQuery):
    await callback.answer("🔄 Сбор цен с сайта по API запущен...")
    loop = asyncio.get_running_loop()
    prices_qty = await loop.run_in_executor(None, update_base_prices_in_db)
    await callback.message.answer(
        f"✅ Базовые цены в БД успешно обновлены. Записано строк:{prices_qty}."
    )
