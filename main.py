import asyncio
import logging
import os.path


from aiogram.bot import Bot
from aiogram.dispatcher import Dispatcher
from aiogram.contrib.fsm_storage.redis import RedisStorage2
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils import executor
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice, \
    PreCheckoutQuery, MediaGroup, InputFile
from aiogram.types.message import ContentTypes

from filters import AdminFilter
import warnings
from configparser import ConfigParser
import sqlite3
import copy


from keyboards import *
from tools import *
import app_logger

logger = app_logger.get_logger(__name__)

config = ConfigParser()
config.read('credentials/config.ini')
API_TOKEN = config['data']['token']
PROVIDER_TOKEN = config['data']['provider_token']
bot = Bot(token=API_TOKEN)
storage = RedisStorage2('localhost', 6379, db=5, pool_size=10, prefix='dombasicho')
loop = asyncio.get_event_loop()
dp = Dispatcher(bot=bot, storage=storage, loop=loop)
dp.filters_factory.bind(AdminFilter)


class UserStates(StatesGroup):
    type_of_rent = State()
    tariff = State()
    date = State()
    time = State()
    quantity_of_people = State()
    additionals = State()
    phone = State()
    payment = State()
    dogovor = State()

    name = State()

    row_id = State()

    new_phone = State()
    new_name = State()

    admin_phone = State()
    admin_name = State()
    admin_note = State()


async def synchronize():
    global dates
    while True:
        dates = update_dates_from_gspread()
        await asyncio.sleep(7200)



@dp.message_handler(commands=['start'], state='*')
async def start_user(message: Message, state: FSMContext):
    logger.info(f"{message.chat.id}-{message.from_user.username} started conversation")
    if get_name(message.chat.id):
        logger.info(f"{message.chat.id}-{message.from_user.username} registered")
        await message.reply(text='Добрый день! Я - чат-бот для брони в БанЧО, выберите нужную услугу:',
                            reply_markup=start_markup())
        await state.finish()
    else:
        logger.info(f"{message.chat.id}-{message.from_user.username} not_registered")
        await message.reply(text='Добрый день! Я - чат-бот для брони в БанЧО, введите, пожалуйста, ваше имя:')
        await UserStates.name.set()



@dp.message_handler(state=UserStates.name)
async def catch_name(message: Message, state: FSMContext):
    logger.info(f"{message.chat.id}-{message.from_user.username}, state={await state.get_state()} entered name for first_time - {message.text}")
    await message.reply(text=f'Добрый день {message.text}! Я - чат-бот для брони в БанЧО, выберите нужную услугу:',
                        reply_markup=start_markup())
    insert_user(message.from_user, name=message.text)
    await state.finish()


@dp.callback_query_handler(UserData.cb_data_change_phone.filter(), state='*')
async def changing_phone(call: CallbackQuery, state: FSMContext):
    logger.info(f"{call.message.chat.id}-{call.message.from_user.username}, state={await state.get_state()} asked for change_phone")
    await call.message.reply('Введите новый номер телефона:')
    await UserStates.new_phone.set()


@dp.message_handler(state=UserStates.new_phone.state)
async def changing_phone2(message: Message, state: FSMContext):
    phone = get_phone(message.chat.id)
    if message.text.isdigit():
        status = set_phone(message.chat.id, message.text)
        if status:
            logger.info(f"{message.chat.id}-{message.from_user.username}, state={await state.get_state()}"
                        f" changed phone - {message.text}, before - {phone}")
            await message.reply('Успешно изменено!')
            await state.finish()
        else:
            logger.warning(f"{message.chat.id}-{message.from_user.username}, state={await state.get_state()}"
                        f" didnt change phone - {message.text}, before - {phone}, tools error")
            await message.reply(f'Что-то пошло не так, напишите администратору, ваш chat_id- {message.chat.id}')
    else:
        logger.info(f"{message.chat.id}-{message.from_user.username} , state={await state.get_state()} "
                    f"didnt change phone - {message.text}, before - {phone}, not isdecimal")
        await message.reply('Кажется вы ввели номер в неправильном формате, попробуйте снова')


@dp.callback_query_handler(UserData.cb_data_change_name.filter(), state='*')
async def changing_name(call: CallbackQuery, state: FSMContext):
    await call.message.reply('Введите новое имя:')
    await UserStates.new_name.set()


@dp.message_handler(state=UserStates.new_name.state)
async def changing_name2(message: Message, state: FSMContext):
    name = get_name(message.chat.id)
    if message.text:
        status = set_name(message.chat.id, message.text)
        if status:
            logger.info(f'succesfully changed_name for {message.chat.id}-{message.from_user.username}, '
                        f'state={await state.get_state()}, before-{name}')
            await message.reply('Успешно изменено!')
            await state.finish()
        else:
            logger.warning(
                f'tools error for {message.chat.id}-{message.from_user.username}, didnt changed name {message.text}')
            await message.reply(f'Что-то пошло не так, напишите администратору, ваш chat_id- {message.chat.id}')
    else:
        logger.warning(f'error for {message.chat.id}-{message.from_user.username}, didnt changed name {message.text}')
        await message.reply('Кажется вы ввели имя в неправильном формате, попробуйте снова')



@dp.callback_query_handler(UserData.cb_data_menu_3.filter(), state='*')
async def about_us(call: CallbackQuery, state: FSMContext):
    with open('docs/about_us.txt', mode='r', encoding='utf-8') as f:
        txt = f.read()
        await bot.send_message(call.message.chat.id, txt)
    await call.answer()


@dp.callback_query_handler(UserData.cb_data_menu_4.filter(), state='*')
async def call_us(call: CallbackQuery, state: FSMContext):
    with open('docs/phone.txt', mode='r', encoding='utf-8') as f:
        txt = f.read()
        await bot.send_message(call.message.chat.id, txt)
    await call.answer()


@dp.callback_query_handler(UserData.cb_data_menu_1.filter(), state='*', admin=False)
async def to_book_process_0(call: CallbackQuery, state: FSMContext):
    logger.info(f'started book0(type_of_rent) {call.message.chat.id}-{call.message.from_user.username}, '
                f'state={await state.get_state()}')
    await bot.send_message(call.message.chat.id, 'Выберите формат бронирования',
                           reply_markup=to_book_process_0_markup())
    await UserStates.type_of_rent.set()
    await call.answer()


@dp.callback_query_handler(UserData.cb_data_menu_1.filter(), state='*', admin=True)
async def admin_book_process_1(call: CallbackQuery, state: FSMContext):
    await bot.send_message(call.message.chat.id, 'введите имя и номер телефона клиента через двоеточие: ИМЯ:ТЕЛЕФОН')
    await UserStates.admin_name.set()


@dp.message_handler(state=UserStates.admin_name, admin=True)
async def admin_book_process_2(message: Message, state: FSMContext):
    await state.update_data(admin_phone=message.text.split(':')[1])
    await state.update_data(admin_name=message.text.split(':')[0])
    await bot.send_message(message.chat.id, 'Выберите формат бронирования',
                           reply_markup=to_book_process_0_markup())
    await UserStates.type_of_rent.set()



@dp.callback_query_handler(UserData.cb_data_to_book_0.filter(), state=UserStates.type_of_rent)
async def to_book_process_1(call: CallbackQuery, state: FSMContext, callback_data: dict):
    logger.info(f'continue book1(tariffs) {call.message.chat.id}-{call.message.from_user.username}, '
                f'state={await state.get_state()}, call_data-{callback_data}')
    for tariff in get_tariffs(callback_data.get('type')):
        if tariff == 'bancho':
            media = MediaGroup()
            media.attach_photo(InputFile('docs/bancho_0.png'))
            media.attach_photo(InputFile('docs/bancho_1.png'))
            media.attach_photo(InputFile('docs/bancho_2.png'))
            media.attach_photo(InputFile('docs/bancho_3.png'))
            media.attach_photo(InputFile('docs/bancho_4.png'))
            await bot.send_media_group(call.message.chat.id, media=media)
            await bot.send_message(call.message.chat.id,
                                   text='Выберите действие',
                                   reply_markup=to_book_process_1_markup(tariff))
            await asyncio.sleep(1)
        elif tariff == 'basicho':
            with open('docs/basicho_1.png', mode='rb') as f:
                await bot.send_photo(call.message.chat.id, f, reply_markup=to_book_process_1_markup(tariff))
        elif tariff == 'dacho':
            with open('docs/dacho_0.png', mode='rb') as f:
                await bot.send_photo(call.message.chat.id, f, reply_markup=to_book_process_1_markup(tariff))
        elif tariff == 'dombanicho':
            with open('docs/dombanicho_0.png', mode='rb') as f:
                await bot.send_photo(call.message.chat.id, f, reply_markup=to_book_process_1_markup(tariff))
        elif tariff == 'dombasicho':
            with open('docs/dombasicho_0.png', mode='rb') as f:
                await bot.send_photo(call.message.chat.id, f, reply_markup=to_book_process_1_markup(tariff))
        elif tariff == 'domicho':
            media = MediaGroup()
            media.attach_photo(InputFile('docs/domicho_0.png'))
            media.attach_photo(InputFile('docs/domicho_1.jpg'))
            media.attach_photo(InputFile('docs/domicho_2.jpg'))
            media.attach_photo(InputFile('docs/domicho_3.jpg'))
            media.attach_photo(InputFile('docs/domicho_4.jpg'))
            await bot.send_media_group(call.message.chat.id, media=media)
            await bot.send_message(call.message.chat.id, text='Выберите действие',
                                   reply_markup=to_book_process_1_markup(tariff))
            await asyncio.sleep(1)
        elif tariff == '1':
            media = MediaGroup()
            media.attach_photo(InputFile('docs/1_1.jpg'))
            media.attach_photo(InputFile('docs/1_2.jpg'))
            media.attach_photo(InputFile('docs/1_3.jpg'))
            media.attach_photo(InputFile('docs/1_4.jpg'))
            await bot.send_media_group(call.message.chat.id, media=media)
            await bot.send_message(call.message.chat.id, text=get_description(tariff),
                                   reply_markup=to_book_process_1_markup(tariff))
            await asyncio.sleep(1)
        elif tariff == '5':
            media = MediaGroup()
            media.attach_photo(InputFile('docs/5_1.jpg'))
            media.attach_photo(InputFile('docs/5_2.jpg'))
            media.attach_photo(InputFile('docs/5_3.jpg'))
            media.attach_photo(InputFile('docs/5_4.jpg'))
            media.attach_photo(InputFile('docs/5_5.jpg'))
            await bot.send_media_group(call.message.chat.id, media=media)
            await bot.send_message(call.message.chat.id, text=get_description(tariff),
                                   reply_markup=to_book_process_1_markup(tariff))
            await asyncio.sleep(1)
        elif tariff == '10':
            media = MediaGroup()
            media.attach_photo(InputFile('docs/10_1.jpg'))
            media.attach_photo(InputFile('docs/10_2.jpg'))
            media.attach_photo(InputFile('docs/10_3.jpg'))
            media.attach_photo(InputFile('docs/10_4.jpg'))
            await bot.send_media_group(call.message.chat.id, media=media)
            await bot.send_message(call.message.chat.id, text=get_description(tariff),
                                   reply_markup=to_book_process_1_markup(tariff))
            await asyncio.sleep(1)




    await UserStates.tariff.set()
    await call.answer()


@dp.callback_query_handler(UserData.cb_data_to_book_1.filter(), state=UserStates.tariff)
async def to_book_process_20(call: CallbackQuery, state: FSMContext, callback_data: dict):
    logger.info(f'continue book2(calendar) {call.message.chat.id}-{call.message.from_user.username}, '
                f'state={await state.get_state()}, call_data-{callback_data}')
    await state.update_data(tariff=callback_data.get('tariff'))
    async with state.proxy() as data:
        tariff = data.get('tariff')
        type_of_rent = get_type_of_rent(tariff)[0]
    await bot.send_message(call.message.chat.id, 'Выберите дату/ы пребывания',
                           reply_markup=await start_calendar(year=datetime.now().year,
                                                             month=datetime.now().month,
                                                             dates=dates,
                                                             tariff=tariff,
                                                             type_of_rent=type_of_rent,
                                                             user_dates=[]
                                                             ))
    await call.answer()


@dp.callback_query_handler(UserData.calendar_callback.filter(), state=UserStates.tariff)
async def to_book_process_21(call: CallbackQuery, state: FSMContext, callback_data: dict):
    logger.info(f'continue book21(choose_dates) {call.message.chat.id}-{call.message.from_user.username}, '
                f'state={await state.get_state()}, call_data-{callback_data}')
    act = callback_data.get('act')
    year = callback_data.get('year')
    month = callback_data.get('month')
    day = callback_data.get('day')
    temp_date = datetime(int(year),
                         int(month),
                         1)
    async with state.proxy() as data:
        tariff = data.get('tariff')
        type_of_rent = get_type_of_rent(tariff)[0]
        user_dates = data.get('date')  # [[day1, month1, year1]], [day1, month1, year1]]
    if user_dates is None:
        user_dates = []
    if act == "IGNORE":
        await call.answer(cache_time=60)
    elif act == "DAY":
        if type_of_rent == 'pochas':
            user_dates = [[int(day), int(month), int(year)]]
            await state.update_data(date=user_dates)
            await call.message.edit_reply_markup(await start_calendar(int(year),
                                                                      int(month),
                                                                      dates=dates,
                                                                      tariff=tariff,
                                                                      type_of_rent=type_of_rent,
                                                                      user_dates=user_dates))
        elif type_of_rent == 'posut':
            user_dates.append([int(day), int(month), int(year)])
            await state.update_data(date=user_dates)
            await call.message.edit_reply_markup(await start_calendar(int(year),
                                                                      int(month),
                                                                      dates=dates,
                                                                      tariff=tariff,
                                                                      type_of_rent=type_of_rent,
                                                                      user_dates=user_dates))
    elif act == "PREV-YEAR":
        prev_date = temp_date - timedelta(days=365)
        await call.message.edit_reply_markup(await start_calendar(int(prev_date.year),
                                                                  int(prev_date.month),
                                                                  dates=dates,
                                                                  tariff=tariff,
                                                                  type_of_rent=type_of_rent,
                                                                  user_dates=user_dates))
    elif act == "NEXT-YEAR":
        next_date = temp_date + timedelta(days=365)
        await call.message.edit_reply_markup(await start_calendar(int(next_date.year),
                                                                  dates=dates,
                                                                  tariff=tariff,
                                                                  type_of_rent=type_of_rent,
                                                                  user_dates=user_dates))
    elif act == "PREV-MONTH":
        prev_date = temp_date - timedelta(days=1)
        await call.message.edit_reply_markup(await start_calendar(int(prev_date.year),
                                                                  int(prev_date.month),
                                                                  dates=dates,
                                                                  tariff=tariff,
                                                                  type_of_rent=type_of_rent,
                                                                  user_dates=user_dates))
    elif act == "NEXT-MONTH":
        next_date = temp_date + timedelta(days=31)
        await call.message.edit_reply_markup(await start_calendar(int(next_date.year),
                                                                  int(next_date.month),
                                                                  dates=dates,
                                                                  tariff=tariff,
                                                                  type_of_rent=type_of_rent,
                                                                  user_dates=user_dates))
    elif act == "DEL-DAY":
        index = user_dates.index([int(day), int(month), int(year)])
        user_dates.pop(index)
        await state.update_data(date=user_dates)
        await call.message.edit_reply_markup(await start_calendar(int(year),
                                                                  int(month),
                                                                  dates=dates,
                                                                  tariff=tariff,
                                                                  type_of_rent=type_of_rent,
                                                                  user_dates=user_dates))
    logger.info(f'book20 {call.message.chat.id}-{call.message.from_user.username}, '
                f'state={await state.get_state()}, user_dates-{user_dates}')
    await call.answer()


@dp.callback_query_handler(UserData.cb_data_to_book_3.filter(), state=UserStates.tariff)
async def to_book_process_3(call: CallbackQuery, state: FSMContext, callback_data: dict):
    logger.info(f'continue book3(generate_time) {call.message.chat.id}-{call.message.from_user.username}, '
                f'state={await state.get_state()}, call_data-{callback_data}')
    async with state.proxy() as data:
        tariff = data.get('tariff')
        user_dates = data.get('date')
        user_times = data.get('time')
    if user_times is None:
        user_times = []
    day = int(user_dates[0][0])
    month = int(user_dates[0][1])
    year = int(user_dates[0][2])
    await UserStates.time.set()
    await call.message.edit_text('Выберите время:')
    await call.message.edit_reply_markup(await start_time(year=year,
                                                          month=month,
                                                          day=day,
                                                          dates=dates,
                                                          tariff=tariff,
                                                          user_times=user_times))
    logger.info(f'book3(generate_time) {call.message.chat.id}-{call.message.from_user.username}, '
                f'state={await state.get_state()}, day-{day}, month-{month}, year-{year}')
    await call.answer()


@dp.callback_query_handler(UserData.cb_data_to_book_30.filter(), state=UserStates.time)
async def to_book_process_30(call: CallbackQuery, state: FSMContext, callback_data: dict):
    async with state.proxy() as data:
        tariff = data.get('tariff')
        user_dates = data.get('date')
        user_times = data.get('time')
    logger.info(f'continue book30(choose_time) {call.message.chat.id}-{call.message.from_user.username}, '
                f'state={await state.get_state()}, call_data-{callback_data}, user_times-{user_times}')
    act = callback_data.get('act')
    hour = callback_data.get('hour')
    day = int(user_dates[0][0])
    month = int(user_dates[0][1])
    year = int(user_dates[0][2])
    if user_times is None:
        user_times = []
    if act == 'nothing':
        await call.answer()
    elif act == 'add':
        user_times.append(hour)
        await state.update_data(time=user_times)
        await call.message.edit_reply_markup(await start_time(year=year,
                                                              month=month,
                                                              day=day,
                                                              dates=dates,
                                                              tariff=tariff,
                                                              user_times=user_times))
    elif act == 'del':
        index = user_times.index(hour)
        user_times.pop(index)
        await state.update_data(time=user_times)
        await call.message.edit_reply_markup(await start_time(year=year,
                                                              month=month,
                                                              day=day,
                                                              dates=dates,
                                                              tariff=tariff,
                                                              user_times=user_times))
    logger.info(f'continue book30(choose_time) {call.message.chat.id}-{call.message.from_user.username}, '
                f'state={await state.get_state()}, user_times-{user_times}')
    await call.answer()


@dp.callback_query_handler(UserData.cb_data_to_book_4.filter(), state='*')
async def to_book_process_4(call: CallbackQuery, state: FSMContext, callback_data: dict):
    logger.info(f'continue book4(peoples) {call.message.chat.id}-{call.message.from_user.username}, '
                f'state={await state.get_state()}, call_data-{callback_data}')
    await call.answer()
    async with state.proxy() as data:
        tariff = data.get('tariff')
        type_of_rent = get_type_of_rent(tariff)[0]
        user_dates = data.get('date')
        user_times = data.get('time')
        quantity_of_people = data.get('quantity_of_people')
    if type_of_rent == 'pochas' and len(user_times) < get_min_hours_book(tariff):
        logger.info(f'{call.message.chat.id}-{call.message.from_user.username} {len(user_times)} < {get_min_hours_book(tariff)}')
        await call.message.reply(text=f'Вы выбрали меньше необходимого времени - {get_min_hours_book(tariff)} часов')
    else:
        await UserStates.quantity_of_people.set()
        await call.message.reply('Введите желаемое количество человек')

    await call.answer()


@dp.message_handler(state=UserStates.quantity_of_people)
async def to_book_process_40(message: Message, state: FSMContext):
    async with state.proxy() as data:
        tariff = data.get('tariff')
        user_dates = data.get('date')
        user_times = data.get('time')
        quantity_of_people = data.get('quantity_of_people')
    if message.text.isdecimal() and int(message.text) > 0:
        if tariff in ['5', '10', '7', '1']:
            markup = InlineKeyboardMarkup()
            markup.insert(InlineKeyboardButton(text='Забронировать!', callback_data=UserData.cb_data_to_book_6.new()))
            await bot.send_message(message.chat.id, f'Вы выбрали {message.text} человек', reply_markup=markup)
        else:
            await UserStates.additionals.set()
            await bot.send_message(message.chat.id, f'Вы выбрали {message.text} человек. Выберите дополнительные улсуги',
                                   reply_markup=await additionals_markup())
    else:
        await message.reply('Вы ввели невалидное число человек, попробуйте снова')


@dp.callback_query_handler(UserData.cb_data_to_book_50.filter(), state=UserStates.additionals)
async def to_book_process_50(call: CallbackQuery, state: FSMContext, callback_data: dict):
    logger.info(f'continue book50(additionals) {call.message.chat.id}-{call.message.from_user.username}, '
                f'state={await state.get_state()}, call_data-{callback_data}')
    async with state.proxy() as data:
        additionals = data.get('additionals')
    if additionals is None:
        additionals = []
    name = callback_data.get('name')
    if name not in additionals:
        additionals.append(name)
        await state.update_data(additionals=additionals)
        await call.message.edit_reply_markup(reply_markup=await additionals_markup(used=additionals))
    else:
        index = additionals.index(name)
        additionals.pop(index)
        await state.update_data(additionals=additionals)
        await call.message.edit_reply_markup(reply_markup=await additionals_markup(used=additionals))
    logger.info(f'continue book50(additionals) {call.message.chat.id}-{call.message.from_user.username}, '
                f'state={await state.get_state()}, ads-{additionals}')
    await call.answer()


@dp.callback_query_handler(UserData.cb_data_to_book_6.filter(), state='*', admin=True)
async def to_book_process_6(call: CallbackQuery, state: FSMContext, callback_data: dict):
    global dates
    async with state.proxy() as data:
        phone = data.get('admin_phone')
        name = data.get('admin_name')
        tariff = data.get('tariff')
        user_dates = data.get('date')
        user_times = data.get('time')
        quantity_of_people = data.get('quantity_of_people')
        additionals = data.get('additionals')
        list_to_sql = pre_load_order(call.message.chat.id, data, phone=phone)
        await state.finish()
        if list_to_sql:
            dates_journal = append_to_dates(dates, *list_to_sql)
            dates = dates_journal.copy()
            status = insert_order(*list_to_sql)
            list_to_gspread = pre_load_to_insert_into_gspread(*list_to_sql, name=name)
            status2 = insert_into_gspread(list_to_gspread)
            if not status or not status2:
                await bot.send_message(call.message.chat.id,
                                        f'Успешно оплачено, но ошибка записи у пользователя {call.message.from_user.username}'
                                        f'фио - {name}'
                                        f'телефон - {phone}'
                                        f'тариф - {tariff}, даты- {user_dates}, время - {user_times}, '
                                        f'количество человек - {quantity_of_people}, допы - {additionals} '
                                        'необходимо вручную ввести в таблицу')
            else:
                await call.message.reply('Ваша бронь принята', reply_markup=start_markup())


@dp.callback_query_handler(UserData.cb_data_to_book_6.filter(), state='*', admin=False)
async def to_book_process_6(call: CallbackQuery, state: FSMContext, callback_data: dict):
    if get_phone(call.message.chat.id):
        await call.message.reply('Вы уже вводили ранее номер телефона поэтому сейчас можете перейти сразу к заказу'
                                 'Ознакомьтесь с договором!')

        async with state.proxy() as data:
            tariff = data.get('tariff')
            logger.warning(
                f'{call.message.chat.id}-{call.message.from_user.username}, data-{data}')
        if get_belong(tariff) == 'complex':
            text = 'https://telegra.ph/Dogovor-vozmezdnogo-okazaniya-uslug-po-vremennomu-prebyvaniyu-na-territorii-dachnogo-kompleksa-DaCHO-i-BanCHO-08-18'

        else:
            text = get_dogovor(tariff)
        await bot.send_message(call.message.chat.id, text, reply_markup=await dogovor())


        await UserStates.dogovor.set()
    else:
        logger.info(f'no phone {call.message.chat.id}-{call.message.from_user.username}')
        await call.message.reply('Для завершения заказа отправьте ваш номер телефона в формате 8хххххххххх')
        await UserStates.phone.set()
    await call.answer()


@dp.callback_query_handler(UserData.cb_data_to_book_7.filter(), state='*', admin=False)
async def to_book_process_7(call:CallbackQuery, state:FSMContext, callback_data):
    global dates
    if callback_data.get('yo') == 'Да':
        async with state.proxy() as data:
            tariff = data.get('tariff')
            user_dates = data.get('date')
            user_times = data.get('time')
            quantity_of_people = data.get('quantity_of_people')
            additionals = data.get('additionals')
            list_to_sql = pre_load_order(call.message.chat.id, data)
            logger.warning(
                f'{call.message.chat.id}-{call.message.from_user.username}, data-{data}')
        if additionals is None:
            additionals = []
        deposit = get_deposit(tariff)
        if deposit != 'None':
            logger.info(f'{call.message.chat.id}-{call.message.from_user.username} waiting for invoice {deposit}')
            await call.message.reply('Отлично! Вы верифицированы, теперь нужно внести предоплату')
            await UserStates.payment.set()
            prices = [LabeledPrice(label='предоплата по тарифу', amount=int(deposit) * 100)]
            await bot.send_invoice(call.message.chat.id, title='предоплата по тарифу',
                                   description='предоплата по тарифу',
                                   provider_token=PROVIDER_TOKEN,
                                   currency='rub',
                                   prices=prices,
                                   start_parameter='start_parameter',
                                   payload='payload')
        else:
            logger.info(f'{call.message.chat.id}-{call.message.from_user.username} deposited {deposit}')
            await call.message.reply('Ваша бронь принята', reply_markup=start_markup())
            await state.finish()
            if list_to_sql:
                dates_journal = append_to_dates(dates, *list_to_sql)
                dates = dates_journal.copy()
                status = insert_order(*list_to_sql)
                list_to_gspread = pre_load_to_insert_into_gspread(*list_to_sql)
                status2 = insert_into_gspread(list_to_gspread)
                logger.info(f'{call.message.chat.id}-{call.message.from_user.username} ordered {list_to_sql}')
                if not status:
                    logger.warning(f'{call.message.chat.id}-{call.message.from_user.username} error with local db')
                if not status2:
                    logger.warning(f'{call.message.chat.id}-{call.message.from_user.username} error with gspread '
                                   f'{list_to_gspread}')
                if not status or not status2:
                    for c_id in get_admins_list():
                        await bot.send_message(int(c_id),
                                               f'Успешно оплачено, но ошибка записи у пользователя {call.message.from_user.username}'
                                               f'фио - {get_name(call.message.chat.id)}'
                                               f'телефон - {get_phone(call.message.chat.id)}'
                                               f'тариф - {tariff}, даты- {user_dates}, время - {user_times}, '
                                               f'количество человек - {quantity_of_people}, допы - {additionals} '
                                               'необходимо вручную ввести в таблицу')
            else:
                for c_id in get_admins_list():
                    await bot.send_message(int(c_id),
                                           f'Успешно оплачено, но ошибка записи у пользователя {call.message.from_user.username}'
                                           f'фио - {get_name(call.message.chat.id)}'
                                           f'телефон - {get_phone(call.message.chat.id)}'
                                           f'тариф - {tariff}, даты- {user_dates}, время - {user_times}, '
                                           f'количество человек - {quantity_of_people}, допы - {additionals} '
                                           'необходимо вручную ввести в таблицу')
    else:
        with open('docs/phone.txt', mode='r', encoding='utf-8') as f:
            txt = f.read()
            await bot.send_message(call.message.chat.id, txt, reply_markup=start_markup())
            await state.finish()


@dp.message_handler(state=UserStates.phone, admin=False)
async def to_book_process_60(message: Message, state: FSMContext):
    if len(message.text) == 11 and message.text[0] == '8':
        set_phone(message.chat.id, message.text)
        await UserStates.payment.set()
        await state.update_data(phone=message.text)

        async with state.proxy() as data:
            tariff = data.get('tariff')
        if get_belong(tariff) == 'complex':
            text = 'https://telegra.ph/Dogovor-vozmezdnogo-okazaniya-uslug-po-vremennomu-prebyvaniyu-na-territorii-dachnogo-kompleksa-DaCHO-i-BanCHO-08-18'
        else:
            text = 'telegraph договор'
        await bot.send_message(message.chat.id, text, reply_markup=await dogovor())
        await UserStates.dogovor.set()
    else:
        await message.reply('Кажется что-то пошло не так, попробуйте снова', reply_markup=start_markup())
        await state.finish()


@dp.pre_checkout_query_handler(lambda query: True, state=UserStates.payment, admin=False)
async def checkout(pre_checkout_query: PreCheckoutQuery):
    logging.warning(f'pre checkout user {pre_checkout_query.from_user.id}-{pre_checkout_query.from_user.username}, '
                    f'id-{pre_checkout_query.id}')
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True,
                                        error_message="Aliens tried to steal your card's CVV,"
                                                      " but we successfully protected your credentials,"
                                                      " try to pay again in a few minutes, we need a small rest.")


@dp.message_handler(content_types=ContentTypes.SUCCESSFUL_PAYMENT, state=UserStates.payment, admin=False)
async def got_payment(message: Message, state: FSMContext):
    global dates
    logging.warning(f'user {message.from_user.id}-{message.from_user.username}, payd for deposit')
    await bot.send_message(message.chat.id,
                           'Hoooooray! Thanks for payment! We will proceed your order for `{} {}`'
                           ' as fast as possible! Stay in touch.'.format(
                               message.successful_payment.total_amount, message.successful_payment.currency),
                           parse_mode='Markdown')
    async with state.proxy() as data:
        list_to_sql = pre_load_order(message.chat.id, data)
        tariff = data.get('tariff')
        user_dates = data.get('date')
        user_times = data.get('time')
        quantity_of_people = data.get('quantity_of_people')
        additionals = data.get('additionals')
    await message.reply('Ваша бронь принята', reply_markup=start_markup())
    await state.finish()
    if list_to_sql:
        dates_journal = append_to_dates(dates, *list_to_sql)
        dates = dates_journal.copy()
        status = insert_order(*list_to_sql)
        list_to_gspread = pre_load_to_insert_into_gspread(*list_to_sql)
        status2 = insert_into_gspread(list_to_gspread)
        logger.info(f'{message.chat.id}-{message.from_user.username} ordered {list_to_sql}')
        if not status:
            logger.warning(f'{message.chat.id}-{message.from_user.username} error with local db')
        if not status2:
            logger.warning(f'{message.chat.id}-{message.from_user.username} error with gspread '
                           f'{list_to_gspread}')
        if not status or not status2:
            for c_id in get_admins_list():
                await bot.send_message(c_id,
                                       f'Успешно оплачено, но ошибка записи у пользователя {message.from_user.username}'
                                       f'фио - {get_name(message.chat.id)}'
                                       f'телефон - {get_phone(message.chat.id)}'
                                       f'тариф - {tariff}, даты- {user_dates}, время - {user_times}, '
                                       f'количество человек - {quantity_of_people}, допы - {additionals} '
                                       'необходимо вручную ввести в таблицу')
    else:
        logger.warning(f'{message.chat.id}-{message.from_user.username} error with local db')
        logger.warning(f'{message.chat.id}-{message.from_user.username} data-{data}')
        for c_id in get_admins_list():
            await bot.send_message(c_id,
                                   f'Успешно оплачено, но ошибка записи у пользователя {message.from_user.username}'
                                   f'фио - {get_name(message.chat.id)}'
                                   f'телефон - {get_phone(message.chat.id)}'
                                   f'тариф - {tariff}, даты- {user_dates}, время - {user_times}, '
                                   f'количество человек - {quantity_of_people}, допы - {additionals} '
                                   'необходимо вручную ввести в таблицу')


async def on_shutdown(dp: Dispatcher):
    await dp.storage.close()
    await dp.storage.wait_closed()
    cur.close()
    conn.close()


@dp.callback_query_handler(UserData.cb_data_menu_2.filter(), state='*')
async def delete_book_process_0(call: CallbackQuery, state: FSMContext, callback_data: dict):
    logger.info(f'user-{call.message.chat.id}-{call.message.from_user.username} want to delete order')
    await bot.send_message(chat_id=call.message.chat.id,
                           text='Выберите бронь, которую нужно удалить')
    for row_id, text in get_orders_from_gspread_by_phone_number(get_phone(call.message.chat.id)).items():
        await bot.send_message(call.message.chat.id, text=text,
                               reply_markup=await delete_book_markup(row_id))
    await UserStates.row_id.set()


@dp.callback_query_handler(UserData.cb_data_delete_order_0.filter(), state=UserStates.row_id)
async def delete_book_process_1(call: CallbackQuery, state: FSMContext, callback_data: dict):
    logger.warning(f'user-{call.message.chat.id}-{call.message.from_user.username} want to delete order {callback_data}')
    await state.update_data(row_id=callback_data.get('row_id'))
    await call.message.reply('Вы уверены???', reply_markup=await y_n_markup())


@dp.callback_query_handler(UserData.cb_data_delete_order_1.filter(), state=UserStates.row_id)
async def delete_book_process_2(call: CallbackQuery, state: FSMContext, callback_data: dict):
    global dates
    if callback_data.get('answ') == 'Да':
        async with state.proxy() as data:
            status = delete_order_by_row_id(int(data.get('row_id')))
            if status:
                logger.warning(
                    f'user-{call.message.chat.id}-{call.message.from_user.username} deleted order {int(data.get("row_id"))}')
                dates_journal = delete_from_dates(dates, int(data.get('row_id')))
                dates = dates_journal.copy()
                await call.message.reply(text='Успешно удалено!',
                                         reply_markup=start_markup())
                await state.finish()
            else:
                logger.warning(
                    f'user-{call.message.chat.id}-{call.message.from_user.username} cant delete order {int(data.get("row_id"))}')
                await call.message.reply(text='Что то пошло не так, обратитесь к администратору!',
                                         reply_markup=start_markup())
                await state.finish()
    elif callback_data.get('answ') == 'Нет':
        await call.message.reply(text='Вы в главном меню!',
                            reply_markup=start_markup())
        await state.finish()


if __name__ == "__main__":
    dp.loop.create_task(synchronize())
    executor.start_polling(dp, skip_updates=True, on_shutdown=on_shutdown)
