from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.callback_data import CallbackData
from aiogram.types import CallbackQuery
import calendar
from datetime import datetime, timedelta
from tools import *


class UserData:
    cb_data_menu_1 = CallbackData('to_book')
    cb_data_menu_2 = CallbackData('delete_book')
    cb_data_menu_3 = CallbackData('about_us')
    cb_data_menu_4 = CallbackData('call_us')

    cb_data_change_phone = CallbackData('change_phone')
    cb_data_change_name = CallbackData('change_name')

    cb_data_to_book_0 = CallbackData('choose_type_of_rent', 'type')
    cb_data_to_book_1 = CallbackData('choose_tariff', 'tariff')
    cb_data_to_book_3 = CallbackData('choose_time_arrival')
    cb_data_to_book_30 = CallbackData('time', 'act', 'hour')
    cb_data_to_book_4 = CallbackData('select_people')
    cb_data_to_book_40 = CallbackData('people', 'act')
    cb_data_to_book_5 = CallbackData('select_additionals')
    cb_data_to_book_50 = CallbackData('ads', 'name', 'cost')
    cb_data_to_book_6 = CallbackData('finish')
    cb_data_to_book_7 = CallbackData('dogovor', 'yo')

    cb_data_delete_order_0 = CallbackData('delete', 'row_id')
    cb_data_delete_order_1 = CallbackData('y_o_process', 'answ')

    calendar_callback = CallbackData('simple_calendar', 'act', 'year', 'month', 'day')


def start_markup() -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup(row_width=2)
    markup.insert(InlineKeyboardButton(text='Забронировать',
                                       callback_data=UserData.cb_data_menu_1.new()))
    markup.insert(InlineKeyboardButton(text='Отменить бронь',
                                       callback_data=UserData.cb_data_menu_2.new()))
    markup.insert(InlineKeyboardButton(text='О нас',
                                       callback_data=UserData.cb_data_menu_3.new()))
    markup.insert(InlineKeyboardButton(text='Позвонить',
                                       callback_data=UserData.cb_data_menu_4.new()))
    markup.insert(InlineKeyboardButton(text='Изменить номер телефона',
                                       callback_data=UserData.cb_data_change_phone.new()))
    markup.insert(InlineKeyboardButton(text='Изменить имя',
                                       callback_data=UserData.cb_data_change_name.new()))
    return markup


def to_book_process_0_markup() -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup(row_width=2)
    markup.insert(InlineKeyboardButton(text='Квартиры посуточно',
                                       callback_data=UserData.cb_data_to_book_0.new(type='apartments')))
    markup.insert(InlineKeyboardButton(text='Комплекс ДаЧО&БанЧО',
                                       callback_data=UserData.cb_data_to_book_0.new(type='complex')))
    return markup


def to_book_process_1_markup(tariff) -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup(row_width=1)
    if tariff in ['5', '10', '7']:
        markup.insert(InlineKeyboardButton(text=f'Забронировать эту квартиру:',
                                           callback_data=UserData.cb_data_to_book_1.new(tariff=tariff)))
    else:
        markup.insert(InlineKeyboardButton(text=f'Забронировать этот тариф:',
                                           callback_data=UserData.cb_data_to_book_1.new(tariff=tariff)))
    #if avito:
    #    markup.insert(InlineKeyboardButton(text='Ссылка на авито', url=avito))
    return markup


async def start_calendar(
        year: int = datetime.now().year,
        month: int = datetime.now().month,
        dates: DataFrame = DataFrame(columns=['tariff', 'year', 'month', 'day', 'time']),
        tariff: str = '',
        type_of_rent: str = 'posut',
        user_dates: list = []) -> InlineKeyboardMarkup:
    if user_dates is None:
        user_dates = []
    inline_kb = InlineKeyboardMarkup(row_width=7)
    ignore_callback = UserData.calendar_callback.new("IGNORE", year, month, 0)  # for buttons with no answer
    # First row - Month and Year
    inline_kb.row()
    inline_kb.insert(InlineKeyboardButton(
        "<<",
        callback_data=UserData.calendar_callback.new("PREV-YEAR", year, month, 1)
    ))
    inline_kb.insert(InlineKeyboardButton(
        f'{calendar.month_name[month]} {str(year)}',
        callback_data=ignore_callback
    ))
    inline_kb.insert(InlineKeyboardButton(
        ">>",
        callback_data=UserData.calendar_callback.new("NEXT-YEAR", year, month, 1)
    ))
    # Second row - Week Days
    inline_kb.row()
    for day in ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"]:
        inline_kb.insert(InlineKeyboardButton(day, callback_data=ignore_callback))
    if type_of_rent == 'posut':
        # Calendar rows - Days of month
        month_calendar = calendar.monthcalendar(year, month)
        for week in month_calendar:
            inline_kb.row()
            for day in week:
                if day == 0:
                    inline_kb.insert(InlineKeyboardButton(" ", callback_data=ignore_callback))
                    continue
                elif [int(day), int(month), int(year)] in user_dates:
                    inline_kb.insert(InlineKeyboardButton(str(day) + '✅',
                                                          callback_data=UserData.calendar_callback.new("DEL-DAY",
                                                                                                       year,
                                                                                                       month,
                                                                                                       day)))
                elif any(list(x in [14, 15, 16, 17, 18, 19, 20, 21, 22, 23]
                              for x in list(dates[(dates.tariff == tariff) &
                                                  (dates.year == year) &
                                                  (dates.month == month) &
                                                  (dates.day == day)]['time']))):
                    inline_kb.insert(InlineKeyboardButton(str(day) + '❌',
                                                          callback_data=ignore_callback))
                else:
                    inline_kb.insert(InlineKeyboardButton(
                        str(day), callback_data=UserData.calendar_callback.new("DAY", year, month, day)
                    ))
    else:
        month_calendar = calendar.monthcalendar(year, month)
        for week in month_calendar:
            inline_kb.row()
            for day in week:
                if day == 0:
                    inline_kb.insert(InlineKeyboardButton(" ", callback_data=ignore_callback))
                    continue
                elif len(dates[(dates.tariff == tariff) &
                               (dates.year == year) &
                               (dates.month == month) &
                               (dates.day == day)]) >= 15:
                    inline_kb.insert(InlineKeyboardButton(str(day) + '❌',
                                                          callback_data=ignore_callback))
                elif [int(day), int(month), int(year)] in user_dates:
                    inline_kb.insert(InlineKeyboardButton(str(day) + '✅',
                                                          callback_data=UserData.calendar_callback.new("DEL-DAY",
                                                                                                       year,
                                                                                                       month,
                                                                                                       day)))
                else:
                    inline_kb.insert(InlineKeyboardButton(
                        str(day), callback_data=UserData.calendar_callback.new("DAY", year, month, day)
                    ))
    # Last row - Buttons
    inline_kb.row()
    inline_kb.insert(InlineKeyboardButton(
        "<", callback_data=UserData.calendar_callback.new("PREV-MONTH", year, month, day)
    ))
    inline_kb.insert(InlineKeyboardButton(" ", callback_data=ignore_callback))
    inline_kb.insert(InlineKeyboardButton(
        ">", callback_data=UserData.calendar_callback.new("NEXT-MONTH", year, month, day)
    ))
    if type_of_rent == 'pochas':
        inline_kb.add(InlineKeyboardButton('Выбрать время заселения:',
                                           callback_data=UserData.cb_data_to_book_3.new()))
    else:
        inline_kb.add(InlineKeyboardButton('Ввести количество человек:',
                                           callback_data=UserData.cb_data_to_book_4.new()))
    return inline_kb


async def start_time(
        year: int = datetime.now().year,
        month: int = datetime.now().month,
        day: int = datetime.now().day,
        dates: DataFrame = DataFrame(columns=['tariff', 'year', 'month', 'day', 'time']),
        tariff: str = '',
        user_times: list = []) -> InlineKeyboardMarkup:
    time_markup = InlineKeyboardMarkup(row_width=4)
    for time in ['09.00', '10.00', '11.00', '12.00', '13.00', '14.00', '15.00', '16.00', '17.00', '18.00', '19.00',
                 '20.00', '21.00', '22.00', '23.00', '00.00', '01.00']:
        if not dates[(dates.tariff == tariff) &
                     (dates.year == year) &
                     (dates.month == month) &
                     (dates.day == day) &
                     (int(time.split('.')[0]) == dates.time)].empty:
            time_markup.insert(InlineKeyboardButton(text=time + '❌',
                                                    callback_data=UserData.cb_data_to_book_30.new(act='nothing',
                                                                                                  hour=time)))
        elif time in user_times:
            time_markup.insert(InlineKeyboardButton(text=time + '✅',
                                                    callback_data=UserData.cb_data_to_book_30.new(act='del',
                                                                                                  hour=time)))
        else:
            time_markup.insert(InlineKeyboardButton(text=time,
                                                    callback_data=UserData.cb_data_to_book_30.new(act='add',
                                                                                                  hour=time)))
    time_markup.add(InlineKeyboardButton('Ввести количество человек:',
                                         callback_data=UserData.cb_data_to_book_4.new()))
    return time_markup


async def quantity_of_people_markup(tariff: str,
                                    quantity: int) -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup()
    maxcount = get_max_count_people(tariff)
    if maxcount:
        markup.add(InlineKeyboardButton(text=f'Не более {maxcount} человек',
                                        callback_data=UserData.cb_data_to_book_40.new(act='nothing')))
    else:
        maxcount = 100
    if quantity < maxcount:
        markup.add(InlineKeyboardButton(text='+1',
                                        callback_data=UserData.cb_data_to_book_40.new(act='plus')))
    markup.add(InlineKeyboardButton(text=f'На данный момент {quantity} человек',
                                    callback_data=UserData.cb_data_to_book_40.new(act='nothing')))
    markup.add(InlineKeyboardButton(text='-1',
                                    callback_data=UserData.cb_data_to_book_40.new(act='minus')))
    if tariff in ['5', '10', '7', '1']:
        markup.add(InlineKeyboardButton(text='Забронировать!',
                                        callback_data=UserData.cb_data_to_book_6.new()))
    else:
        markup.add(InlineKeyboardButton(text='Выбрать дополнительные услуги',
                                        callback_data=UserData.cb_data_to_book_5.new()))
    return markup


async def additionals_markup(used=[]) -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup(row_width=2)
    for name, cost in get_additionals().items():
        if name in used:
            markup.insert(InlineKeyboardButton(text=name + '✅',
                                               callback_data=UserData.cb_data_to_book_50.new(name=name, cost=cost)))
        else:
            markup.insert(InlineKeyboardButton(text=name,
                                               callback_data=UserData.cb_data_to_book_50.new(name=name, cost=cost)))
    markup.add(InlineKeyboardButton(text='Забронировать!',
                                    callback_data=UserData.cb_data_to_book_6.new()))
    return markup


async def delete_book_markup(id) -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup(row_width=1)
    markup.insert(InlineKeyboardButton(text='Удалить эту бронь',
                                           callback_data=UserData.cb_data_delete_order_0.new(row_id=id)))
    return markup


async def y_n_markup() -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup(row_width=2)
    markup.insert(InlineKeyboardButton(text='Да',
                                       callback_data=UserData.cb_data_delete_order_1.new(answ='Да')))
    markup.insert(InlineKeyboardButton(text='Нет',
                                       callback_data=UserData.cb_data_delete_order_1.new(answ='Нет')))
    return markup


class AdminData():
    a_cb_data_0 = CallbackData('add_order')


async def admin_start_markup() -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup(row_width=1)
    markup.insert(InlineKeyboardButton(text='Добавить заказ', callback_data=AdminData.a_cb_data_0.new()))
    return markup


async def dogovor() -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup(row_width=2)
    markup.insert(InlineKeyboardButton(text='Согласиться',
                                       callback_data=UserData.cb_data_to_book_7.new(yo='Да')))
    markup.insert(InlineKeyboardButton(text='Связаться с администратором',
                                       callback_data=UserData.cb_data_to_book_7.new(yo='Связаться с администратором')))
    return markup