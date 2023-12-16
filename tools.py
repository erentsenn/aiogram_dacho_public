import sqlite3
from aiogram.types import User
from itertools import chain
from aiogram.dispatcher.storage import FSMContextProxy
import gspread
from pandas import DataFrame
from datetime import datetime, timedelta, date
from configparser import ConfigParser
import re
import app_logger

logger = app_logger.get_logger(__name__)

conn = sqlite3.connect('database_aiogram.db')
cur = conn.cursor()

config = ConfigParser()
config.read('credentials/config.ini')
gspread_key = config['data']['gspread_key']
gc = gspread.service_account(filename='credentials/gspread.json')
sh = gc.open_by_key(gspread_key)
worksheet = sh.sheet1

dict_from_int_to_rus = {1: 'января',
                        2: 'февраля',
                        3: 'марта',
                        4: 'апреля',
                        5: 'мая',
                        6: 'июня',
                        7: 'июля',
                        8: 'августа',
                        9: 'сентября',
                        10: 'октября',
                        11: 'ноября',
                        12: 'декабря'}

dict_from_rus_to_int = {'января': 1,
                        'февраля': 2,
                        'марта': 3,
                        'апреля': 4,
                        'мая': 5,
                        'июня': 6,
                        'июля': 7,
                        'августа': 8,
                        'сентября': 9,
                        'октября': 10,
                        'ноября': 11,
                        'декабря': 12}

weekdays_dict = {0: 'Понедельник',
                 1: 'Вторник',
                 2: 'Среда',
                 3: 'Четверг',
                 4: 'Пятница',
                 5: 'Суббота',
                 6: 'Воскресенье'}

dict_from_bancho_to_gspread = {'ДаЧО': 'dacho',
                               'БанЧО': 'bancho',
                               'ДОМиЧО': 'domicho',
                               'БанЧО +': 'bancho +',
                               'БассиЧо': 'basicho',
                               'ДомБассиЧо': 'dombasicho',
                               'ДомБаниЧо': 'dombanicho',
                               '10 мкр': '10',
                               '5 мкр': '5',
                               '1 мкр': '1',
                               '7 мкр': '7'}

dict_from_gspread_to_bancho = {'dacho': 'ДаЧО',
                               'bancho': 'БанЧО',
                               'domicho': 'ДОМиЧО',
                               'bancho +': 'БанЧО +',
                               'basicho': 'БассиЧо',
                               'dombasicho': 'ДомБассиЧо',
                               'dombanicho': 'ДомБаниЧо',
                               '10': '10 мкр',
                               '5': '5 мкр',
                               '1': '1 мкр',
                               '7': '7 мкр'}

def get_belong(tariff):
    cur.execute(
        f"""
                SELECT belong FROM tariffs WHERE tariff=?
                """,
        (tariff,)
    )
    return list(*cur.fetchall())[0]


def get_description(tariff):
    cur.execute(
        f"""
                SELECT description FROM tariffs WHERE tariff=?
                """,
        (tariff,)
    )
    return list(*cur.fetchall())[0]



def find_admin(username: str) -> bool:
    info = cur.execute('SELECT * FROM admins WHERE username=?', (username,))
    if info.fetchone() is None:
        logger.info(f'{username} is not admin')
        return False
    else:
        logger.info(f'{username} is admin')
        return True


def check_user(chat_id: int):
    info = cur.execute('SELECT * FROM users WHERE chat_id=?', (chat_id,))
    if info.fetchone() is None:
        logger.info(f'{chat_id} not in users table')
        return False
    else:
        logger.info(f'{chat_id} in users table')
        return True


def insert_user(user: User, name, phone='Null'):
    if check_user(user.id):
        logger.info(f'{user.id} is already in users table')
        pass
    else:
        cur.execute(
            f'''
            INSERT INTO 
            users (name, chat_id, username, phone, time_modified) 
            VALUES (?, ?, ?, ?, ?)''',
            (name, user.id, user.username, phone, datetime.now())
        )
        conn.commit()
        logger.info(f'{user.id} has successfully inserted in users table')


def get_type_of_rent(tariff):
    cur.execute(
        f"""
            SELECT type_of_rent FROM tariffs WHERE tariff=?
            """,
        (tariff,)
    )
    return list(*cur.fetchall())



def get_tariffs(belong):
    cur.execute(
        f"""
        SELECT tariff FROM tariffs WHERE belong=?
        """,
        (belong,)
    )

    res = cur.fetchall()
    print(res, belong)
    result = [res[x][0] for x in range(len(res))]
    print(result)
    return result


def get_dogovor(tariff):
    cur.execute(
        f"""
            SELECT dogovor FROM tariffs WHERE tariff=?
            """,
        (tariff,)
    )
    return list(*cur.fetchall())[0]


def update_dates_from_gspread() -> DataFrame:
    values_list = DataFrame(worksheet.get_all_values(), columns=['ФИО',
                                                                 'телефон',
                                                                 'тариф',
                                                                 'дата заезда',
                                                                 'Месяц',
                                                                 'дата выезда',
                                                                 'итого суток',
                                                                 'время заезда',
                                                                 'время выезда',
                                                                 'итого часов',
                                                                 'сумма брони',
                                                                 'сумма долга',
                                                                 'уборка оплачена',
                                                                 'ИТОГО',
                                                                 'человек',
                                                                 'допы (в,к,л,чаша, д,бк,чай, мж,мс,х,п,)',
                                                                 'общая сумма допов ',
                                                                 'Доп.услуга',
                                                                 'Кол-во',
                                                                 'Способ оплаты',
                                                                 'Статус',
                                                                 'комментарии',
                                                                 'test_column'])
    dates = DataFrame(columns=['tariff', 'year', 'month', 'day', 'time'])
    for key, row in values_list.iterrows():
        if row['дата заезда'] and row['дата заезда'] != 'дата заезда' and row['Статус'] != 'Отказ':
            day_arrival = int(row['дата заезда'].split(',')[1].split()[0])
            month_arrival = dict_from_rus_to_int.get(row['дата заезда'].split(',')[1].split()[1])
            year_arrival = int(row['дата заезда'].split(',')[1].split()[2])
            tariff = row['тариф']
            if tariff == 'Весь комплекс':
                for t in ['ДаЧО', 'БанЧО', 'ДОМиЧО', 'БанЧО +', 'БассиЧо', 'ДомБассиЧо', 'ДомБаниЧо']:
                    if row['дата выезда']:
                        time_arrival = datetime(day=day_arrival, month=month_arrival, year=year_arrival, hour=14)
                        day_eviction = int(row['дата выезда'].split(',')[1].split()[0])
                        month_eviction = dict_from_rus_to_int.get(row['дата выезда'].split(',')[1].split()[1])
                        year_eviction = int(row['дата выезда'].split(',')[1].split()[2])
                        time_eviction = datetime(day=day_eviction, month=month_eviction, year=year_eviction, hour=12)
                        if row['время заезда']:
                            hour_arrival = int(float(row['время заезда'].split(':')[0]))
                            time_arrival = datetime(day=day_arrival, month=month_arrival, year=year_arrival,
                                                    hour=hour_arrival)
                        if row['время выезда']:
                            hour_eviction = int(float(row['время выезда'].split(':')[0]))
                            if hour_eviction == 0:
                                time_eviction = datetime(day=day_eviction, month=month_eviction, year=year_eviction,
                                                         hour=0) + timedelta(days=1)
                            else:
                                time_eviction = datetime(day=day_eviction, month=month_eviction, year=year_eviction,
                                                         hour=hour_eviction)
                        delta = time_eviction - time_arrival
                        totally_delta_hours = delta.days * 24 + delta.seconds / 3600
                        if totally_delta_hours > 0:
                            for i in range(int(totally_delta_hours) + 1):
                                dct = {'tariff': dict_from_bancho_to_gspread.get(t),
                                       'year': time_arrival.year,
                                       'month': time_arrival.month,
                                       'day': time_arrival.day,
                                       'time': time_arrival.hour}
                                time_arrival = time_arrival + timedelta(hours=1)
                                dates = dates.append(dct, ignore_index=True)
                    elif row['время заезда'] and row['время выезда']:
                        hour_arrival = int(float(row['время заезда'].split(':')[0]))
                        time_arrival = datetime(day=day_arrival, month=month_arrival, year=year_arrival,
                                                hour=hour_arrival)
                        hour_eviction = int(float(row['время выезда'].split(':')[0]))
                        if hour_eviction == 0:
                            time_eviction = datetime(day=day_arrival, month=month_arrival, year=year_arrival,
                                                     hour=hour_eviction) + timedelta(days=1)
                        else:
                            time_eviction = datetime(day=day_arrival, month=month_arrival, year=year_arrival,
                                                     hour=hour_eviction)

                        delta = time_eviction - time_arrival
                        totally_delta_hours = delta.days * 24 + delta.seconds / 3600
                        if totally_delta_hours > 0:
                            for i in range(int(totally_delta_hours) + 1):
                                dct = {'tariff': dict_from_bancho_to_gspread.get(t),
                                       'year': time_arrival.year,
                                       'month': time_arrival.month,
                                       'day': time_arrival.day,
                                       'time': time_arrival.hour}
                                time_arrival = time_arrival + timedelta(hours=1)
                                dates = dates.append(dct, ignore_index=True)
            else:
                if row['дата выезда']:
                    time_arrival = datetime(day=day_arrival, month=month_arrival, year=year_arrival, hour=14)
                    day_eviction = int(row['дата выезда'].split(',')[1].split()[0])
                    month_eviction = dict_from_rus_to_int.get(row['дата выезда'].split(',')[1].split()[1])
                    year_eviction = int(row['дата выезда'].split(',')[1].split()[2])
                    time_eviction = datetime(day=day_eviction, month=month_eviction, year=year_eviction, hour=12)
                    if row['время заезда']:
                        hour_arrival = int(float(row['время заезда'].split(':')[0]))
                        time_arrival = datetime(day=day_arrival, month=month_arrival, year=year_arrival,
                                                hour=hour_arrival)
                    if row['время выезда']:
                        hour_eviction = int(float(row['время выезда'].split(':')[0]))
                        if hour_eviction == 0:
                            time_eviction = datetime(day=day_eviction, month=month_eviction, year=year_eviction,
                                                     hour=0) + timedelta(days=1)
                        else:
                            time_eviction = datetime(day=day_eviction, month=month_eviction, year=year_eviction,
                                                     hour=hour_eviction)
                    delta = time_eviction - time_arrival
                    totally_delta_hours = delta.days * 24 + delta.seconds / 3600
                    if totally_delta_hours > 0:
                        for i in range(int(totally_delta_hours) + 1):
                            dct = {'tariff': dict_from_bancho_to_gspread.get(tariff),
                                   'year': time_arrival.year,
                                   'month': time_arrival.month,
                                   'day': time_arrival.day,
                                   'time': time_arrival.hour}
                            time_arrival = time_arrival + timedelta(hours=1)
                            dates = dates.append(dct, ignore_index=True)
                elif row['время заезда'] and row['время выезда']:
                    hour_arrival = int(float(row['время заезда'].split(':')[0]))
                    time_arrival = datetime(day=day_arrival, month=month_arrival, year=year_arrival, hour=hour_arrival)
                    hour_eviction = int(float(row['время выезда'].split(':')[0]))
                    if hour_eviction == 0:
                        time_eviction = datetime(day=day_arrival, month=month_arrival, year=year_arrival,
                                                 hour=hour_eviction) + timedelta(days=1)
                    else:
                        time_eviction = datetime(day=day_arrival, month=month_arrival, year=year_arrival,
                                                 hour=hour_eviction)

                    delta = time_eviction - time_arrival
                    totally_delta_hours = delta.days * 24 + delta.seconds / 3600
                    if totally_delta_hours > 0:
                        for i in range(int(totally_delta_hours) + 1):
                            dct = {'tariff': dict_from_bancho_to_gspread.get(tariff),
                                   'year': time_arrival.year,
                                   'month': time_arrival.month,
                                   'day': time_arrival.day,
                                   'time': time_arrival.hour}
                            time_arrival = time_arrival + timedelta(hours=1)
                            dates = dates.append(dct, ignore_index=True)
    return dates


def get_max_count_people(tariff=''):
    try:
        cur.execute(
            f"""
                SELECT max_people FROM tariffs WHERE tariff=?
                """,
            (tariff,)
        )
        result = str(*cur.fetchone())
        if result:
            return int(result)
        else:
            return False
    except Exception as e:
        logger.warning(f'exception while extracted max people from tariffs where tariff={tariff} '
                       f'exception - {e}')
        return False


def get_min_hours_book(tariff=''):
    try:
        cur.execute(
            f"""
                SELECT minimal_time FROM tariffs WHERE tariff=?
                    """,
            (tariff,)
        )
        result = str(*cur.fetchone())
        if result and result.isdigit():
            return int(result) + 1
        else:
            return False
    except Exception as e:
        logger.warning(f'exception while extracted min_hours from tariffs where tariff={tariff} '
                       f'exception - {e}')
        return False


def get_overload(tariff=''):
    try:
        cur.execute(
            f"""
                SELECT overload_people, overload_cost FROM tariffs WHERE tariff=?
                    """,
            (tariff,)
        )
        result = list(*cur.fetchall())
        if result != [None, None]:
            return result
        else:
            return False
    except Exception as e:
        logger.warning(f'exception while extracted overload from tariffs where tariff={tariff} '
                       f'exception - {e}')
        return False


def get_additionals():
    cur.execute(
        f"""
        SELECT name, cost FROM additionals
        """
    )
    return dict(cur.fetchall())


def get_phone(chat_id):
    try:
        cur.execute(
            f"""
                    SELECT phone FROM users WHERE chat_id=?
                        """,
            (chat_id,)
        )
        result = str(*cur.fetchone())
        if result and result != 'Null':
            return str(result)
        else:
            logger.info(f'no phone for {chat_id}, {result}')
            return False
    except Exception as e:
        logger.warning(f'exception while extracted phone from users where chat_id={chat_id} '
                       f'exception - {e}')
        return False


def set_phone(chat_id, phone):
    try:
        cur.execute(
            f"""
            Update users set phone='{phone}' WHERE chat_id={chat_id}
            """)
        conn.commit()
        logger.info(f'successfully set phone for {chat_id} is {phone}')
        return True
    except Exception as e:
        logger.warning(f'exception while set phone-{phone} to users where chat_id={chat_id} '
                       f'exception - {e}')
        return False


def set_name(chat_id, name):
    try:
        cur.execute(
            f"""
            Update users set name='{name}' WHERE chat_id={chat_id}
            """)
        conn.commit()
        logger.info(f'successfully set name for {chat_id} is {name}')
        return True
    except Exception as e:
        logger.warning(f'exception while set name-{name} to users where chat_id={chat_id} '
                       f'exception - {e}')
        return False


def get_deposit(tariff):
    try:
        cur.execute(
            f"""
                SELECT deposit FROM tariffs WHERE tariff=?
                """,
            (tariff,)
        )
        info = str(*cur.fetchone())
        return info
    except Exception as e:
        logger.warning(f'exception while extracted deposit from tariffs where tariff={tariff} '
                       f'exception - {e}')
        return False


def insert_order(chat_id, phone, tariff, date_arrival, date_departure, time_arrival, time_departure, quantity_of_people,
                 additionalls):
    order = [chat_id, phone, tariff, date_arrival, date_departure, time_arrival, time_departure, quantity_of_people,
             additionalls, datetime.now()]
    try:
        cur.execute(
            f'''
                    INSERT INTO 
                    orders (chat_id, phone, tariff, date_arrival, date_departure, time_arrival, time_departure,
                    quantity_of_people, additionalls, time_modified) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (chat_id, phone, tariff, date_arrival, date_departure, time_arrival, time_departure, quantity_of_people,
             additionalls, datetime.now())
        )
        conn.commit()
        return True
    except Exception as e:
        logger.warning(f'exception while inserted order to orders- {order} '
                       f'exception - {e}')
        return False


def pre_load_order(chat_id, data: FSMContextProxy, phone=''):
    type_of_rent = data.get('type_of_rent')
    tariff = data.get('tariff')
    user_dates = data.get('date')
    user_times = data.get('time')
    quantity_of_people = data.get('quantity_of_people')
    additionals = data.get('additionals')
    chat_id = chat_id
    if not phone:
        phone = get_phone(chat_id)
    date_arrival = datetime(day=1, month=1, year=2030)
    date_departure = datetime(day=1, month=1, year=2000)
    try:
        if user_dates:
            for dt in user_dates:
                date_arrival = min(datetime(day=int(dt[0]), month=int(dt[1]), year=int(dt[2])), date_arrival)
                date_departure = max(datetime(day=int(dt[0]), month=int(dt[1]), year=int(dt[2])), date_departure)
            time_arrival = '14.00'
            time_departure = '12.00'
            if type_of_rent == 'pochas' and user_times:
                time_arrival = '01.00'
                time_departure = '09.00'
                times = ['09.00', '10.00', '11.00', '12.00', '13.00', '14.00', '15.00', '16.00', '17.00', '18.00',
                         '19.00',
                         '20.00', '21.00', '22.00', '23.00', '00.00', '01.00']
                for time in user_times:
                    time_arrival = times[min(times.index(time_arrival), times.index(time))]
                    time_departure = times[max(times.index(time_departure), times.index(time))]
            elif type_of_rent == 'pochas' and not user_times:
                logger.info(f'no user_times for {chat_id}, data-{data}')
                return False
            elif type_of_rent == 'posut':
                date_departure = date_departure + timedelta(days=1)
            if additionals is None:
                additionalls = ''
            else:
                additionalls = '; '.join(additionals)
            if not quantity_of_people:
                quantity_of_people = 1
            list_to_sql = [chat_id, phone, tariff, date_arrival, date_departure, time_arrival, time_departure,
                    quantity_of_people, additionalls]
            logger.info(f'successfully pre_loaded data {chat_id}, list_to_sql-{list_to_sql}')
            return list_to_sql
        logger.warning(f'no user_dates for {chat_id}, data-{data}')
        return False
    except Exception as e:
        logger.warning(f'exception while pre_loaded order data-{data}'
                       f'exception - {e}')
        return False


def get_admins_list() -> list:
    cur.execute(
        f"""
        SELECT chat_id from admins
        """
    )
    return list(chain(*cur.fetchall()))


def get_name(chat_id):
    info = cur.execute('SELECT name FROM users WHERE chat_id=?', (chat_id,))
    name = info.fetchone()
    if name == (None,) or name is None:
        logger.info(f'no name for {chat_id}, name={name}')
        return False
    else:
        return str(*name)


def get_cost(tariff):
    info = cur.execute('SELECT cost FROM tariffs WHERE tariff=?', (tariff,))
    name = info.fetchone()
    if name == (None,) or name is None:
        logger.warning(f'no cost for {tariff}, cost={name}')
        return False
    else:
        return int(str(*name))


def get_prefix_additionals(name):
    info = cur.execute('SELECT prefix FROM additionals WHERE name=?', (name,))
    name = info.fetchone()
    if name == (None,) or name is None:
        return False
    else:
        return str(*name)


def get_type_of_rent_from_tariff(tariff):
    info = cur.execute('SELECT type_of_rent FROM tariffs WHERE tariff=?', (tariff,))
    name = info.fetchone()
    if name == (None,) or name is None:
        return False
    else:
        return str(*name)


def get_additionals_cost(ad):
    info = cur.execute('SELECT cost FROM additionals WHERE name=?', (ad,))
    name = info.fetchone()
    if name == (None,) or name is None:
        return False
    else:
        return int(str(*name))


def pre_load_to_insert_into_gspread(chat_id, phone, tariff, date_arrival: datetime, date_departure: datetime,
                                    time_arrival_str, time_departure_str, quantity_of_people, additionalls, name=''):
    if not name:
        name = get_name(chat_id)
    time_arrival = datetime(day=date_arrival.day, month=date_arrival.month, year=date_arrival.year,
                            hour=int(float(time_arrival_str)))
    time_departure = datetime(day=date_departure.day, month=date_departure.month, year=date_departure.year,
                              hour=int(float(time_departure_str)))
    cost = get_cost(tariff)
    ads_str = ''
    total_ads = 0
    type_of_rent = get_type_of_rent_from_tariff(tariff)
    for i in additionalls:
        total_ads += get_additionals_cost(i)
        if get_prefix_additionals(i):
            ads_str += get_prefix_additionals(i)
    text_data_arrival = f'{weekdays_dict.get(date_arrival.weekday())}, {date_arrival.day} {dict_from_int_to_rus.get(date_arrival.month)} {date_arrival.year}'
    text_data_eviction = ''
    total_days = ''
    total_hours = ''
    if get_deposit(tariff) and get_deposit(tariff) != 'None':
        book_sum = get_deposit(tariff)
    else:
        book_sum = 0
    if type_of_rent == 'posut':
        if date_departure == date_arrival:
            total_days = 1
            date_departure = date_departure + timedelta(days=1)
        else:
            total_days = (
                    datetime(day=date_departure.day, month=date_departure.month, year=date_departure.year, hour=23,
                             minute=59) - date_arrival).days
        text_data_eviction = f'{weekdays_dict.get(date_departure.weekday())}, {date_departure.day} {dict_from_int_to_rus.get(date_departure.month)} {date_departure.year}'
        cost = cost * total_days
    else:
        total_hours = (time_departure - time_arrival).seconds / 3600
        cost = cost * total_hours
    status = 'Внесли предоплату'
    if get_overload(tariff):
        overload_people, overload_cost = get_overload(tariff)
        if overload_people < quantity_of_people:
            if type_of_rent == 'pochas':
                ads_str += f'л={quantity_of_people - overload_people}={(quantity_of_people - overload_people) * overload_cost * int(total_hours)}'
                total_ads += (quantity_of_people - overload_people) * overload_cost * int(total_hours)
            else:
                ads_str += f'л={quantity_of_people - overload_people}={(quantity_of_people - overload_people) * overload_cost * int(total_days)}'
                total_ads += (quantity_of_people - overload_people) * overload_cost * int(total_days)

    values_list = {'ФИО': name,
                   'телефон': phone,
                   'тариф': dict_from_gspread_to_bancho.get(tariff),
                   'дата заезда': text_data_arrival,
                   'Месяц': date_arrival.month,
                   'дата выезда': text_data_eviction,
                   'итого суток': total_days,
                   'время заезда': time_arrival.strftime('%H:%M'),
                   'время выезда': time_departure.strftime('%H:%M'),
                   'итого часов': str(total_hours),
                   'сумма брони': book_sum,
                   'сумма долга': str(cost - int(book_sum)),
                   'уборка оплачена': '',
                   'ИТОГО': str(cost + int(total_ads)),
                   'человек': quantity_of_people,
                   'допы (в,к,л,чаша, д,бк,чай, мж,мс,х,п,)': ads_str,
                   'общая сумма допов ': total_ads,
                   'Доп.услуга': ' '.join(additionalls),
                   'Кол-во': '',
                   'Способ оплаты': '',
                   'Статус': status,
                   'комментарии': '',
                   'test_column': ''
                   }
    logger.warning(f'pre_load_to_insert_into_gspread for {chat_id} is {values_list}')
    return list(values_list.values())


def get_last_id_gspread():
    lst = worksheet.col_values(col=1)
    return len(lst) + 1


def insert_into_gspread(values: list):
    id = get_last_id_gspread()
    try:
        worksheet.insert_row(values, index=id, value_input_option='RAW')
        return True
    except Exception as e:
        logger.warning(f'exception for {values} is {e}')
        return False


def get_orders_from_gspread_by_phone_number(phone):
    list_of_cells = worksheet.findall(query=phone, in_column=2)
    list_of_ids = []
    row_dict = {}
    for cell in list_of_cells:
        list_of_ids.append(cell.row)
    for id in list_of_ids:
        row = worksheet.row_values(id)
        if row and row[20] != 'Отказ':
            try:
                day_arrival = int(row[3].split(',')[1].split()[0])
                month_arrival = dict_from_rus_to_int.get(row[3].split(',')[1].split()[1])
                year_arrival = int(row[3].split(',')[1].split()[2])
                if datetime.now() < datetime(day=day_arrival, month=month_arrival, year=year_arrival):
                    if not row[7] and not row[8]:
                        row[7] = '14:00'
                        row[8] = '12:00'
                    else:
                        row[7] = ':'.join([row[7].split(':')[0], row[7].split(':')[1]])
                        row[8] = ':'.join([row[8].split(':')[0], row[8].split(':')[1]])
                    row_dict[
                        id] = f"""тариф - {row[2]}, дата и время заезда - {row[3]} {row[7]}\n, дата выезда - {row[5]} {row[8]}"""
            except Exception as e:
                logger.warning(f'exception for {row} is {e}')
                continue
    return row_dict


def delete_order_by_row_id(row_id):
    try:
        worksheet.update(f'U{row_id}', 'Отказ')
        return True
    except Exception as e:
        logger.warning(f'exception for {row_id} is {e}')
        return False


def delete_from_dates(dates: DataFrame, row_id) -> DataFrame:
    print(worksheet.row_values(row_id))
    values_list = worksheet.row_values(row_id)
    row = {
        'тариф': values_list[2],
        'дата заезда': values_list[3],
        'дата выезда': values_list[5],
        'время заезда': values_list[7],
        'время выезда': values_list[8],
        'Способ оплаты': values_list[19],
        'Статус': values_list[20],
    }
    if row['дата заезда'] and row['дата заезда'] != 'дата заезда' and row['Статус'] == 'Отказ':
        day_arrival = int(row['дата заезда'].split(',')[1].split()[0])
        month_arrival = dict_from_rus_to_int.get(row['дата заезда'].split(',')[1].split()[1])
        year_arrival = int(row['дата заезда'].split(',')[1].split()[2])
        tariff = row['тариф']
        if tariff == 'Весь комплекс':
            for t in ['ДаЧО', 'БанЧО', 'ДОМиЧО', 'БанЧО +', 'БассиЧо', 'ДомБассиЧо', 'ДомБаниЧо']:
                if row['дата выезда']:
                    time_arrival = datetime(day=day_arrival, month=month_arrival, year=year_arrival, hour=14)
                    day_eviction = int(row['дата выезда'].split(',')[1].split()[0])
                    month_eviction = dict_from_rus_to_int.get(row['дата выезда'].split(',')[1].split()[1])
                    year_eviction = int(row['дата выезда'].split(',')[1].split()[2])
                    time_eviction = datetime(day=day_eviction, month=month_eviction, year=year_eviction, hour=12)
                    if row['время заезда']:
                        hour_arrival = int(float(row['время заезда'].split(':')[0]))
                        time_arrival = datetime(day=day_arrival, month=month_arrival, year=year_arrival,
                                                hour=hour_arrival)
                    if row['время выезда']:
                        hour_eviction = int(float(row['время выезда'].split(':')[0]))
                        if hour_eviction == 0:
                            time_eviction = datetime(day=day_eviction, month=month_eviction, year=year_eviction,
                                                     hour=0) + timedelta(days=1)
                        else:
                            time_eviction = datetime(day=day_eviction, month=month_eviction, year=year_eviction,
                                                     hour=hour_eviction)
                    delta = time_eviction - time_arrival
                    totally_delta_hours = delta.days * 24 + delta.seconds / 3600
                    if totally_delta_hours > 0:
                        for i in range(int(totally_delta_hours) + 1):
                            index = dates[(dates.tariff == dict_from_bancho_to_gspread.get(t)) &
                                          (dates.year == time_arrival.year) &
                                          (dates.month == time_arrival.month) &
                                          (dates.day == time_arrival.day) &
                                          (dates.time == time_arrival.hour)].index.tolist()
                            dates.drop(labels=index, axis=0, inplace=True)
                            time_arrival = time_arrival + timedelta(hours=1)
                elif row['время заезда'] and row['время выезда']:
                    hour_arrival = int(float(row['время заезда'].split(':')[0]))
                    time_arrival = datetime(day=day_arrival, month=month_arrival, year=year_arrival,
                                            hour=hour_arrival)
                    hour_eviction = int(float(row['время выезда'].split(':')[0]))
                    if hour_eviction == 0:
                        time_eviction = datetime(day=day_arrival, month=month_arrival, year=year_arrival,
                                                 hour=hour_eviction) + timedelta(days=1)
                    else:
                        time_eviction = datetime(day=day_arrival, month=month_arrival, year=year_arrival,
                                                 hour=hour_eviction)

                    delta = time_eviction - time_arrival
                    totally_delta_hours = delta.days * 24 + delta.seconds / 3600
                    if totally_delta_hours > 0:
                        for i in range(int(totally_delta_hours) + 1):
                            index = dates[(dates.tariff == dict_from_bancho_to_gspread.get(t)) &
                                          (dates.year == time_arrival.year) &
                                          (dates.month == time_arrival.month) &
                                          (dates.day == time_arrival.day) &
                                          (dates.time == time_arrival.hour)].index.tolist()
                            dates.drop(labels=index, axis=0, inplace=True)
                            time_arrival = time_arrival + timedelta(hours=1)
        else:
            if row['дата выезда']:
                time_arrival = datetime(day=day_arrival, month=month_arrival, year=year_arrival, hour=14)
                day_eviction = int(row['дата выезда'].split(',')[1].split()[0])
                month_eviction = dict_from_rus_to_int.get(row['дата выезда'].split(',')[1].split()[1])
                year_eviction = int(row['дата выезда'].split(',')[1].split()[2])
                time_eviction = datetime(day=day_eviction, month=month_eviction, year=year_eviction, hour=12)
                if row['время заезда']:
                    hour_arrival = int(float(row['время заезда'].split(':')[0]))
                    time_arrival = datetime(day=day_arrival, month=month_arrival, year=year_arrival,
                                            hour=hour_arrival)
                if row['время выезда']:
                    hour_eviction = int(float(row['время выезда'].split(':')[0]))
                    if hour_eviction == 0:
                        time_eviction = datetime(day=day_eviction, month=month_eviction, year=year_eviction,
                                                 hour=0) + timedelta(days=1)
                    else:
                        time_eviction = datetime(day=day_eviction, month=month_eviction, year=year_eviction,
                                                 hour=hour_eviction)
                delta = time_eviction - time_arrival
                totally_delta_hours = delta.days * 24 + delta.seconds / 3600
                if totally_delta_hours > 0:
                    for i in range(int(totally_delta_hours) + 1):
                        index = dates[(dates.tariff == dict_from_bancho_to_gspread.get(tariff)) &
                                      (dates.year == time_arrival.year) &
                                      (dates.month == time_arrival.month) &
                                      (dates.day == time_arrival.day) &
                                      (dates.time == time_arrival.hour)].index.tolist()
                        dates.drop(labels=index, axis=0, inplace=True)
                        time_arrival = time_arrival + timedelta(hours=1)
            elif row['время заезда'] and row['время выезда']:
                hour_arrival = int(float(row['время заезда'].split(':')[0]))
                time_arrival = datetime(day=day_arrival, month=month_arrival, year=year_arrival, hour=hour_arrival)
                hour_eviction = int(float(row['время выезда'].split(':')[0]))
                if hour_eviction == 0:
                    time_eviction = datetime(day=day_arrival, month=month_arrival, year=year_arrival,
                                             hour=hour_eviction) + timedelta(days=1)
                else:
                    time_eviction = datetime(day=day_arrival, month=month_arrival, year=year_arrival,
                                             hour=hour_eviction)

                delta = time_eviction - time_arrival
                totally_delta_hours = delta.days * 24 + delta.seconds / 3600
                if totally_delta_hours > 0:
                    for i in range(int(totally_delta_hours) + 1):
                        index = dates[(dates.tariff == dict_from_bancho_to_gspread.get(tariff)) &
                                      (dates.year == time_arrival.year) &
                                      (dates.month == time_arrival.month) &
                                      (dates.day == time_arrival.day) &
                                      (dates.time == time_arrival.hour)].index.tolist()
                        dates.drop(labels=index, axis=0, inplace=True)
                        time_arrival = time_arrival + timedelta(hours=1)
    return dates


def append_to_dates(dates: DataFrame, chat_id: int, phone, tariff: str, date_arrival: datetime,
                    date_departure: datetime,
                    time_arrival: str, time_departure: str, quantity_of_people: int, additionalls) -> DataFrame:
    day_arrival = date_arrival.day
    month_arrival = date_arrival.month
    year_arrival = date_arrival.year
    hour_arrival = int(time_arrival.split('.')[0])
    datetime_arrival = datetime(day=day_arrival, month=month_arrival, year=year_arrival, hour=hour_arrival)

    day_eviction = date_departure.day
    month_eviction = date_departure.month
    year_eviction = date_departure.year
    hour_eviction = int(time_departure.split('.')[0])
    if hour_eviction == 0:
        datetime_eviction = datetime(day=day_eviction, month=month_eviction, year=year_eviction,
                                     hour=hour_eviction) + timedelta(days=1)
    else:
        datetime_eviction = datetime(day=day_eviction, month=month_eviction, year=year_eviction, hour=hour_eviction)
    delta_hours = (datetime_eviction - datetime_arrival).days * 24 + (
            datetime_eviction - datetime_arrival).seconds / 3600
    if tariff == 'Весь комплекс':
        for t in ['ДаЧО', 'БанЧО', 'ДОМиЧО', 'БанЧО +', 'БассиЧо', 'ДомБассиЧо', 'ДомБаниЧо']:
            time_arrival = datetime(day=day_arrival, month=month_arrival, year=year_arrival, hour=hour_arrival)
            for i in range(int(delta_hours) + 1):
                dct = {'tariff': t,
                       'year': time_arrival.year,
                       'month': time_arrival.month,
                       'day': time_arrival.day,
                       'time': time_arrival.hour}
                time_arrival = time_arrival + timedelta(hours=1)
                dates = dates.append(dct, ignore_index=True)
    else:
        time_arrival = datetime(day=day_arrival, month=month_arrival, year=year_arrival, hour=hour_arrival)
        for i in range(int(delta_hours) + 1):
            dct = {'tariff': tariff,
                   'year': time_arrival.year,
                   'month': time_arrival.month,
                   'day': time_arrival.day,
                   'time': time_arrival.hour}
            time_arrival = time_arrival + timedelta(hours=1)
            dates = dates.append(dct, ignore_index=True)
    return dates
