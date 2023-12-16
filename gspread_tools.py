import gspread
from pandas import DataFrame
from datetime import datetime, timedelta

gc = gspread.service_account(filename='credentials/gspread.json')
sh = gc.open_by_key('SHEET_KEY')
worksheet = sh.worksheet("Суточные и часовые объекты")
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

dict_from_bancho_to_gspread = {'ДаЧО': 'dacho',
                               'БанЧО': 'bancho',
                               'ДОМиЧО': 'domicho',
                               'БанЧО +': 'bancho +',
                               'БассиЧо': 'basicho',
                               'ДомБассиЧо': 'dombasicho',
                               'ДомБаниЧо': 'dombanicho',
                               '10 мкр': '10',
                               '5 мкр': '5',
                               '1 мкр': '1'}

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
    if row['дата заезда'] and row['дата заезда'] != 'дата заезда':
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

print(dates[(dates.tariff == '10') &
            (dates.year == 2022) &
            (dates.month == 8)],
      len(dates[(dates.tariff == '10') &
                (dates.year == 2022) &
                (dates.month == 8)]))
