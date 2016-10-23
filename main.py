# encoding: utf8

import json
import cjson
import math

import os.path
from hashlib import sha1
from collections import namedtuple, defaultdict, Counter
from datetime import datetime

import requests as requests_
requests_.packages.urllib3.disable_warnings()

import numpy as np
import pandas as pd

import seaborn as sns
import matplotlib as mpl
from matplotlib import pyplot as plt
from matplotlib import rc
import matplotlib.ticker as mtick
# For cyrillic labels
rc('font', family='Verdana', weight='normal')


DATA_DIR = 'data'
JSON_DIR = os.path.join(DATA_DIR, 'json')
JSON_LIST = os.path.join(JSON_DIR, 'list.txt')
BEGTIN = os.path.join(DATA_DIR, 'crash.json')
BEGTIN_LINES = 145604

IZBIRKOM_DIR = os.path.join(DATA_DIR, 'izbirkom')
UIKS = os.path.join(IZBIRKOM_DIR, 'uiks.tsv')
PARTY_RESULTS = os.path.join(IZBIRKOM_DIR, 'party_results.tsv')
GIBDD_DIR = os.path.join(DATA_DIR, 'gibdd')
GIBDD_REGIONS = os.path.join(GIBDD_DIR, 'regions.tsv')
GIBDD_CARDS = os.path.join(GIBDD_DIR, 'cards.tsv')

SAFEROADS_DIR = os.path.join(DATA_DIR, 'saferoads')
SAFEROADS = os.path.join(SAFEROADS_DIR, 'saferoads.jsonl')

USER_AGENT = 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36'

GIBDD_RUSSIA_ID = 877

OTHER = u'Другое'
TYPES = [
    u'Столкновение',
    u'Наезд на пешехода',
    u'Опрокидывание',
    u'Наезд на препятствие',
    u'Съезд с дороги',
    u'Наезд на велосипедиста',
    u'Падение пассажира',
    u'Наезд на стоящее ТС',
    u'Наезд на животное',
    u'Отбрасывание предмета',
    u'Наезд на лицо, не являющееся участником дорожного движения, осуществляющее производство работ',
    u'Наезд на лицо, не являющееся участником дорожного движения, осуществляющее несение службы',
    u'Наезд на лицо, не являющееся участником дорожного движения, осуществляющее какую-либо другую деятельность',
    u'Наезд на гужевой транспорт',
    u'Наезд на внезапно возникшее препятствие',
    u'Падение груза',
]
TOP_TYPES = [
    u'Столкновение',
    u'Наезд на пешехода',
    u'Опрокидывание',
    u'Наезд на препятствие',
    u'Съезд с дороги',
    u'Наезд на велосипедиста',
    u'Падение пассажира',
    u'Наезд на стоящее ТС',
    OTHER
]
OTHER_TYPE = u'Иной вид ДТП'

MILLIONS = {
    u'г. Москва',
    u'г. Санкт-Петербург',
    u'г.Новосибирск',
    u'г.Екатеринбург',
    u'г. Нижний новгород',
    u'г. Казань',
    u'г.Челябинск',
    u'г. Омск',
    u'г. Самара',
    u'г.Ростов-на-Дону',
    u'г.Уфа',
    u'г.Красноярск',
    u'Пермь',
    u'г. Воронеж',
    u'Волгоград',
}

MONTH_LABELS = {
    'Jan': u'Янв',
    'Feb': u'Фев',
    'Mar': u'Мар',
    'Apr': u'Апр',
    'May': u'Май',
    'Jun': u'Июн',
    'Jul': u'Июл',
    'Aug': u'Авг',
    'Sep': u'Сен',
    'Oct': u'Окт',
    'Nov': u'Ноя',
    'Dec': u'Дек',
}

UNKNOWN_GENDER = u'не определено'

TOP_PLACES = [
    u'Нерегулируемый перекрёсток неравнозначных улиц (дорог)',
    u'Нерегулируемый пешеходный переход',
    u'Регулируемый перекресток',
    u'Выезд с прилегающей территории',
    u'Регулируемый пешеходный переход',
    u'Иное место',
    u'Внутридворовая территория',
    u'Остановка общественного транспорта',
    u'Мост, эстакада, путепровод',
    u'Нерегулируемый перекрёсток равнозначных улиц (дорог)',
    OTHER
]
UNKNOWN_PLACE = u'Перегон (нет объектов на месте ДТП)'

TOP_PROBLEMS = [
    u'Отсутствие, плохая различимость горизонтальной разметки проезжей части',
    u'Отсутствие дорожных знаков в необходимых местах',
    u'Недостатки зимнего содержания',
    u'Неправильное применение, плохая видимость дорожных знаков',
    u'Отсутствие пешеходных ограждений в необходимых местах',
    u'Дефекты покрытия',
    u'Отсутствие освещения',
    u'Неудовлетворительное состояние обочин',
    u'Отсутствие тротуаров (пешеходных дорожек)',
    u'Нарушения в размещении наружной рекламы',
    OTHER
]

DRIVER = u'Водитель'
TOP_DRIVER_REASONS = [
    u'Другие нарушения ПДД водителем',
    u'Несоблюдение очередности проезда',
    u'Несоответствие скорости конкретным условиям движения',
    u'Нарушение правил проезда пешеходного перехода',
    u'Неправильный выбор дистанции',
    u'Нарушение правил расположения ТС на проезжей части',
    u'Выезд на полосу встречного движения',
    u'Превышение установленной скорости движения',
    u'Несоблюдение условий, разрешающих движение транспорта задним ходом',
    u'Нарушение правил перестроения',
    OTHER
]

PEDESTRIAN = u'Пешеход'
TOP_PEDESTRIAN_REASONS = [
    u'Переход через проезжую часть вне пешеходного перехода в зоне его видимости либо при наличии в непосредственной близости подземного (надземного) пешеходного перехода',
    u'Переход через проезжую часть в неустановленном месте (при наличии в зоне видимости перекрёстка)',
    u'Иные нарушения',
    u'Нахождение на проезжей части без цели её перехода',
    u'Неподчинение сигналам регулирования',
    u'Ходьба вдоль проезжей части попутного направления вне населенного пункта при удовлетворительном состоянии обочины',
    u'Неожиданный выход из-за стоящего ТС',
    u'Ходьба вдоль проезжей части при наличии и удовлетворительном состоянии тротуара',
    u'Неожиданный выход из-за ТС',
    u'Переход проезжей части в запрещённом месте (оборудованном пешеходными  ограждениями)',
    OTHER
]

PASSENGER = u'Пассажир'
TOP_PASSENGER_REASONS = [
    u'Иные нарушения',
    u'Нарушение правил пользования общественным транспортом',
    u'Оставление движущегося транспортного средства (выход или выпрыгивание на ходу и т.д.)',
    u'Создание помех для водителя в управлении транспортным средством',
    OTHER
]

BICYCLE = u'Велосипедист'
TOP_BICYCLE_REASONS = [
    u'Несоблюдение очередности проезда',
    u'Другие нарушения ПДД водителем',
    u'Пересечение велосипедистом проезжей части по пешеходному переходу',
    u'Нарушение правил расположения ТС на проезжей части',
    u'Нарушение правил перестроения',
    u'Нарушение требований сигналов светофора',
    u'Выезд на полосу встречного движения',
    u'Управление велосипедом, не оснащённым светоотражающими элементами',
    u'Несоблюдение бокового интервала',
    u'Неподача или неправильная подача сигналов',
    OTHER
]

TOP_PARTS = [
    u'Передняя часть',
    u'Передняя левая часть',
    u'Левая сторона',
    u'Передняя правая часть',
    u'Задняя часть',
    u'Задняя правая часть',
    u'Задняя левая часть',
    u'Крыша',
    u'Полная деформация кузова',
    u'Правая сторона',
    OTHER
]

TOP_VEHICLE_TYPES = [
    u'Легковые автомобили В-класса (малый) до 3,9 м',
    u'Легковые автомобили С-класса (малый средний, компактный) до 4,3 м',
    u'Легковые автомобили',
    u'Легковые автомобили D-класса (средний) до 4,6 м',
    u'Седельные тягачи',
    u'Минивэны и универсалы повышенной вместимости',
    u'Фургоны',
    u'Прочие легковые автомобили',
    u'Бортовые грузовые автомобили',
    u'Мотоциклы',
    u'Легковые автомобили А-класса (особо малый) до 3,5 м',
    u'Самосвалы',
    u'Грузовые автомобили',
    u'Легковые автомобили Е-класса (высший средний, бизнес-класс) до 4,9 м',
    OTHER
]

TOP_PRIVODS = [
    u'Передний (левый руль)',
    u'Задний (левый руль)',
    u'Полноприводный (левый руль)',
    u'Передний (правый руль)',
    u'Полноприводный (правый руль)',
    u'Задний (правый руль)',
    OTHER
]

TOP_TYRES = [
    u'Летние',
    u'Всесезонные',
    u'Зимние шипованные',
    u'Зимние',
    OTHER
]
UNKNOWN_TYRES = u'Не заполнено'


Coordinates = namedtuple(
    'Coordinates',
    ['latitude', 'longitude']
)
Point = namedtuple(
    'Point',
    ['x', 'y', 'value']
)

# FROM_MIA

# vehicles                      67666
# road_type_name                67666
# region_code                   67666
# region_name                   67666
# road_significance_name        67666
# suffer_amount                 67666
# road_significance_code        67666
# road_drawbacks                67666
# road_loc                      67666
# road_code                     67666
# transp_amount                 67666
# tr_area_state_name            67666
# tr_area_state_code            67666
# there_road_constructions      67666
# road_type_code                67666
# road_loc_m                    67666
# suffer_child_amount           67666
# type                          67666
# road_name                     67666
# address                       67666
# place_id                      67666
# author                        67666
# created_at                    67666
# datetime                      67666
# em_moment_date                67666
# em_moment_time                67666
# em_place_latitude             67666
# em_place_longitude            67666
# em_type_code                  67666
# em_type_name                  67666
# from_mia                      67666
# geo_code                      67666
# here_road_constructions       67666
# hidden_by_okato               67666
# place_path                    67666
# infractions                   67666
# light_type_code               67666
# light_type_name               67666
# loss_amount                   67666
# loss_child_amount             67666
# mark                          67666
# motion_influences             67666
# mt_rate_code                  67666
# mt_rate_name                  67666
# num_of_fatalities             67666
# num_of_vehicle                67666
# num_of_victim                 67666
# okato_code                    67666
# participants                  67666
# is_hidden                     67666
# num_of_party                  67666

# NOT FROM_MIA

# address              77938
# author               77938
# created_at           77938
# datetime             77938
# from_mia             77938
# geo_code             77938
# hidden_by_okato      77938
# infractions          77938
# is_hidden            77938
# mark                 77938
# num_of_fatalities    77938
# num_of_party         77938
# num_of_vehicle       77938
# num_of_victim        77938
# place_id             77938
# type                 77938

# PERSON

# person_infos                   67258
# attendant_pdd_derangements    123303
# driver_service_length         123303
# hv_type_code                  123303
# hv_type_name                  123303
# legal_opinions                123303
# main_pdd_derangements         123303
# part_type_code                123303
# part_type_name                123303
# person_birthday               123303
# person_sex                    123303
# person_sex_name               123303
# person_sort                   123303
# vl_sort                       123303

# VEHICLES

# optional_equipments     42533
# damage_dispositions    113985
# okfs_code              113985
# okfs_name              113985
# prod_type_code         113985
# prod_type_name         113985
# rudder_type_code       113985
# rudder_type_name       113985
# technical_failures     113985
# tyre_type_code         113985
# tyre_type_name         113985
# vl_sort                113985
# vl_year                113985


SaferoadsParticipants = namedtuple(
    'SaferoadsParticipants',
    ['type', 'birthdate', 'gender', 'experience']
)
SaferoadsVehicle = namedtuple(
    'SaferoadsVehicle',
    ['parts', 'type', 'privod', 'failures', 'tyres', 'year']
)
SaferoadsRoad = namedtuple(
    'SaferoadsRoad',
    ['drawbacks', 'light']
)
SaferoadsRecord = namedtuple(
    'SaferoadsRecord',
    ['type', 'timestamp', 'coordinates',
     'participants_count', 'vehicles_count', 'victims', 'fatalities',
     'participants', 'vehicles', 'road', 'mia']
)
Uik = namedtuple(
    'Uik',
    ['id', 'name', 'region_id', 'region_name', 'oik',
     'tik_id', 'tik_name', 'address', 'coordinates']
)
ResultsCell = namedtuple(
    'ResultsCell',
    ['uik_id', 'row_id', 'value']
)


class GibddRequest(object):
    def __init__(self, url, payload):
        self.url = url
        self.payload = payload
        
    @property
    def data(self):
        return gibdd_dumps(self.payload)

    @property
    def key(self):
        return '{url}#{payload}'.format(
            url=self.url,
            payload=self.data
        )
    
    def __repr__(self):
        return 'GibddRequest(url={url!r}, payload={payload!r})'.format(
            url=self.url,
            payload=self.payload
        )


class GibddCardsRequest(GibddRequest):
    def __init__(self, url, payload, region):
        super(GibddCardsRequest, self).__init__(url, payload)
        self.region = region


GibddRegion = namedtuple(
    'GibddRegion',
    ['level', 'parent_id', 'id', 'name']
)


# {
#   "District": "Филевский парк", 
#   "K_UCH": 2, 
#   "RAN": 1, 
#   "KartId": 199891433, 
#   "infoDtp": {
#     "house": "6А", 
#     "n_p": "г Москва", 
#     "m": "", 
#     "km": "", 
#     "street": "проезд Багратионовский", 
#     "sdor": [
#       "Выезд с прилегающей территории"
#     ], 
#     "dor": "", 
#     "ndu": [
#       "Неправильное применение, плохая видимость дорожных знаков"
#     ], 
#     "npdd": [
#       {
#         "ArrN": [
#           "Несоблюдение очередности проезда"
#         ], 
#         "name": "Водитель"
#       }
#     ]
#   }, 
#   "DTP_V": "Наезд на пешехода", 
#   "Time": "09:10", 
#   "date": "28.09.2016", 
#   "POG": 0, 
#   "rowNum": 1, 
#   "K_TS": 1
# }

GibddAddress = namedtuple(
    'GibddAddress',
    ['city', 'street', 'house', 'road', 'km', 'm', 'where']
)
GibddWhy = namedtuple(
    'GibddWhy',
    ['who', 'text']
)
GibddCard = namedtuple(
    'GibddCard',
    ['id', 'region_id', 'timestamp', 'type',
     'participants', 'vehicles', 'victims', 'fatalities',
     'address', 'problems', 'why']
)


def log_progress(sequence, every=None, size=None):
    from ipywidgets import IntProgress, HTML, VBox
    from IPython.display import display

    is_iterator = False
    if size is None:
        try:
            size = len(sequence)
        except TypeError:
            is_iterator = True
    if size is not None:
        if every is None:
            if size <= 200:
                every = 1
            else:
                every = size / 200     # every 0.5%
    else:
        assert every is not None, 'sequence is iterator, set every'

    if is_iterator:
        progress = IntProgress(min=0, max=1, value=1)
        progress.bar_style = 'info'
    else:
        progress = IntProgress(min=0, max=size, value=0)
    label = HTML()
    box = VBox(children=[label, progress])
    display(box)

    index = 0
    try:
        for index, record in enumerate(sequence, 1):
            if index == 1 or index % every == 0:
                if is_iterator:
                    label.value = '{index} / ?'.format(index=index)
                else:
                    progress.value = index
                    label.value = u'{index} / {size}'.format(
                        index=index,
                        size=size
                    )
            yield record
    except:
        progress.bar_style = 'danger'
        raise
    else:
        progress.bar_style = 'success'
        progress.value = index
        label.value = str(index or '?')


def jobs_manager():
    from IPython.lib.backgroundjobs import BackgroundJobManager
    from IPython.core.magic import register_line_magic
    from IPython import get_ipython
    
    jobs = BackgroundJobManager()

    @register_line_magic
    def job(line):
        ip = get_ipython()
        jobs.new(line, ip.user_global_ns)

    return jobs


def kill_thread(thread):
    import ctypes
    
    id = thread.ident
    code = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_long(id),
        ctypes.py_object(SystemError)
    )
    if code == 0:
        raise ValueError('invalid thread id')
    elif code != 1:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_long(id),
            ctypes.c_long(0)
        )
        raise SystemError('PyThreadState_SetAsyncExc failed')


def get_chunks(sequence, count):
    count = min(count, len(sequence))
    chunks = [[] for _ in range(count)]
    for index, item in enumerate(sequence):
        chunks[index % count].append(item) 
    return chunks


def hash_item(item):
    return sha1(item.encode('utf8')).hexdigest()


hash_url = hash_item


def get_json_filename(url):
    return '{hash}.json'.format(
        hash=hash_url(url)
    )


def get_json_path(url):
    return os.path.join(
        JSON_DIR,
        get_json_filename(url)
    )


def load_items_cache(path):
    with open(path) as file:
        for line in file:
            line = line.decode('utf8').rstrip('\n')
            if '\t' in line:
                # several lines in cache got currepted
                hash, item = line.split('\t', 1)
                yield item


def list_json_cache():
    return load_items_cache(JSON_LIST)


def update_items_cache(item, path):
    with open(path, 'a') as file:
        hash = hash_item(item)
        file.write('{hash}\t{item}\n'.format(
            hash=hash,
            item=item.encode('utf8')
        ))
        

def update_json_cache(url):
    update_items_cache(url, JSON_LIST)


def dump_json(path, data):
    with open(path, 'w') as file:
        file.write(cjson.encode(data))


def load_raw_json(path):
    with open(path) as file:
        return cjson.decode(file.read())


def download_json(url):
    response = requests_.get(
        url,
        headers={
            'User-Agent': USER_AGENT
        },
    )
    try:
        return response.json()
    except ValueError:
        return


def fetch_json(url):
    path = get_json_path(url)
    data = download_json(url)
    dump_json(path, data)
    update_json_cache(url)


def fetch_jsons(urls):
    for url in urls:
        fetch_json(url)


def load_json(url):
    path = get_json_path(url)
    return load_raw_json(path)


def get_saferoads_page_url(offset, limit):
    return u'http://xn--80abhddbmm5bieahtk5n.xn--p1ai/api/v1/crash?sort=dateDesc&limit={limit}&offset={offset}'.format(
        limit=limit,
        offset=offset
    )


def parse_datetime(timestamp):
    return datetime.fromtimestamp(timestamp / 100000)


def maybe_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

    
def read_begtin():
    with open(BEGTIN) as file:
        for line in file:
            line = line.decode('utf8')
            data = cjson.decode(line)
            yield data


def read_saferoads(urls):
    for url in urls:
        data = load_json(url)
        for record in data['items']:
            yield record


def parse_saferoads_vehicles(data):
    # sometimes data is dict, ignore
    if data and isinstance(data, list):
        for record in data:
            parts = [_['disp_name'] for _ in record['damage_dispositions']]
            type = record['prod_type_name']
            privod = record['rudder_type_name']
            failures = [_['fail_type_name'] for _ in record['technical_failures']]
            tyres = record['tyre_type_name'] or None

            year = record['vl_year']
            year = int(year) if year else None
            if year == 0 or year > 2016:
                year = None

            yield SaferoadsVehicle(parts, type, privod, failures, tyres, year)


def parse_saferoads_birthdate(date):
    try:
        date =  datetime.strptime(date, '%d.%m.%Y')
        if date.year > 1900:
            return date
    except ValueError:
        return


def parse_saferoads_participants(data):
    if data and isinstance(data, list):
        for record in data:
            type = record['part_type_name']
            birthdate = parse_saferoads_birthdate(record['person_birthday'])
            gender = record['person_sex_name']
            if not gender or gender == UNKNOWN_GENDER:
                gender = None

            experience = record['driver_service_length']
            experience = int(experience) if experience != '' else None
                
            yield SaferoadsParticipants(type, birthdate, gender, experience)


def parse_saferoads_road(data):
    drawbacks = []
    if 'road_drawbacks' in data:
        items = data['road_drawbacks']
        if isinstance(items, list):
            for item in items:
                drawbacks.append(item['drawback_name'])
    light = data.get('light_type_name')
    return SaferoadsRoad(drawbacks, light)
    

def parse_saferoads(records):
    for record in records:
        type = record['type']
        timestamp = parse_datetime(record['datetime'])

        coordinates = record['geo_code']
        coordinates = Coordinates(
            latitude=coordinates['latitude'],
            longitude=coordinates['longitude']
        )

        participants_count = record['num_of_party']
        vehicles_count = record['num_of_vehicle']
        victims = record['num_of_victim']
        fatalities = record['num_of_fatalities']

        vehicles = list(parse_saferoads_vehicles(record.get('vehicles')))
        participants = list(parse_saferoads_participants(record.get('participants')))
        road = parse_saferoads_road(record)
        mia = bool(record['from_mia'])

        yield SaferoadsRecord(
            type, timestamp, coordinates,
            participants_count, vehicles_count, victims, fatalities,
            participants, vehicles, road, mia
        )


def get_saferoads_crash_url(id):
    return 'http://xn--80abhddbmm5bieahtk5n.xn--p1ai/crashes/{id}'.format(
        id=id
    )


def load_uiks():
    table = pd.read_csv(UIKS, encoding='utf8', sep='\t')
    table = table.where(pd.notnull(table), None)
    for _, row in table.iterrows():
        (id, name, region_id, region_name, oik,
         tik_id, tik_name, address, latitude, longitude) = row
        if id.isdigit():
            id = int(id)
        coordinates = None
        if latitude and longitude:
            coordinates = Coordinates(latitude, longitude)
        yield Uik(
            id, name, region_id, region_name, oik,
            tik_id, tik_name, address, coordinates
        )



def load_party_cells():
    with open(PARTY_RESULTS) as file:
        columns = next(file)
        columns = columns.rstrip('\n').split('\t')
        columns = [(int(_) if _.isdigit() else _) for _ in columns[1:]]
        for line in file:
            line = line.rstrip()
            row = line.split('\t')
            uik_id = row[0]
            if uik_id.isdigit():
                uik_id = int(uik_id)
            
            row = [int(_) for _ in row[1:]]
            assert len(row) == len(columns)
            for row_id, value in zip(columns, row):
                yield ResultsCell(uik_id, row_id, value)


def is_russia_coordinates(coordinates):
    if coordinates:
        latitude, longitude = coordinates
        return 40 <= latitude <= 80 and 0 <= longitude <= 200


def to_mercator(coordinates):
    latitude, longitude = coordinates
    R = 6378137.000
    x = R * math.radians(longitude)
    scale = x / longitude
    y = 180.0 / math.pi * math.log(
        math.tan(math.pi / 4.0 + latitude * (math.pi / 180.0) / 2.0)
    ) * scale
    return x, y


def get_saferoads_points(saferoads):
    for record in saferoads:
        coordinates = record.coordinates
        if is_russia_coordinates(coordinates):
            x, y = to_mercator(coordinates)
            yield Point(x, y, 1)


def points_density(points, xlim, ylim, width, height):
    matrix = []
    for _ in xrange(height):
        row = [0] * width
        matrix.append(row)
    
    x_min, x_max = xlim
    x_step = (x_max - x_min) / width
    y_min, y_max = ylim
    y_step = (y_max - y_min) / height
    for x, y, value in points:
        if x_min <= x < x_max and y_min <= y < y_max:
            column = int((x - x_min) / x_step)
            row = int((y - y_min) / y_step)
            matrix[row][column] += value
    return np.array(matrix)


def show_density(matrix, cmap='hot'):
    fig, ax = plt.subplots(facecolor='black')
    matrix = np.log(matrix + 1)
    ax.imshow(matrix, cmap=cmap, origin='lower')
    ax.axis('off')
    fig.set_size_inches(14, 6)


def get_uik_points(uiks, party_cells):
    # Число избирателей, внесенных в список избирателей на момент
    # окончания голосования
    uik_sizes = {_.uik_id: _.value for _ in party_cells if _.row_id == 1}  

    for record in uiks:
        coordinates = record.coordinates
        if is_russia_coordinates(coordinates):
            x, y = to_mercator(coordinates)
            size = uik_sizes.get(record.id)
            if size:
                point = Point(x, y, size)
                yield point


def show_crashes_during_day(data):
    table = pd.Series(
        [datetime(2000, 1, 1, hour=_.timestamp.hour, minute=_.timestamp.minute)
        for _ in data]
    )
    fig, ax = plt.subplots()
    table.value_counts().resample('1800s').sum().plot(ax=ax)
    ax.set_ylabel(u'Число ДТП по 30 мин. интервалам')
    ax.grid('off')
    ax.grid(which='minor')
    ax.xaxis.grid(True)
    ax.set_xticks([]) 


def months_range(start, stop):
    if start > stop:
        start, stop = stop, start
    while start <= stop:
        yield start
        year = start.year
        month = start.month + 1
        if month > 12:
            year += 1
            month = 1
        start = datetime(year=year, month=month, day=1)


def parse_date(value):
    return datetime.strptime(value, '%Y-%m-%d')


def format_gibdd_request_month(date, prefix='MONTH'):
    return '{prefix}:{month}.{year}'.format(
        prefix=prefix,
        month=date.month,
        year=date.year
    )


def parse_gibdd_region(data, parent_id, level):
    meta = cjson.decode(data[u'metabase'])
    for item in cjson.decode(meta[0]['maps']):
        id = int(item['id'])
        name = item['name']
        yield GibddRegion(level, parent_id, id, name)
        
        
def load_gibdd_region(request, parent_id=None, level=None):
    data = load_json(request.key)
    return parse_gibdd_region(data, parent_id, level)


def gibdd_dumps(data):
    return json.dumps(data, separators=(',', ':'))


def get_gibdd_region_request(id):
    # get region tree for september
    date = parse_date('2016-09-01')
    months = [
        format_gibdd_request_month(_)
        for _ in months_range(date, date)
    ]
    payload = {
        'date': gibdd_dumps(months),
        'maptype': 1,
        'pok': '1',
        'region': str(id)
    }
    return GibddRequest(
        'http://stat.gibdd.ru/map/getMainMapData',
        payload
    )


def download_gibdd_json(request):
    response = requests_.post(
        request.url,
        data=request.data,
        headers={
            'User-Agent': USER_AGENT,
            'Content-Type': 'application/json; charset=UTF-8'
        },
        timeout=300
    )
    try:
        return response.json()
    except ValueError:
        return


def fetch_gibdd_json(request):
    key = request.key
    path = get_json_path(key)
    data = download_gibdd_json(request)
    dump_json(path, data)
    update_json_cache(key)


def load_raw_gibdd_regions(root_id):
    request = get_gibdd_region_request(root_id)
    for parent in load_gibdd_region(request, parent_id=root_id, level=1):
        yield parent
        parent_id = parent.id
        request = get_gibdd_region_request(parent_id)
        for region in load_gibdd_region(request, parent_id=parent_id, level=2):
            yield region


def get_gibdd_cards_request(region, months):
    months = [format_gibdd_request_month(_, prefix='MONTHS') for _ in months]
    data = {
        'date': months,
        'ParReg': str(region.parent_id),
        'order': {
            'type': '1',
            'fieldName': 'dat'
        },
        'reg': str(region.id),
        'ind': '1',
        'st': '1',
        'en': '100000000'
    }
    payload = {'data': gibdd_dumps(data)}
    return GibddCardsRequest(
        'http://stat.gibdd.ru/map/getDTPCardData',
        payload,
        region
    )


def parse_gibdd_timestamp(date, time):
    return datetime.strptime(
        date + ' ' + time,
        '%d.%m.%Y %H:%M'
    )


def parse_gibdd_card(data, region_id):
    id = data['KartId']
    timestamp = parse_gibdd_timestamp(data['date'], data['Time'])
    type = data['DTP_V']
    vehicles = data['K_TS']
    participants = data['K_UCH']
    fatalities = data['POG']
    victims = data['RAN']
    info = data['infoDtp']
    address = parse_gibdd_address(info)
    problems = [_ for _ in info['ndu'] if _]
    why = list(parse_gibdd_why(info))
    return GibddCard(
        id, region_id, timestamp, type,
        participants, vehicles, victims, fatalities,
        address, problems, why
    )


def parse_gibdd_address(data):
    city = data['n_p'] or None
    street = data['street'] or None
    house = data['house'] or None
    road = data['dor'] or None
    km = data['km'] or None
    if km is not None:
        km = int(km)
    m = data['m'] or None
    if m is not None:
        m = int(m)
    where = data['sdor']
    return GibddAddress(city, street, house, road, km, m, where)


def parse_gibdd_why(data):
    for record in data['npdd']:
        who = record['name']
        for text in record['ArrN']:
            yield GibddWhy(who, text)


def parse_gibdd_cards(data, region_id):
    if not data:
        return
    data = data['data']
    if not data:
        return
    data = cjson.decode(data)
    items = data['tab']
    assert data['countCard'] == len(items)
    for item in items:
        yield parse_gibdd_card(item, region_id)


def load_raw_gibdd_cards(requests):
    for request in requests:
        data = load_json(request.key)
        for record in parse_gibdd_cards(data, request.region.id):
            yield record


def show_saferoads_gibdd(saferoads, gibdd_cards):
    counts = Counter()
    for record in saferoads:
        counts[record.timestamp] += 1
    saferoads_series = pd.Series(counts).resample('M').sum()

    counts = Counter()
    for record in gibdd_cards:
        counts[record.timestamp] += 1
    gibdd_series = pd.Series(counts).resample('M').sum()
    
    table = pd.DataFrame({
        u'безопасныедороги.рф': saferoads_series,
        'stat.gibdd.ru': gibdd_series
        })
    fig, ax = plt.subplots()
    table.plot(ax=ax)
    ax.set_ylabel(u'Число ДТП')
    ax.set_ylim((0, None))
    ax.xaxis.grid(None)


def dump_gibdd_regions(regions):
    table = pd.DataFrame(regions)
    table.to_csv(GIBDD_REGIONS, sep='\t', encoding='utf8', index=False)


def load_gibdd_regions():
    table = pd.read_csv(GIBDD_REGIONS, sep='\t', encoding='utf8')
    for _, row in table.iterrows():
        level, parent_id, id, name = row
        yield GibddRegion(level, parent_id, id, name)


def serialize_timestamp(timestamp):
    if timestamp:
        return datetime.strftime(timestamp, '%Y-%m-%d %H:%M:%S')


def deserialize_timestamp(value):
    if value:
        return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')


def dump_gibdd_cards(records):
    for record in gibdd_cards:
        for item in record.address.where:
            assert ';' not in item
        for item in record.why:
            text = item.text
            assert ';' not in text
            assert ':' not in text
        for item in record.problems:
            assert ';' not in item
        
    data = []
    for record in records:
        timestamp = serialize_timestamp(record.timestamp)
        address = record.address
        places = '; '.join(address.where)
        problems = '; '.join(record.problems)
        reasons = '; '.join(
            _.who + ': ' + _.text
            for _ in record.why
        )
        km = address.km
        km = str(km) if km is not None else None
        m = address.m
        m = str(m) if m is not None else None
        data.append((
            record.id, record.region_id, timestamp, record.type,
            record.participants, record.vehicles, record.victims, record.fatalities,
            address.city, address.street, address.house, address.road, km, m, places,
            problems, reasons
            ))
    table = pd.DataFrame(
        data,
        columns=[
            'id', 'region_id', 'timestamp', 'type',
            'participants', 'vehicles', 'victims', 'fatalities',
            'city', 'street', 'house', 'road', 'km', 'm', 'places',
            'problems', 'reasons'
        ]
    )
    table.to_csv(GIBDD_CARDS,  sep='\t', encoding='utf8', index=False)


def deserialize_gibdd_why(value):
    if value:
        for item in value.split('; '):
            who, text = item.split(': ', 1)
            yield GibddWhy(who, text)
    
    
def read_gibdd_cards(path):
    with open(path) as file:
        lines = iter(file)
        next(lines) # skip header
        for line in lines:
            line = line.rstrip('\n').decode('utf8')
            record = [(_ if _ != '' else None) for _ in  line.split('\t')]
            yield record

            
def load_gibdd_cards():
    for row in read_gibdd_cards(GIBDD_CARDS):
        (id, region_id, timestamp, type,
         participants, vehicles, victims, fatalities,
         city, street, house, road, km, m, places,
         problems, reasons) = row
        id = int(id)
        region_id = int(region_id)
        participants = int(participants)
        vehicles = int(vehicles)
        victims = int(victims)
        fatalities = int(fatalities)
        timestamp = deserialize_timestamp(timestamp)
        where = places.split('; ')
        if problems:
            problems = problems.split('; ')
        else:
            problems = []
        why = list(deserialize_gibdd_why(reasons))
        km = int(km) if km is not None else None
        m = int(m) if m is not None else None
        yield GibddCard(
            id, region_id, timestamp, type,
            participants, vehicles, victims, fatalities,
            GibddAddress(
                city, street, house,
                road, km, m, where
            ),
            problems, why
        )


def show_fatalities_during_day(data):
    total = Counter()
    counts = Counter()
    for record in data:
        timestamp = record.timestamp
        day = timestamp.isocalendar()[2]
        time = datetime(2000, 1, 1, hour=timestamp.hour, minute=timestamp.minute)
        counts[time] += record.fatalities
        total[time] += 1
    table = pd.DataFrame({
        'fatalities': pd.Series(counts),
        'total': pd.Series(total)
    })
    table = table.resample('1800s').sum()
    fig, ax = plt.subplots()
    (table.fatalities / table.total).plot(ax=ax)
    formater = mtick.FuncFormatter(
        lambda value, _: '{}%'.format(int(value * 100))
    )
    ax.yaxis.set_major_formatter(formater)
    ax.set_ylabel(u'Доля аварий со смертями')
    ax.grid('off')
    ax.xaxis.grid(True)
    ax.grid(which='minor')
    ax.set_xticks([]) 


def patch_ru_month_tick(tick):
    text = tick.get_text()
    for source, target in MONTH_LABELS.iteritems():
        if source in text:
            return text.replace(source, target)
    return text


def show_crashes_during_year(data):
    counts = Counter()
    for record in data:
        counts[record.timestamp] += 1
    table = pd.Series(counts)
    table = table.resample('W').sum()
    fig, ax = plt.subplots()
    table.plot(ax=ax)
    ax.set_ylabel(u'Число ДТП по месяцам')
    ax.grid('off')
    ax.xaxis.grid(True)
    fig.canvas.draw()
    ticks = [patch_ru_month_tick(_) for _ in ax.get_xticklabels()]
    ax.set_xticklabels(ticks)


def show_fatalities_during_year(data):
    total = Counter()
    counts = Counter()
    for record in data:
        timestamp = record.timestamp
        counts[timestamp] += record.fatalities
        total[timestamp] += 1
    table = pd.DataFrame({
        'fatalities': pd.Series(counts),
        'total': pd.Series(total)
    })
    table = table.resample('W').sum()
    fig, ax = plt.subplots()
    (table.fatalities / table.total).plot(ax=ax)
    # ax.set_ylabel(u'Число смертей по 30 мин. интервалам')


def show_crashes_during_week(data, alphas):
    counts = Counter()
    size = len(alphas)
    for record in data:
        timestamp = record.timestamp
        day = timestamp.isocalendar()[2]
        if day <= size:
            time = datetime(2000, 1, 1, hour=timestamp.hour, minute=timestamp.minute)
            counts[time, day] += 1
    colors = []
    for index in xrange(size):
        alpha = alphas[index]
        color = (74./255 , 113./255, 178./255, alpha)
        if alpha > 0.5:
            # color = (68./255, 156./255, 83./255, alpha)
            color = (1, 0, 0, alpha)

        colors.append(color)
    table = pd.Series(counts)
    table = table.unstack()
    fig, ax = plt.subplots()
    table = table.resample('1800s').sum().plot(ax=ax, colors=colors)
    ax.set_ylabel(u'Число ДТП по 30 мин. интервалам')
    ax.grid('off')
    ax.legend().remove()
    ax.grid(which='minor')
    ax.xaxis.grid(True)
    ax.set_xticks([]) 


def show_crashes_during_year_by_type(data, types, other=False):
    counts = Counter()
    for record in data:
        type = record.type
        if other and type not in types:
            type = OTHER
        counts[record.timestamp, type] += 1
    table = pd.Series(counts)
    table = table.unstack()
    table = table.resample('W').sum()

    if other:
        types.append(OTHER)
    table = table.reindex(columns=types)
    fig, ax = plt.subplots()
    table.plot(ax=ax)
    ax.set_ylabel(u'Число ДТП по неделям')
    ax.grid('off')
    ax.xaxis.grid(True)
    ax.legend(bbox_to_anchor=(0, -0.1), loc=2, ncol=2)
    fig.canvas.draw()
    ticks = [patch_ru_month_tick(_) for _ in ax.get_xticklabels()]
    ax.set_xticklabels(ticks)


def show_crashes_during_week_by_region_size(cards, regions):
    region_ids = {_.id: _ for _ in regions}
    counts = Counter()
    for record in cards:
        region = region_ids[record.region_id]
        name = region.name
        if name not in MILLIONS:
            region = region_ids[region.parent_id]
            name = region.name
            if name not in MILLIONS:
                name = OTHER
        timestamp = record.timestamp
        time = datetime(2000, 1, 1, hour=timestamp.hour, minute=timestamp.minute)
        counts[time, name] += 1

    table = pd.Series(counts)
    table = table.unstack()
    table = table.resample('1800s').sum()
    table = table[table.columns[1:]]     
    table.plot()


def show_death_by_types(records):
    total = Counter()
    counts = Counter()
    for record in records:
        type = record.type
        total[type] += 1
        if record.fatalities:
            counts[type] += 1
    table = pd.DataFrame({
        'total': total,
        'fatalities': counts
    })
    table = table.sort_values(by='total', ascending=False)
    table = table.head(10)
    (table.fatalities / table.total).plot(kind='bar')


def show_types_by_places(records):
    counts = Counter()
    for record in records:
        type = record.type
        if type not in TOP_TYPES:
            type = OTHER
        for place in record.address.where:
            if place != UNKNOWN_PLACE and place not in TOP_PLACES:
                place = OTHER
            counts[place, type] += 1
    table = pd.Series(counts)
    table = table.unstack()
    table = table.reindex(index=TOP_PLACES, columns=TOP_TYPES)
    table = table.div(table.sum(axis=0), axis=1)
    sns.heatmap(table)


def show_types_by_problem(records):
    counts = Counter()
    for record in records:
        type = record.type
        if type not in TOP_TYPES:
            type = OTHER
        for problem in record.problems:
            if problem not in TOP_PROBLEMS:
                problem = OTHER
            counts[problem, type] += 1
            
    table = pd.Series(counts)
    table = table.unstack()
    table = table.reindex(index=TOP_PROBLEMS, columns=TOP_TYPES)
    table = table.div(table.sum(axis=0), axis=1)
    sns.heatmap(table)


def show_types_by_reason(records, who, reasons):
    counts = Counter()
    for record in records:
        type = record.type
        if type not in TOP_TYPES:
            type = OTHER
        for reason in record.why:
            if reason.who == who:
                reason = reason.text
                if reason not in reasons:
                    reason = OTHER
                counts[reason, type] += 1

    table = pd.Series(counts)
    table = table.unstack()
    table = table.reindex(index=reasons, columns=TOP_TYPES)
    table = table.div(table.sum(axis=0), axis=1)
    sns.heatmap(table)


def show_vehicle_count_by_type(records):
    counts = Counter()
    for record in records:
        type = record.type
        if type not in TOP_TYPES:
            type = OTHER
        if record.mia:
            counts[len(record.vehicles), type] += 1
    table = pd.Series(counts)
    table = table.unstack()
    table = table.reindex(index=range(10), columns=TOP_TYPES)
    table = table.div(table.sum(axis=0), axis=1)
    sns.heatmap(table)


def show_participants_count_by_type(records):
    counts = Counter()
    for record in records:
        type = record.type
        if type not in TOP_TYPES:
            type = OTHER
        if record.mia:
            counts[len(record.participants), type] += 1
    table = pd.Series(counts)
    table = table.unstack()
    table = table.reindex(index=range(10), columns=TOP_TYPES)
    table = table.div(table.sum(axis=0), axis=1)
    sns.heatmap(table)


def show_types_by_parts(records):
    counts = Counter()
    for record in records:
        if record.mia:
            type = record.type
            if type not in TOP_TYPES:
                type = OTHER
            for item in record.vehicles:
                for part in item.parts:
                    if part not in TOP_PARTS:
                        part = OTHER
                    counts[part, type] += 1
    table = pd.Series(counts)
    table = table.unstack()
    table = table.reindex(index=TOP_PARTS, columns=TOP_TYPES)
    table = table.div(table.sum(axis=0), axis=1)
    sns.heatmap(table)


def show_types_by_vehicle(records):
    counts = Counter()
    for record in records:
        if record.mia:
            type = record.type
            if type not in TOP_TYPES:
                type = OTHER
            for item in record.vehicles:
                vehicle_type = item.type
                if vehicle_type not in TOP_VEHICLE_TYPES:
                    vehicle_type = OTHER
                counts[vehicle_type, type] += 1
    table = pd.Series(counts)
    table = table.unstack()
    table = table.reindex(index=TOP_VEHICLE_TYPES, columns=TOP_TYPES)
    table = table.div(table.sum(axis=0), axis=1)
    sns.heatmap(table)


def show_types_by_privod(records):
    counts = Counter()
    for record in records:
        if record.mia:
            type = record.type
            if type not in TOP_TYPES:
                type = OTHER
            for item in record.vehicles:
                privod = item.privod
                if privod not in TOP_PRIVODS:
                    privod = OTHER
                counts[privod, type] += 1
    table = pd.Series(counts)
    table = table.unstack()
    table = table.reindex(index=TOP_PRIVODS, columns=TOP_TYPES)
    table = table.div(table.sum(axis=0), axis=1)
    sns.heatmap(table)


def show_types_by_tyre(records):
    counts = Counter()
    for record in records:
        if record.mia:
            type = record.type
            if type not in TOP_TYPES:
                type = OTHER
            for item in record.vehicles:
                tyres = item.tyres
                if tyres not in TOP_TYRES:
                    tyres = OTHER
                counts[tyres, type] += 1
    table = pd.Series(counts)
    table = table.unstack()
    table = table.reindex(index=TOP_TYRES, columns=TOP_TYPES)
    table = table.div(table.sum(axis=0), axis=1)
    sns.heatmap(table)


def show_crashes_during_week_by_type(records, types):
    counts = Counter()
    for record in records:
        timestamp = record.timestamp
        time = datetime(2000, 1, 1, hour=timestamp.hour, minute=timestamp.minute)
        counts[time, record.type] += 1
    table = pd.Series(counts)
    table = table.unstack()
    table = table.resample('1800s').sum()
    table = table.reindex(columns=types)
    table.plot()


def show_mia_by_regions(records):
    xs = []
    ys = []
    colors = []
    for record in records:
        if is_russia_coordinates(record.coordinates):
            x, y = to_mercator(record.coordinates)
            xs.append(x)
            ys.append(y)
            color = 'r' if record.mia else 'b'
            colors.append(color)
    fig, ax = plt.subplots()
    ax.scatter(xs, ys, c=colors, s=1, lw=0, alpha=0.5)
    fig.set_size_inches((14, 6))
    ax.grid(None)
    ax.axis('off')


def show_crashes_during_year_by_tyre(records):
    counts = Counter()
    for record in records:
        timestamp = record.timestamp
        for item in record.vehicles:
            tyres = item.tyres
            if tyres and tyres != UNKNOWN_TYRES:
                if tyres not in TOP_TYRES:
                    tyres = OTHER
                counts[timestamp, tyres] += 1
    table = pd.Series(counts)
    table = table.unstack()
    table = table.reindex(columns=TOP_TYRES)
    table = table.resample('W').sum()
    table = table.div(table.sum(axis=1), axis=0)    
    table.plot()


def show_wheel_by_regions(records):
    xs = []
    ys = []
    colors = []
    for record in records:
        coordinates = record.coordinates
        if is_russia_coordinates(coordinates):
            x, y = to_mercator(coordinates)
            for item in record.vehicles:
                privod = item.privod
                color = None
                if u'левый руль' in privod:
                    color = (1, 0, 0, 0.2)
                elif u'правый руль' in privod: 
                    color = (0, 1, 1, 0.9)
                if color:
                    colors.append(color)
                    xs.append(x)
                    ys.append(y)
    fig, ax = plt.subplots(facecolor='black')
    ax.scatter(xs, ys, c=colors, s=1, lw=0)
    fig.set_size_inches((14, 6))
    ax.grid(None)
    ax.set_axis_bgcolor('black') # ax.axis('off')
    

def show_new_year_spike(records):
    counts = Counter()
    for record in records:
        timestamp = record.timestamp
        if (timestamp.year, timestamp.month) in [(2015, 12), (2016, 1)]:
            counts[timestamp, record.type] += 1
    table = pd.Series(counts)
    table = table.unstack()
    table = table.resample('d').sum()

    table = table.reindex(columns=TOP_TYPES[:2])
    fig, ax = plt.subplots()
    table.plot(ax=ax)
    ax.set_ylabel(u'Число ДТП по неделям')
    ax.grid('off')
    ax.xaxis.grid(True)


def show_vehicle_years(records):
    counts = Counter()
    for record in records:
        for item in record.vehicles:
            counts[item.year] += 1
    table = pd.Series(counts)
    table[table.index > 1980].plot()


def show_genders(records):
    counts = Counter()
    for record in records:
        if record.mia:
            for item in record.participants:
                gender = item.gender
                if gender:
                    counts[gender] += 1
    table = pd.Series(counts)
    fig, ax = plt.subplots()
    table.index = [u'Женщины', u'Мужчины']
    table.plot(kind='pie', explode=(0.1, 0), shadow=True, radius=0.6)
    fig.set_size_inches(6, 6)
    ax.set_ylabel('')


def show_genders_by_type(records):
    counts = Counter()
    for record in records:
        if record.mia:
            type = record.type
            if type not in TOP_TYPES:
                type = OTHER
            for item in record.participants:
                gender = item.gender
                if gender:
                    counts[gender, type] += 1
    table = pd.Series(counts)
    table = table.unstack()
    table = table.reindex(columns=TOP_TYPES)
    table = table.div(table.sum(axis=0), axis=1)
    table.T.plot(kind='bar')


def show_genders_by_experience(records):
    counts = Counter()
    for record in records:
        if record.mia:
            for item in record.participants:
                gender = item.gender
                experience = item.experience
                if gender and experience and experience < 40:
                    counts[experience, gender] += 1

    table = pd.Series(counts)
    table = table.unstack()
    table = table.div(table.sum(axis=0), axis=1)
    table.columns = [u'Женщины', u'Мужчины']
    fig, ax = plt.subplots()
    table.plot(ax=ax)
    formater = mtick.FuncFormatter(
        lambda value, _: '{}%'.format(int(value * 100))
    )
    ax.yaxis.set_major_formatter(formater)
    ax.set_ylabel(u'Доля аварий')
    ax.set_xlabel(u'Стаж, лет')


def show_genders_by_birth(records):
    counts = Counter()
    for record in records:
        if record.mia:
            for item in record.participants:
                gender = item.gender
                birthdate = item.birthdate
                if gender and birthdate and birthdate.year > 1930:
                    counts[birthdate, gender] += 1
    table = pd.Series(counts)
    table = table.unstack()
    table = table.resample('A').sum()
    table = table.div(table.sum(axis=0), axis=1)
    table.plot()


def show_genders_by_age(records):
    counts = Counter()
    for record in records:
        if record.mia:
            for item in record.participants:
                gender = item.gender
                birthdate = item.birthdate
                experience = item.experience
                if gender and birthdate and birthdate.year > 1930 and experience and experience < 40:
                    age = (record.timestamp.year - experience) - birthdate.year
                    counts[age, gender] += 1
    table = pd.Series(counts)
    table = table.unstack()
    table = table.div(table.sum(axis=0), axis=1)
    fig, ax = plt.subplots()
    table.plot(xlim=(10, 70), ax=ax)
    ax.set_ylabel(u'Доля')
    ax.set_xlabel(u'Возраст получения прав')
    ax.legend(bbox_to_anchor=(1, 1.12), loc=1, ncol=3)


def dump_saferoads_participants(records):
    for record in records:
        type, birthdate, gender, experience = record
        birthdate = serialize_timestamp(birthdate)
        yield {
            'type': type,
            'birthdate': birthdate,
            'gender': gender,
            'experience': experience
        }

def dump_saferoads_participants(records):
    for record in records:
        type, birthdate, gender, experience = record
        birthdate = serialize_timestamp(birthdate)
        yield {
            'type': type,
            'birthdate': birthdate,
            'gender': gender,
            'experience': experience
        }


def format_saferoads(records):
    for record in records:
        (type, timestamp, coordinates,
         participants_count, vehicles_count, victims, fatalities,
         participants, vehicles, road, mia) = record
        timestamp = serialize_timestamp(timestamp)
        participants = list(dump_saferoads_participants(participants))
        vehicles = [_._asdict() for _ in vehicles]
        data = {
            'type': type,
            'timestamp': timestamp,
            'coordinates': coordinates._asdict(),
            'participants_count': participants_count,
            'vehicles_count': vehicles_count,
            'victims': victims,
            'fatalities': fatalities,
            'participants': participants,
            'vehicles': vehicles,
            'road': road._asdict(),
            'mia': mia
        }
        yield json.dumps(data, ensure_ascii=False)

        
def dump_saferoads(records):
    with open(SAFEROADS, 'w') as file:
        for line in format_saferoads(records):
            file.write(line.encode('utf8') + '\n')


def load_saferoads():
    with open(SAFEROADS) as file:
        for line in file:
            data = json.loads(line)
            
            yield SaferoadsRecord(
                type=data['type'],
                timestamp=deserialize_timestamp(data['timestamp']),
                coordinates=Coordinates(**data['coordinates']),
                participants_count=data['participants_count'],
                vehicles_count=data['vehicles_count'],
                victims=data['victims'],
                fatalities=data['fatalities'],
                participants=[
                    SaferoadsParticipants(
                        type=_['type'],
                        birthdate=deserialize_timestamp(_['birthdate']),
                        gender=_['gender'],
                        experience=_['experience']
                    )
                    for _ in data['participants']
                ],
                vehicles=[
                    SaferoadsVehicle(**_)
                    for _ in data['vehicles']
                ],
                road=SaferoadsRoad(**data['road']),
                mia=data['mia']
            )
