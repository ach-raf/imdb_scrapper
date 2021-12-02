# Importing the required modules
import time
import re
import sqlite3
import os
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import json
import psycopg2

import logging
from selenium.webdriver.remote.remote_connection import LOGGER

LOGGER.setLevel(logging.WARNING)

PATH_TO_CHROME_DRIVER = r'chromedriver'

# options for selenium
options = Options()
options.add_argument('--headless')
options.add_argument("--incognito")
options.add_argument("--disable-crash-reporter")
options.add_argument("--disable-extensions")
options.add_argument("--disable-in-process-stack-traces")
options.add_argument("--disable-logging")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--log-level=3")
options.add_argument("--output=/dev/null")
options.headless = True

browser = webdriver.Chrome(
    executable_path=PATH_TO_CHROME_DRIVER, options=options)

database_location = 'imdb.db'
CONNECTION = sqlite3.connect(database_location)
CURSOR = CONNECTION.cursor()

GENRES = ['Animation', 'Action', 'Adventure']


def set_up_database():
    if not check_table_exists('movie_details'):
        print('creating movie_details table')
        _sql_command = \
            f''' 
        CREATE TABLE movie_details (imdb_id TEXT NOT NULL  PRIMARY KEY, title TEXT, score TEXT, voters TEXT, plot TEXT, 
        poster TEXT, director Text, rated TEXT, runtime TEXT, genre TEXT, type TEXT, release_date TEXT) '''
        CURSOR.execute(_sql_command)
        CONNECTION.commit()

    if not check_table_exists('serie_details'):
        print('creating serie_details table')
        _sql_command = \
            f''' 
        CREATE TABLE serie_details (imdb_id TEXT NOT NULL  PRIMARY KEY, title TEXT, score TEXT, voters TEXT, plot TEXT, 
        poster TEXT, creator Text, rated TEXT, runtime TEXT, genre TEXT, year TEXT, type TEXT, seasons TEXT, release_date TEXT) '''
        CURSOR.execute(_sql_command)
        CONNECTION.commit()


def check_table_exists(_table_name):
    _rows = CURSOR.execute(
        f''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{_table_name}' ''')
    CONNECTION.commit()
    if _rows.fetchone()[0] == 1:
        print(f'{_table_name} table found')
        return True
    else:
        print(f'{_table_name} table not found')
        return False


def get_imdb_id(_link):
    return re.search('https://www.imdb.com/title/(.{10}?|.{9})', _link).group(1)


def check_item_exists(_imdb_id):
    _row = CURSOR.execute(
        f''' SELECT title from movie_details where imdb_id = "{_imdb_id}" ''').fetchone()
    CONNECTION.commit()
    if _row:
        return _row[0]
    else:
        _row = CURSOR.execute(
            f''' SELECT title from serie_details where imdb_id = "{_imdb_id}" ''').fetchone()
        CONNECTION.commit()
        if _row:
            return _row[0]
        else:
            return False


def get_dataframe(_query):
    return pd.read_sql_query(_query, CONNECTION)


def clean_text(_text):
    return _text.strip().replace('\n', '') \
        .replace('\xa0', '') \
        .replace('             EN', '') \
        .replace('"', '') \
        .replace('See full summary»', '').replace("'", '').strip()


def get_image_full_size(_image):
    _image = re.search(
        '(https://m\.media-amazon\.com/images/M.*?\.)', _image).group(1)
    return f'{_image}jpg'


def list_to_string(_list):
    formatted_list = _list[0]
    for item in _list[1:]:
        formatted_list.join(f', {item}')
    return formatted_list


def clean_details_information(_soup):
    try:
        _details_information = clean_text(
            _soup.select_one('.subtext').text).split('|')
    except AttributeError:
        _details_information = 'NA'
    _clean_info = []
    for info in _details_information:
        _clean_info.append(clean_text(info))
    return _clean_info


def get_html(_link):
    browser.implicitly_wait(0.1)
    browser.get(_link)
    browser.minimize_window()
    # sleep to let the page load
    # time.sleep(0.5)
    _html = browser.page_source
    _soup = BeautifulSoup(_html, 'lxml')
    return _soup


def list_to_string(_list):
    formatted_list = _list[0]
    for item in _list[1:]:
        formatted_list.join(f', {item}')
    return formatted_list


def get_title(_media_info):
    _title = 'NA'
    return clean_text(_title)


def get_score(_media_info):
    _score = 'NA'
    return _score


def get_poster(_media_info):
    try:
        _poster = _media_info['image']
        if 'https' in _poster:
            return get_image_full_size(_poster)
        else:
            return _poster
    except KeyError:
        return 'NA'


def get_plot(_media_info):
    _plot = 'NA'
    return _plot


def get_voters(_media_info):
    _voters = 'NA'
    return _voters


def get_creators(_creators):
    person_creators = []
    for _creator in _creators:
        try:
            if _creator['@type'] == 'Person':
                person_creators.append(_creator["name"])
        except KeyError:
            print('This is an Organization')
    if person_creators:
        return list_to_string(person_creators)
    else:
        return 'This was created by an Organization'


def get_seasons(_soup):
    try:
        _seasons = _soup.select('#browse-episodes-season')
        return int(_seasons[0]['aria-label'].replace(' seasons', ''))
    except IndexError:
        try:
            _seasons = _soup.select_one(
                '.BrowseEpisodes__BrowseLinksContainer-sc-1a626ql-4 > a:nth-child(2) > div:nth-child(1)').text
            return _seasons.replace(' seasons', '')
        except AttributeError:
            return 'NA'


def get_series_runtime(_soup):
    try:
        _runtime = _soup.select_one(
            '.TitleBlockMetaData__MetaDataList-sc-12ein40-0 > li:nth-child(4)').text
        return _runtime.strip()
    except AttributeError:
        return 'NA'


def get_year(_soup):
    _year = (_soup.select_one(
        '.TitleBlockMetaData__MetaDataList-sc-12ein40-0 > li:nth-child(2)').text).strip()
    return _year[:len(_year)//2]


def get_details(_link):
    print(_link)

    is_series = False
    seasons = 'NA'
    imdb_info = []

    soup = get_html(_link)
    _script = soup.find('script', type='application/ld+json')
    _script = str(_script).replace(
        '</script>', '').replace('<script type="application/ld+json">', '')

    media_info = json.loads(_script)

    imdb_id = get_imdb_id(_link)
    title = clean_text(media_info['name'])
    try:
        score = media_info['aggregateRating']['ratingValue']
    except KeyError:
        score = 'NA'
    try:
        plot = clean_text(media_info['description'])
    except KeyError:
        plot = 'NA'
    try:
        rated = media_info['contentRating']
        voters = media_info['aggregateRating']['ratingCount']
    except KeyError:
        rated = 'NA'
        voters = 'NA'
    genres = ', '.join(media_info['genre'])
    try:
        release_date = media_info['datePublished']
    except KeyError:
        release_date = 'NA'
    poster = get_poster(media_info)
    imdb_info.append({'title': title})
    imdb_info.append({'score': score})
    imdb_info.append({'voters': voters})
    imdb_info.append({'plot': plot})
    imdb_info.append({'poster': poster})

    type = media_info['@type']
    match type:
        case 'TVSeries':
            try:
                creators = get_creators(media_info['creator'])
            except KeyError:
                creators = 'NA'
            seasons = get_seasons(soup)
            runtime = get_series_runtime(soup)
            years = get_year(soup)

            insertion_details = CURSOR.execute(
                f'INSERT INTO serie_details VALUES ("{imdb_id}", "{title}" , "{score}", "{voters}", "{plot}", "{poster}", "{creators}"'
                f', "{rated}", "{runtime}", "{genres}", "{years}", "{type}", "{seasons}", "{release_date}")')

        case 'Movie':
            try:
                directors = get_creators(media_info['director'])
            except KeyError:
                directors = 'NA'
            try:
                duration = media_info['duration']
            except KeyError:
                duration = 'NA'
            insertion_details = CURSOR.execute(
                f'INSERT INTO movie_details VALUES ("{imdb_id}", "{title}" , "{score}", "{voters}", "{plot}", "{poster}", "{directors}"'
                f', "{rated}", "{duration}", "{genres}", "{type}", "{release_date}")')

    CONNECTION.commit()
    print(f'{title} Added to database')

    return insertion_details


def show_all():
    db = CONNECTION.cursor()
    _rows = db.execute(
        f''' SELECT * from movie_details inner join serie_details on movie_details.title = serie_details.title''').fetchall()
    CONNECTION.commit()
    print(_rows)


def mongodb_format():
    # This enables column access by name: row['column_name']
    CONNECTION.row_factory = sqlite3.Row
    _rows = CURSOR.execute(
        f''' SELECT * from movie_details inner join serie_details on movie_details.title = serie_details.title''').fetchall()
    CONNECTION.commit()
    with open('data.json', 'a+') as outfile:
        for _row in _rows:
            imdb_movie = dict(
                (CURSOR.description[i][0], value) for i, value in enumerate(_row))
            imdb_movie['_class'] = 'com.back_sync.models.Imdb'
            json.dump(imdb_movie, outfile)
            outfile.write('\n')


def get_json_data(_imdb_id):
    # This enables column access by name: row['column_name']
    CONNECTION.row_factory = sqlite3.Row
    _rows = CURSOR.execute(
        f''' SELECT * from details where imdb_id = "{_imdb_id}" ''').fetchall()
    CONNECTION.commit()
    return [dict(_row) for _row in _rows]  # CREATE JSON


def delete_item_by_id(_imdb_id):
    # This enables column access by name: row['column_name']
    CONNECTION.row_factory = sqlite3.Row
    _rows = CURSOR.execute(
        f''' delete from details where imdb_id = "{_imdb_id}" ''')
    CONNECTION.commit()
    print(f'{_imdb_id} deleted')


def main():
    imdb_base_path = 'https://www.imdb.com/title/'
    ids = ['tt0372784', 'tt1474684', 'tt1345836', 'tt0468569', 'tt0482571', 'tt1375666', 'tt0816692', 'tt4154756',
           'tt3428912', 'tt2249364', 'tt2303687', 'tt7493974', 'tt2294189', 'tt4258440', 'tt3877200', 'tt5269594',
           'tt4192812', 'tt0310455', 'tt3581932', 'tt1738419', 'tt0185906', 'tt5084170', 'tt5722190', 'tt0495027',
           'tt13567480', 'tt10011298', 'tt0120669', 'tt2084970', 'tt0484855', 'tt0423661', 'tt6902676',
           'tt3428912', 'tt8946378', 'tt0096633', 'tt3238856', 'tt3502172', 'tt1337672', 'tt0223897',
           'tt0223897', 'tt0156887', 'tt0109040', 'tt2155025', 'tt3516878', 'tt1948563', 'tt0114469', 'tt0121955',
           'tt2494280', 'tt0120237', 'tt1493239', 'tt0101761', 'tt0901487', 'tt0081874', 'tt0756683', 'tt11639414',
           'tt0416394', 'tt0459159', 'tt1093357', 'tt1740299', 'tt2544316', 'tt2384811', 'tt0038032', 'tt0243017',
           'tt0112230', 'tt2210479', 'tt2104011', 'tt9266104', 'tt6262096', 'tt5805470', 'tt0105946', 'tt0791205',
           'tt7131720', 'tt7382936', 'tt2967286', 'tt3539986', 'tt0079944', 'tt0185906', 'tt0795176', 'tt1749004',
           'tt8416494', 'tt2701582', 'tt8772296', 'tt6317068', 'tt4378376', 'tt5687612', 'tt5607976', 'tt7725422',
           'tt7908628', 'tt8879940', 'tt7939766', 'tt5555260', 'tt0756683', 'tt11639414', 'tt9304350', 'tt11147852',
           'tt8107988', 'tt7078180', 'tt0816398', 'tt1847445', 'tt8101850'
           ]
    more_ids = ['tt2164430', 'tt1305826', 'tt10937602', 'tt3742982', 'tt12331342', 'tt7653274', 'tt11714178',
                'tt11754514', 'tt10885406', 'tt2560140', 'tt4378376', 'tt0981456', 'tt8515016', 'tt5348176',
                'tt3032476', 'tt4270492', 'tt7441658', 'tt0434665', 'tt0903747', 'tt2578560', 'tt13192574',
                'tt6548228', 'tt6517102', 'tt8673610', 'tt12150836', 'tt7263380', 'tt0213338', 'tt7865090',
                'tt11405390', 'tt12443322', 'tt9335498', 'tt11656892', 'tt8314920', 'tt9679542', 'tt10011298',
                'tt6866266', 'tt9307686', 'tt0863046', 'tt3551096', 'tt0944947', 'tt7661390', 'tt12350092',
                'tt10479420', 'tt8690728', 'tt8225204', 'tt11680468', 'tt0209631', 'tt0948103', 'tt7806844',
                'tt0495212', 'tt8253044', 'tt5607976', 'tt0478838', 'tt3306838', 'tt9004082', 'tt11295582', 'tt0385426',
                'tt2674806', 'tt0472954', 'tt12767982', 'tt12343534', 'tt9522300', 'tt10320398', 'tt3114390',
                'tt2348803', 'tt3530232', 'tt0096633', 'tt10487750', 'tt6128254', 'tt2942218', 'tt12486080',
                'tt5057130', 'tt5897304', 'tt12831098', 'tt5626028', 'tt0388629', 'tt4508902', 'tt1266020', 'tt6586318',
                'tt7649694', 'tt5607616', 'tt3973820', 'tt2861424', 'tt9402026', 'tt3526078', 'tt7137906', 'tt2575988',
                'tt10204658', 'tt8910922', 'tt0121955', 'tt0187664', 'tt8747928', 'tt5514358', 'tt7660850',
                'tt10986410', 'tt9054364', 'tt4604612', 'tt1190634', 'tt3230854', 'tt12227418', 'tt6470478',
                'tt4955642', 'tt5834204', 'tt3874528', 'tt6859260', 'tt8111088', 'tt5788792', 'tt0416394', 'tt12432936',
                'tt5691552', 'tt8788458', 'tt9529546', 'tt0141842', 'tt0327386', 'tt1312171', 'tt0306414', 'tt2432604',
                'tt5057054', 'tt12057106', 'tt2356777', 'tt12464182', 'tt1759761', 'tt10233448', 'tt9140560',
                'tt11785582', 'tt8679236', 'tt3950102', 'tt0500092', 'tt9140560',
                'tt9307686', 'tt9304350', 'tt10479420', 'tt11008522', 'tt10470444', 'tt9335498', 'tt8000674',
                'tt10981954', 'tt12361974'
                ]

    #id_test = ['tt1345836', 'tt0482571', 'tt1375666', 'tt2084970', 'tt0756683']
    id_test = ['tt3877200']

    loop_counter = 1
    set_up_database()
    ids.extend(more_ids)
    for imdb_id in ids:
        print(f'{loop_counter}/{len(ids)}')
        _item = check_item_exists(imdb_id)
        if _item:
            print(f'{_item} found')
        else:
            get_details(f'{imdb_base_path}{imdb_id}')
        loop_counter += 1
    show_all()
    browser.quit()


if __name__ == '__main__':
    main()
    mongodb_format()

    CONNECTION.close()
