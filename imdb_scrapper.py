# Importing the required modules
import ast
import json
import os
import random
import re
import sqlite3
import string
import time
import logging

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dataclass.imdb import Imdb, ImdbSerie, ImdbMovie
from data.imdb_id import get_imdb_ids_dump, write_imdb_id
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.remote.remote_connection import LOGGER


################################################################################

CURRENT_DIR_PATH = os.path.dirname(os.path.realpath(__file__))

PATH_TO_CHROME_DRIVER = os.path.join(
    CURRENT_DIR_PATH, 'chromedriver', 'chromedriver')

DATABASE_LOCATION = os.path.join(CURRENT_DIR_PATH, 'database', 'imdb.db')

LOG_LOCATION = os.path.join(CURRENT_DIR_PATH, 'data', 'imdb_scrapper.log')

my_logger = logging.getLogger(__name__)
logging.basicConfig(filename=LOG_LOCATION,
                    encoding='utf-8', level=logging.INFO)


################################################################################


def get_selenium_soup(_link):
    # chrome_options for selenium
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument("--incognito")
    chrome_options.add_argument("--disable-crash-reporter")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-in-process-stack-traces")
    chrome_options.add_argument("--disable-my_logger")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--output=/dev/null")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-features=NetworkService")
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_argument("--disable-features=VizDisplayCompositor")
    chrome_options.headless = True
    #selenium_service = Service(PATH_TO_CHROME_DRIVER)

    browser = webdriver.Chrome(executable_path=PATH_TO_CHROME_DRIVER,
                               options=chrome_options)
    browser.implicitly_wait(0.1)
    browser.get(_link)
    # sleep to let the page load
    time.sleep(0.5)
    _html = browser.page_source
    _soup = BeautifulSoup(_html.text, 'lxml')
    browser.quit()
    browser.close()
    return _soup


def get_html(_link):
    try:
        _html = requests.get(_link)
        if _html.status_code == 404:
            my_logger.warn(f'{_link}, 404 page not found')
            return False
        _soup = BeautifulSoup(_html.text, 'lxml')
        return _soup
    except requests.exceptions.ChunkedEncodingError:
        time.sleep(10)
        get_html(_link)
    except requests.exceptions.ConnectionError:
        return get_selenium_soup(_link)


def get_media_info(_link, _sleep_timer=100):
    soup = get_html(_link)
    if not soup:
        return False
    _script = soup.find('script', type='application/ld+json')
    _script = str(_script).replace(
        '</script>', '').replace('<script type="application/ld+json">', '')
    try:
        media_info = json.loads(_script)
        return soup, media_info
    except json.decoder.JSONDecodeError:
        _sleep_timer += 20
        if _sleep_timer >= 250:
            my_logger.warn('Timeout, get_media_info took too long')
            return False
        my_logger.warn(
            f'get_media_info sleep for {_sleep_timer / 60} min, {_link}')
        time.sleep(_sleep_timer)
        get_media_info(_link, _sleep_timer)


def database_excute_command(_command, _fetch_type='none'):
    try:
        _connection = sqlite3.connect(DATABASE_LOCATION)
        _cursor = _connection.cursor()
        match _fetch_type.lower():
            case 'none':
                result = _cursor.execute(_command)
            case 'fetch_one':
                result = _cursor.execute(_command).fetchone()
            case 'fetch_all':
                result = _cursor.execute(_command).fetchall()
        _connection.commit()
        _cursor.close()
        _connection.close()
        return result
    except sqlite3.Error as e:
        my_logger.warn(_command)
        my_logger.warn(e)
        return False


def check_table_exists(_table_name):
    _command = f''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{_table_name}' '''
    _rows = database_excute_command(_command, 'fetch_one')
    if _rows[0] == 1:
        my_logger.info(f'{_table_name} table found')
        return True
    else:
        my_logger.info(f'{_table_name} table not found')
        return False


def set_up_database():
    if not check_table_exists('movie_details'):
        my_logger.info('creating movie_details table')
        _sql_command = \
            f'''
        CREATE TABLE movie_details (imdb_id TEXT NOT NULL PRIMARY KEY, title TEXT, original_title TEXT, score FLOAT, voters INT, plot TEXT,
        poster TEXT, rated TEXT, genre TEXT, media_type TEXT, release_date TEXT, countries TEXT, actors TEXT, director Text, runtime TEXT) '''
        movie_details_result = database_excute_command(_sql_command)

    if not check_table_exists('serie_details'):
        my_logger.info('creating serie_details table')
        _sql_command = \
            f'''
        CREATE TABLE serie_details (imdb_id TEXT NOT NULL PRIMARY KEY, title TEXT, original_title TEXT, score FLOAT, voters INT, plot TEXT,
        poster TEXT, rated TEXT, genre TEXT, media_type TEXT, release_date TEXT, countries TEXT, actors TEXT, creator TEXT, runtime TEXT, years TEXT, seasons TEXT)'''
        serie_details_result = database_excute_command(_sql_command)


def check_item_exists(_imdb_id):
    sql_command = f"""SELECT count(*)
                    FROM movie_details movie
                    INNER JOIN serie_details serie
                    ON movie.imdb_id = serie.imdb_id
                    WHERE (movie.imdb_id like '{_imdb_id}') OR (serie.imdb_id like '{_imdb_id}')
                    """

    _command = f''' SELECT title from movie_details where imdb_id = "{_imdb_id}" '''
    _row = database_excute_command(_command, 'fetch_one')
    if _row:
        return _row[0]
    else:
        _command = f''' SELECT title from serie_details where imdb_id = "{_imdb_id}" '''
        _row = database_excute_command(_command, 'fetch_one')
        if _row:
            return _row[0]
        else:
            return False


def list_to_string(_list):
    formatted_string = _list[0]
    formatted_string = [formatted_string.join(
        f', {item}') for item in _list[1:]]
    return formatted_string


def get_dataframe(_query):
    connection = sqlite3.connect(DATABASE_LOCATION)
    data = pd.read_sql_query(_query, connection)
    connection.close()
    return data


def clean_text(_text):
    cleaned_text = _text.strip().replace('\n', '')
    cleaned_text = cleaned_text.replace('"', '')
    cleaned_text = cleaned_text.replace(';', '')
    cleaned_text = cleaned_text.replace(':', '')
    cleaned_text = cleaned_text.replace('\xa0', '')
    cleaned_text = cleaned_text.replace('&amp;', '&')
    cleaned_text = cleaned_text.replace('&amp', '&')
    cleaned_text = cleaned_text.replace("""&quot""", '')
    cleaned_text = cleaned_text.replace('&apos;', "\'")
    cleaned_text = cleaned_text.replace('&apos', "\'")
    cleaned_text = cleaned_text.replace('             EN', '')
    cleaned_text = cleaned_text.replace('See full summary»', '').replace(
        "'", '').strip()
    return cleaned_text


def get_imdb_id(_link):
    return re.search('https://www.imdb.com/title/(.{10}?|.{9})', _link).group(1)


def get_countries(_soup):
    clean_countries = []
    _css_selectors = ['section.ipc-page-section:nth-child(28) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)',
                      'section.ipc-page-section:nth-child(32) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)',
                      'section.ipc-page-section:nth-child(33) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)',
                      'section.ipc-page-section:nth-child(34) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)',
                      'section.ipc-page-section:nth-child(35) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)',
                      'section.ipc-page-section:nth-child(36) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)',
                      'section.ipc-page-section:nth-child(37) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)',
                      'section.ipc-page-section:nth-child(40) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)',
                      'section.ipc-page-section:nth-child(41) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)',
                      'section.ipc-page-section:nth-child(44) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)',
                      'section.ipc-page-section:nth-child(45) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)',
                      'section.ipc-page-section:nth-child(46) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)'

                      ]
    for _css_selector in _css_selectors:
        try:
            ul = _soup.select_one(_css_selector)
            items = ul.find_all("li")
            clean_countries = [item.text for item in items]
            if 'Color' in clean_countries or 'Black and White' in clean_countries or '$' in clean_countries[0]:
                clean_countries = ['NA']
                continue
        except AttributeError:
            clean_countries = []
            continue
    if 'NA' in clean_countries or not clean_countries:
        try:
            countries = re.search(
                '(?<=\"countriesOfOrigin\":{\"countries\":)(.*)(?=,\"__typename\":\"CountriesOfOrigin\"},\"detailsExternalLinks\")',
                str(_soup)).group(1)
            countries = ast.literal_eval(countries)
            clean_countries = [country['text'] for country in countries]
        except AttributeError:
            clean_countries = ['NA']
        except SyntaxError:
            countries = re.search(
                '(?<=\"countriesOfOrigin\":{\"countries\":)(.*)(?=,\"__typename\":\"CountryOfOrigin\")',
                str(countries)).group(1)

            countries = str(f'{countries}' + '}]')
            """my_logger.info('#######################################################################')
            my_logger.info('countries', countries)
            my_logger.info('#######################################################################')"""
            try:
                countries = ast.literal_eval(countries)
            except SyntaxError:
                countries = re.search(
                    '(?<=\"countriesOfOrigin\":{\"countries\":)(.*)(?=\])',
                    str(countries)).group(1)
                countries = str(f'{countries}' + ']')
            try:
                countries = ast.literal_eval(str(countries))
                clean_countries = [country['text'] for country in countries]
            except KeyError:
                clean_countries = ['NA']
            except TypeError:
                my_logger.warn('TypeError countries', countries)
                raise Exception('TypeError')
            except SyntaxError:
                my_logger.warn('SyntaxError countries', countries)
                raise Exception('SyntaxError')
    if not clean_countries:
        clean_countries = ['NA']
    return ', '.join(clean_countries)


def get_image_full_size(_image):
    _image = re.search(
        '(https://m\.media-amazon\.com/images/M.*?\.)', _image).group(1)
    return f'{_image}jpg'


def get_title(_soup):
    _title = 'NA'
    _css_selector = '.TitleHeader__TitleText-sc-1wu6n3d-0'
    try:
        _title = _soup.select_one(_css_selector).text
    except AttributeError:
        my_logger.warn(f'get_title AttributeError')
    return clean_text(_title)


def get_score(_media_info):
    try:
        _score = float(_media_info['aggregateRating']['ratingValue'])
    except KeyError:
        _score = -1
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
    try:
        _plot = clean_text(_media_info['description'])
    except KeyError:
        _plot = 'NA'
    return _plot


def get_creators(_soup):
    _css_selectors = ['.PrincipalCredits__PrincipalCreditsPanelWideScreen-sc-hdn81t-0 > ul:nth-child(1) > li:nth-child(1) > div:nth-child(2) > ul:nth-child(1)',
                      '.PrincipalCredits__PrincipalCreditsPanelWideScreen-hdn81t-0 > ul:nth-child(1) > li:nth-child(1) > div:nth-child(2) > ul:nth-child(1)',
                      ]
    for _css_selector in _css_selectors:
        try:
            ul = _soup.select_one(_css_selector)
            li = ul.find_all("li")
            _creators = [item.find('a').text for item in li]
            return ', '.join(_creators)
        except AttributeError:
            _creators = ['NA']
            continue
    return ', '.join(_creators)


def clean_creator(_creator):
    person_creator = []
    for _creator in _creator:
        try:
            if _creator['@type'] == 'Person':
                person_creator.append(_creator["name"])
        except KeyError:
            my_logger.warn('This is an Organization')
    if person_creator:
        return list_to_string(person_creator)
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
            _cleaned_season = _seasons.replace(' seasons', '').replace(
                ' season', '').replace(' Seasons', '').replace(' Season', '').strip()
            return int(_cleaned_season)
        except AttributeError:
            return 'NA'


def get_series_runtime(_soup):
    _runtime = 'NA'
    try:
        _runtime = _soup.select_one(
            '.TitleBlockMetaData__MetaDataList-sc-12ein40-0 > li:nth-child(4)').text
    except AttributeError:
        try:
            _runtime = _soup.select_one(
                '.TitleBlockMetaData__MetaDataList-sc-12ein40-0 > li:nth-child(3)').text
        except AttributeError:
            return 'NA'
    return _runtime.strip()


def _get_actors_helper(_soup, _css_selector):
    ul = _soup.select_one(_css_selector)
    li = ul.find_all("li")
    _actors = [item.find('a').text for item in li]
    return _actors


def get_actors(_soup, is_series=False):
    _movie_css_selectors = ['.PrincipalCredits__PrincipalCreditsPanelWideScreen-hdn81t-0 > ul:nth-child(1) > li:nth-child(1) > div:nth-child(2) > ul:nth-child(1)',
                            '.PrincipalCredits__PrincipalCreditsPanelWideScreen-hdn81t-0 > ul:nth-child(1) > li:nth-child(3) > div:nth-child(2) > ul:nth-child(1)',
                            '.PrincipalCredits__PrincipalCreditsPanelWideScreen-sc-hdn81t-0 > ul:nth-child(1) > li:nth-child(3) > div:nth-child(2) > ul:nth-child(1)',
                            '.PrincipalCredits__PrincipalCreditsPanelWideScreen-sc-hdn81t-0 > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)'
                            ]
    _serie_css_selector = '.PrincipalCredits__PrincipalCreditsPanelWideScreen-hdn81t-0 > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)'
    _current_css_selector = _movie_css_selectors
    if is_series:
        _current_css_selector = [_serie_css_selector]
    for _css_selector in _current_css_selector:
        try:
            _actors = _get_actors_helper(_soup, _css_selector)
            return ', '.join(_actors)
        except AttributeError:
            _actors = ['NA']
            continue
    return ', '.join(_actors)


def get_series_years(_soup):
    try:
        _year = (_soup.select_one(
            '.TitleBlockMetaData__MetaDataList-sc-12ein40-0 > li:nth-child(2)').text).strip()
        return _year[:len(_year) // 2]
    except AttributeError:
        return 'NA'


def get_genres(_media_info, _soup):
    _genres = []
    _flag = False
    try:
        _genres = ', '.join(_media_info['genres'])
        return _genres
    except KeyError:
        try:
            ul = _soup.select_one(
                'ul.ipc-metadata-list:nth-child(4) > li:nth-child(1) > div:nth-child(2)')
            items = ul.find_all("li")
            for item in items:
                if len(item.text) > 12 or '"' in item.text:
                    _flag = True
                _genres.append(item.text)
            if _flag:
                _genres = []
                try:
                    ul = _soup.select_one(
                        'ul.ipc-metadata-list:nth-child(4) > li:nth-child(2) > div:nth-child(2)')
                    items = ul.find_all("li")
                    _genres = [item.text for item in items]
                except AttributeError:
                    return 'NA'
            return ', '.join(_genres)
        except AttributeError:
            return 'NA'


def get_voters(_media_info, _soup):
    try:
        return int(_media_info['aggregateRating']['ratingCount'])
    except KeyError:
        try:
            _div = _soup.select_one(
                'ul.ipc-metadata-list:nth-child(4) > li:nth-child(2) > div:nth-child(2)')
            return int(_div.text)
        except AttributeError:
            return 'NA'
        except ValueError:
            return 'NA'


def get_release_date(_media_info, _soup):
    _release_date = 'NA'
    try:
        _release_date = _media_info['datePublished']
    except KeyError:
        try:
            _div = _soup.select_one(
                '.TitleBlockMetaData__MetaDataList-sc-12ein40-0 > li:nth-child(1) > a:nth-child(1)')
            _release_date = _div.text
        except AttributeError:
            return _release_date
    return _release_date


def get_rated(_media_info, _soup):
    _rated = 'NA'
    try:
        _rated = _media_info['contentRating']
    except KeyError:
        try:
            _div = _soup.select_one(
                'ul.ipc-inline-list--show-dividers:nth-child(2) > li:nth-child(3) > a:nth-child(1)')
            _rated = _div.text
        except AttributeError:
            return _rated
    return _rated


def get_details(_link):
    imdb_id = get_imdb_id(_link)
    try:
        soup, media_info = get_media_info(_link)
    except TypeError:
        return False
    if not media_info:
        return False
    media_type = media_info['@type']
    if 'TVEpisode' in media_type:
        my_logger.info(f'{imdb_id}: {media_type} Skipped')
        return False
    title = get_title(soup)
    original_title = clean_text(media_info['name'])
    voters = get_voters(media_info, soup)
    rated = get_rated(media_info, soup)
    release_date = get_release_date(media_info, soup)
    poster = get_poster(media_info)
    countries = get_countries(soup)
    score = get_score(media_info)
    plot = get_plot(media_info)
    genre = get_genres(media_info, soup)

    match media_type:
        case 'TVSeries':
            media_type = 'TV Series'
            actors = get_actors(soup, True)
            seasons = get_seasons(soup)
            runtime = get_series_runtime(soup)
            years = get_series_years(soup)
            try:
                creator = clean_creator(media_info['creators'])
            except KeyError:
                creator = get_creators(soup)
            if release_date == 'NA':
                if years != 'NA':
                    release_date = years.split('-')[0]

            imdb_serie = ImdbSerie(imdb_id, title, original_title, score, voters, plot, poster,
                                   rated, genre, media_type, release_date, countries, actors, creator, runtime, years,
                                   seasons)
            return imdb_serie
        case 'Movie':
            actors = get_actors(soup)
            try:
                director = clean_creator(media_info['directors'])
            except KeyError:
                director = get_creators(soup)
            try:
                runtime = media_info['duration'].replace(
                    'PT', '').replace('H', 'h').replace('M', 'm').lower()
            except KeyError:
                runtime = 'NA'
            imdb_movie = ImdbMovie(imdb_id, title, original_title, score, voters, plot,
                                   poster, rated, genre, media_type, release_date, countries, actors, director, runtime)
            return imdb_movie
        case _:
            return False


def add_to_database(_media: Imdb):
    _insert_command = _media.insertion_command()
    return database_excute_command(_insert_command)


def update_media(_reddit_goal, _table_name):
    # todo, change the behavior create some sort of an interface that can take either movie or serie and handle how to update insert etc...
    _insert_command = f"UPDATE {_table_name} SET direct_url = '{_reddit_goal.direct_url}' WHERE {_table_name}.id = '{_reddit_goal.id}' "
    return database_excute_command(_insert_command)


def show_all():
    connection = sqlite3.connect(DATABASE_LOCATION)
    db = connection.cursor()
    _rows = db.execute(
        f''' SELECT * from movie_details inner join serie_details on movie_details.title = serie_details.title''').fetchall()
    connection.commit()
    my_logger.info(_rows)
    connection.close()


def mongodb_format():
    connection = sqlite3.connect(DATABASE_LOCATION)
    _cursor = connection.cursor()
    # This enables column access by name: row['column_name']
    connection.row_factory = sqlite3.Row
    _rows = _cursor.execute(
        f''' SELECT * from movie_details inner join serie_details on movie_details.title = serie_details.title''').fetchall()
    connection.commit()
    with open('data.json', 'a+') as outfile:
        for _row in _rows:
            imdb_movie = dict(
                (_cursor.description[i][0], value) for i, value in enumerate(_row))
            imdb_movie['_class'] = 'com.back_sync.models.Imdb'
            json.dump(imdb_movie, outfile)
            outfile.write('\n')
    _cursor.close()
    connection.close()


def get_json_data(_imdb_id):
    connection = sqlite3.connect(DATABASE_LOCATION)
    _cursor = connection.cursor()
    # This enables column access by name: row['column_name']
    connection.row_factory = sqlite3.Row
    _rows = _cursor.execute(
        f''' SELECT * from details where imdb_id = "{_imdb_id}" ''').fetchall()
    connection.commit()
    _cursor.close()
    connection.close()
    return [dict(_row) for _row in _rows]  # CREATE JSON


def delete_item_by_id(_imdb_id):
    connection = sqlite3.connect(DATABASE_LOCATION)
    _cursor = connection.cursor()
    # This enables column access by name: row['column_name']
    connection.row_factory = sqlite3.Row
    _rows = _cursor.execute(
        f''' delete from details where imdb_id = "{_imdb_id}" ''')
    connection.commit()
    if _rows:
        my_logger.info(f'{_imdb_id} deleted')
    _cursor.close()
    connection.close()


def seed_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def shuffle_list(_list_to_shuffle):
    _seed = seed_generator()
    random.seed(_seed)
    random.shuffle(_list_to_shuffle)
    return _list_to_shuffle


def temp_id_list():
    query = f'SELECT imdb_id FROM movie_details'
    movies_id = database_excute_command(query, 'fetch_all')
    query = f'SELECT imdb_id FROM serie_details'
    series_id = database_excute_command(query, 'fetch_all')
    result = movies_id + series_id
    with open('db_ids.txt', 'w', encoding='utf-8') as file:
        strint_to_write = ", ".join(str(f'"{item[0]}"') for item in result)
        file.write(f'[{strint_to_write}]')
        write_imdb_id(ast.literal_eval(f'[{strint_to_write}]'))


def single_scrape(imdb_id):
    set_up_database()
    imdb_base_path = 'https://www.imdb.com/title/'
    details = get_details(f'{imdb_base_path}{imdb_id}')
    if details:
        insertion_details = add_to_database(details)
    if insertion_details:
        my_logger.info(f'{details.title} Added to database')


def main(imdb_ids):
    imdb_base_path = 'https://www.imdb.com/title/'
    # id_test = ['tt1345836', 'tt0482571', 'tt1375666', 'tt2084970', 'tt0756683']
    # id_test = ['tt0002610', 'tt0372784', 'tt0903747']
    # id_test = ['tt0172495', 'tt0107290', 'tt0046912']
    loop_counter = 1
    _movies_added = 0
    _series_added = 0
    _database_counter = 0
    _programe_pause_counter = 0
    _list_original_lenght = len(imdb_ids)
    set_up_database()
    for imdb_id in imdb_ids[:]:
        insertion_details = False
        if _programe_pause_counter >= 1000:
            my_logger.warn('Program paused for 2 min')
            time.sleep(120)
            my_logger.warn('Program restarting')
            _programe_pause_counter = 0
        if _database_counter >= 20:
            write_imdb_id(imdb_ids)
            _database_counter = 0
        start_time = time.time()
        my_logger.info(imdb_id)
        my_logger.info(
            f'{loop_counter}/{_list_original_lenght} - movies added: {_movies_added}, series added: {_series_added}')
        _item = check_item_exists(imdb_id)
        if _item:
            my_logger.info(f'{_item} found')
            imdb_ids.remove(imdb_id)
        else:
            details = get_details(f'{imdb_base_path}{imdb_id}')
            if details:
                match details.media_type:
                    case 'TV Series':
                        insertion_details = add_to_database(details)
                        _series_added += 1
                        _programe_pause_counter += 1
                    case 'Movie':
                        insertion_details = add_to_database(details)
                        _movies_added += 1
                        _programe_pause_counter += 1
                    case _:
                        my_logger.warn('Unknown type', details.type)
            else:
                imdb_ids.remove(imdb_id)

            if insertion_details:
                my_logger.info(f'{details.title} Added to database')
                imdb_ids.remove(imdb_id)

        loop_counter += 1
        _database_counter += 1
        my_logger.info(f"--- {(time.time() - start_time)} seconds ---")


if __name__ == '__main__':
    imdb_ids = get_imdb_ids_dump()
    main(imdb_ids)
    # temp_id_list()
