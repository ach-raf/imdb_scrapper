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

PATH_TO_CHROME_DRIVER = r'chromedriver'

# options for selenium
options = Options()
options.add_argument('--headless')
options.add_argument("--incognito")
options.headless = True

browser = webdriver.Chrome(
    executable_path=PATH_TO_CHROME_DRIVER, options=options)

database_location = 'imdb.db'
CONNECTION = sqlite3.connect(database_location)
CURSOR = CONNECTION.cursor()

GENRES = ['Animation', 'Action', 'Adventure']


def set_up_database():
    if not check_table_exists('details'):
        print('creating details table')
        _sql_command = \
            f''' 
        CREATE TABLE details (imdb_id TEXT NOT NULL  PRIMARY KEY, title TEXT, score TEXT, voters TEXT, plot TEXT, 
        poster TEXT, director Text, rated TEXT, runtime TEXT, genre TEXT, year TEXT, type TEXT, seasons TEXT) '''
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
        f''' SELECT title from details where imdb_id = "{_imdb_id}" ''').fetchone()
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


def get_title(_soup):
    try:
        _title = _soup.select_one("""
            html body#styleguide-v2.fixed div#wrapper div#root.redesign div#pagecontent.pagecontent 
            div#content-2-wide.flatland div#main_top.main div.title-overview div#title-overview-widget.heroic-overview 
            div.vital div.title_block div.title_bar_wrapper div.titleBar div.title_wrapper h1
            """).text
    except AttributeError:
        _title = 'NA'

    return clean_text(_title)


def get_score(_soup):
    _score = 'NA'
    try:
        _score = clean_text(
            _soup.select_one("""html body#styleguide-v2.fixed div#wrapper div#root.redesign div#pagecontent.pagecontent 
            div#content-2-wide.flatland div#main_top.main div.title-overview div#title-overview-widget.heroic-overview 
            div.vital div.title_block div.title_bar_wrapper div.ratings_wrapper div.imdbRating div.ratingValue strong 
            span""").text)
    except AttributeError:
        pass
    return _score


def get_poster(_soup):
    _poster = 'No image Found'
    try:
        _poster = clean_text(
            _soup.select_one("""html body#styleguide-v2.fixed div#wrapper div#root.redesign div#pagecontent.pagecontent 
            div#content-2-wide.flatland div#main_top.main div.title-overview div#title-overview-widget.heroic-overview 
            div.posterWithPlotSummary div.poster a img""")['src'])
    except TypeError:
        pass
    try:
        _poster = clean_text(
            _soup.select_one("""html body#styleguide-v2.fixed div#wrapper div#root.redesign div#pagecontent.pagecontent 
            div#content-2-wide.flatland div#main_top.main div.title-overview div#title-overview-widget.heroic-overview 
            div.vital div.slate_wrapper div.poster a img""")['src'])
    except AttributeError:
        pass
    except TypeError:
        pass
    try:
        _poster = clean_text(
            _soup.select_one("""html body#styleguide-v2.fixed div#wrapper div#root.redesign div#pagecontent.pagecontent 
            div#content-2-wide.flatland div#main_top.main div.title-overview div#title-overview-widget.heroic-overview 
            div.posterWithPlotSummary div.poster a img""")['src'])
    except AttributeError:
        pass
    except TypeError:
        pass
    if 'https' in _poster:
        return get_image_full_size(_poster)
    else:
        return _poster


def get_plot(_soup):
    try:
        _plot = clean_text(
            _soup.select_one('html body#styleguide-v2.fixed div#wrapper div#root.redesign div#pagecontent.pagecontent '
                             'div#content-2-wide.flatland div#main_top.main div.title-overview '
                             'div#title-overview-widget.heroic-overview div.plot_summary_wrapper '
                             'div.plot_summary div.summary_text').text)
    except AttributeError:
        _plot = 'NA'
    return _plot.replace(',', ';')


def get_voters(_soup):
    try:
        _voters = clean_text(_soup.select_one('span.small').text)
    except AttributeError:
        _voters = 'NA'
    return _voters.replace(',', '.')


def get_director(_soup):
    _director = 'NA'
    try:
        _director = clean_text(_soup.select_one("""html body#styleguide-v2.fixed div#wrapper div#root.redesign 
                                                div#pagecontent.pagecontent div#content-2-wide.flatland 
                                                div#main_top.main div.title-overview 
                                                div#title-overview-widget.heroic-overview div.plot_summary_wrapper 
                                                div.plot_summary div.credit_summary_item a""").text)
    except AttributeError:
        pass
    return _director


def get_seasons_year_info(_soup):
    _seasons_year_info = _soup.select("""html body#styleguide-v2.fixed div#wrapper div#root.redesign 
                                        div#pagecontent.pagecontent div#content-2-wide.flatland div#main_bottom.main 
                                        div.article div#title-episode-widget.table.full-width div.seasons-and-year-nav 
                                        div""")
    return _seasons_year_info


def get_information(information, _is_series):
    imdb_info_step = 0
    _rated = 'NA'
    _runtime = 'NA'
    _genre = 'NA'
    _release_date = 'NA'
    for info in information:
        if imdb_info_step == 0 and 'min' in info:
            imdb_info_step = 1
        """if imdb_info_step == 0 and 'Series' in info:
            imdb_info_step = 2"""
        if imdb_info_step == 0 and 'Animation' in info:
            imdb_info_step = 2
        if imdb_info_step == 0 and 'Comedy' in info:
            imdb_info_step = 2
        if imdb_info_step == 0 and 'Music' in info:
            imdb_info_step = 2
        if imdb_info_step == 0 and 'Drama' in info:
            imdb_info_step = 2
        if imdb_info_step == 0:
            _rated = clean_text(info)
        if imdb_info_step == 1:
            _runtime = clean_text(info)
        if imdb_info_step == 2:
            _genre = clean_text(info)
        if imdb_info_step == 3:
            _release_date = clean_text(info)
            if 'Series' in _release_date:
                _is_series = True
        imdb_info_step += 1
    return _rated, _runtime, _genre.replace(',', ' '), _release_date, _is_series


def get_type(_is_series):
    if _is_series:
        return 'TV Series'
    else:
        return 'Movie'


def get_seasons_years(_seasons_year_info):
    seasons = []
    years = []
    seasons_and_years_step = 0
    for season in _seasons_year_info[2:]:
        for a in season.find_all('a'):
            if seasons_and_years_step == 0:
                try:
                    seasons.append(int(a.text))
                except ValueError:
                    pass
            if seasons_and_years_step == 1:
                try:
                    years.append(int(a.text))
                except ValueError:
                    pass
        seasons_and_years_step += 1
    return seasons, years


def get_year(_soup):
    _year = 'NA'
    try:
        _year = _soup.select_one(
            """html body#styleguide-v2.fixed div#wrapper div#root.redesign div#pagecontent.pagecontent 
            div#content-2-wide.flatland div#main_top.main div.title-overview div#title-overview-widget.heroic-overview 
            div.vital div.title_block div.title_bar_wrapper div.titleBar div.title_wrapper h1 span#titleYear a""").text
    except AttributeError:
        pass
    return _year


def get_details(_link):
    print(_link)
    is_series = False
    imdb_info = []
    _imdb_id = get_imdb_id(_link)
    imdb_info.append({'imdb_id': _imdb_id})
    soup = get_html(_link)
    _script = soup.find('script', type='application/ld+json')
    print(f'script: {_script}')
    with open('script.js', 'w', encoding='utf-8') as f:
        f.write(f'{_script}')
    return 0
    # title = clean_text(soup.select_one('.title_wrapper > h1:nth-child(1)').text)
    title = get_title(soup)
    score = get_score(soup)
    plot = get_plot(soup)
    voters = get_voters(soup)
    director = get_director(soup)
    information = clean_details_information(soup)
    rated, runtime, genre, release_date, is_series = get_information(
        information, is_series)
    poster = get_poster(soup)
    seasons_year_info = get_seasons_year_info(soup)
    _type = get_type(is_series)
    _seasons = '-1'
    year = 'NA'
    if is_series:
        seasons, years = get_seasons_years(seasons_year_info)
        try:
            _seasons = f'{max(seasons)}'
            year = f'{min(years)} - {max(years)}'
        except ValueError:
            pass
    else:
        year = get_year(soup)
        _year = get_year(soup)

    imdb_info.append({'title': title})
    imdb_info.append({'score': score})
    imdb_info.append({'voters': voters})
    imdb_info.append({'plot': plot})
    imdb_info.append({'poster': poster})
    if 'Series' in genre:
        is_series = True
        _seasons = 1
        _type = 'TV Series'
    if not is_series:
        title = title[:-6]
    insertion_details = CURSOR.execute(
        f'INSERT INTO details VALUES ("{_imdb_id}", "{title}" , "{score}", "{voters}", "{plot}", "{poster}", "{director}"'
        f', "{rated}", "{runtime}", "{genre}", "{year}", "{_type}", "{_seasons}" )')
    CONNECTION.commit()
    print(f'{title} Added to database')
    """if seasons:
        print(seasons, years)"""


def show_all():
    db = CONNECTION.cursor()
    _rows = db.execute(f''' SELECT * from details ''').fetchall()
    CONNECTION.commit()
    print(_rows)


def mongodb_format():
    # This enables column access by name: row['column_name']
    CONNECTION.row_factory = sqlite3.Row
    _rows = CURSOR.execute(f''' SELECT * from details''').fetchall()
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
    id_test = ['tt1345836']

    loop_counter = 1
    set_up_database()
    ids.extend(more_ids)
    for imdb_id in id_test:
        print(f'{loop_counter}/{len(id_test)}')
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
