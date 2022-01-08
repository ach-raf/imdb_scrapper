import os
import urllib.parse
import uvicorn
from fastapi import FastAPI, Request, Form, Query
from databases import Database
from datetime import datetime
from imdb_scrapper import single_scrape
from typing import Optional

app = FastAPI()
CURRENT_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
DATABASE_NAME = "imdb.db"
MOVIES_TABLE = "movie_details"
SERIES_TABLE = "serie_details"
DATABASE_LOCATION = os.path.join(CURRENT_DIR_PATH, DATABASE_NAME)
REDDIT_GOALS_DB = Database(f"sqlite:///{DATABASE_LOCATION}")


@app.on_event("startup")
async def database_connect():
    await REDDIT_GOALS_DB.connect()


@app.on_event("shutdown")
async def database_disconnect():
    await REDDIT_GOALS_DB.disconnect()


@app.get("/")
async def fetch_data():
    query = f"SELECT * FROM {MOVIES_TABLE} ORDER BY voters DESC, score DESC LIMIT 200"
    results = await REDDIT_GOALS_DB.fetch_all(query=query)

    return results


@app.get("/movies")
async def fetch_movies():
    query = f"SELECT * FROM {MOVIES_TABLE} WHERE voters > 10000 and countries NOT LIKE 'IN%' ORDER BY score DESC, voters DESC LIMIT 200"
    movies = await REDDIT_GOALS_DB.fetch_all(query=query)

    return movies


@app.get("/series")
async def fetch_movies():
    query = f"SELECT * FROM {SERIES_TABLE} WHERE voters > 10000 and countries NOT LIKE 'IN%' ORDER BY score DESC, voters DESC LIMIT 200"
    series = await REDDIT_GOALS_DB.fetch_all(query=query)

    return series


@app.get("/api/{imdb_id}")
async def fetch_movies(imdb_id: str):
    query = f'SELECT * FROM {MOVIES_TABLE} WHERE imdb_id like "{imdb_id}"'
    search_result = await REDDIT_GOALS_DB.fetch_all(query=query)
    if not search_result:
        query = f'SELECT * FROM {SERIES_TABLE} WHERE imdb_id like "{imdb_id}"'
        search_result = await REDDIT_GOALS_DB.fetch_all(query=query)
    if not search_result:
        single_scrape(imdb_id)
        query = f'SELECT * FROM {MOVIES_TABLE} WHERE imdb_id like "{imdb_id}"'
        search_result = await REDDIT_GOALS_DB.fetch_all(query=query)
        if not search_result:
            query = f'SELECT * FROM {SERIES_TABLE} WHERE imdb_id like "{imdb_id}"'
            search_result = await REDDIT_GOALS_DB.fetch_all(query=query)
    return search_result


@app.get("/api/search/{title}")
async def fetch_movies(title: str, year: Optional[str] = None):
    if not year:
        query = f'SELECT * FROM {MOVIES_TABLE} WHERE title like "%{title}%" ORDER BY score DESC, voters DESC LIMIT 200'
        search_movies = await REDDIT_GOALS_DB.fetch_all(query=query)
        query = f'SELECT * FROM {SERIES_TABLE} WHERE title like "%{title}%" ORDER BY score DESC, voters DESC LIMIT 200'
        search_series = await REDDIT_GOALS_DB.fetch_all(query=query)
        return search_movies + search_series

    query = f'SELECT * FROM {MOVIES_TABLE} WHERE title like "%{title}%" and strftime("%Y", release_date) like "{year}" ORDER BY score DESC, voters DESC LIMIT 200'
    print(query)
    search_movies = await REDDIT_GOALS_DB.fetch_all(query=query)

    query = f'SELECT * FROM {SERIES_TABLE} WHERE title like "%{title}%" and strftime("%Y", release_date) like "{year}" ORDER BY score DESC, voters DESC LIMIT 200'
    search_series = await REDDIT_GOALS_DB.fetch_all(query=query)
    return search_movies + search_series


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8008)
