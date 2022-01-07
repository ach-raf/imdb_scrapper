from dataclasses import dataclass


@dataclass
class Imdb:
    imdb_id: str
    title: str
    original_title: str
    score: int
    voters: int
    plot: str
    poster: str
    rated: str
    genre: str
    media_type: str
    release_date: str
    countries: str
    actors: str


@dataclass
class ImdbSerie(Imdb):
    creator: str
    runtime: str
    years: str
    seasons: str


@dataclass
class ImdbMovie(Imdb):
    director: str
    runtime: str
