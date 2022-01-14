FROM python:3.10

WORKDIR /imdb_scrapper

COPY requirements.txt .

COPY . .

RUN pip install -r requirements.txt


CMD [ "python", "imdb_scrapper.py"]

