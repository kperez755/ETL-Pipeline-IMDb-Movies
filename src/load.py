import psycopg2
from psycopg2 import sql
import pandas as pd
import os

## Correct import
from configs.config import DB_CONFIG

## Logger
from utils.get_logger import get_logger
import logging


logger = get_logger(
    name='ETL',
    log_file='logs/etl',
    level=logging.INFO

)


def create_schema(cur):


    ## Drop everything
    logger.info("Dropping tables if exist")

    cur.execute("""
        DROP TABLE IF EXISTS movie_languages CASCADE;
        DROP TABLE IF EXISTS movie_country CASCADE;
        DROP TABLE IF EXISTS movie_actors CASCADE;
        DROP TABLE IF EXISTS movie_writers CASCADE;
        DROP TABLE IF EXISTS movie_directors CASCADE;

        DROP TABLE IF EXISTS languages CASCADE;
        DROP TABLE IF EXISTS countries CASCADE;
        DROP TABLE IF EXISTS "cast" CASCADE;
        DROP TABLE IF EXISTS writers CASCADE;
        DROP TABLE IF EXISTS directors CASCADE;

        DROP TABLE IF EXISTS movies_cleaned CASCADE;
    """)
    ## Movie Cleaned

    logger.info("Creating movies_cleaned")
    cur.execute("""
        CREATE TABLE movies_cleaned (
            id INT PRIMARY KEY,
            title TEXT,
            average_rating FLOAT,
            budget NUMERIC,
            worldwide_gross NUMERIC,
            runtime INT
        );
    """)

    ##Look up tables
    logger.info("Creating Look up tables")
    cur.execute("""
        CREATE TABLE directors (
            director_id INT PRIMARY KEY,
            director TEXT NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE writers (
            writer_id INT PRIMARY KEY,
            writer TEXT NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE "cast" (
            cast_id INT PRIMARY KEY,
            cast_name TEXT NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE countries (
            country_id INT PRIMARY KEY,
            country TEXT NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE languages (
            language_id INT PRIMARY KEY,
            language TEXT NOT NULL
        );
    """)

    ## Junction
    logger.info("Creating junction tables")
    cur.execute("""
        CREATE TABLE movie_directors (
            id INT,
            director_id INT,
            PRIMARY KEY (id, director_id),
            FOREIGN KEY (id) REFERENCES movies_cleaned(id) ON DELETE CASCADE,
            FOREIGN KEY (director_id) REFERENCES directors(director_id) ON DELETE CASCADE
        );
    """)

    cur.execute("""
        CREATE TABLE movie_writers (
            id INT,
            writer_id INT,
            PRIMARY KEY (id, writer_id),    
            FOREIGN KEY (id) REFERENCES movies_cleaned(id) ON DELETE CASCADE,
            FOREIGN KEY (writer_id) REFERENCES writers(writer_id) ON DELETE CASCADE
        );
    """)

    cur.execute("""
        CREATE TABLE movie_actors (
            id INT,
            cast_id INT,
            PRIMARY KEY (id, cast_id),
            FOREIGN KEY (id) REFERENCES movies_cleaned(id) ON DELETE CASCADE,
            FOREIGN KEY (cast_id) REFERENCES "cast"(cast_id) ON DELETE CASCADE
        );
    """)

    cur.execute("""
        CREATE TABLE movie_country (
            id INT,
            country_id INT,
            PRIMARY KEY (id, country_id),
            FOREIGN KEY (id) REFERENCES movies_cleaned(id) ON DELETE CASCADE,
            FOREIGN KEY (country_id) REFERENCES countries(country_id) ON DELETE CASCADE
        );
    """)

    cur.execute("""
        CREATE TABLE movie_languages (
            id INT,
            language_id INT,
            PRIMARY KEY (id, language_id),
            FOREIGN KEY (id) REFERENCES movies_cleaned(id) ON DELETE CASCADE,
            FOREIGN KEY (language_id) REFERENCES languages(language_id) ON DELETE CASCADE
        );
    """)



def load_csv(cur, table_name, csv_path):

    df = pd.read_csv(csv_path)

    ## automapping csv
    logger.info("automapping CSV")
    columns = ", ".join([f'"{c}"' for c in df.columns])

    with open(csv_path, "r", encoding="utf-8") as f:
        cur.copy_expert(
            sql.SQL("COPY {} ({}) FROM STDIN WITH CSV HEADER")
            .format(sql.Identifier(table_name), sql.SQL(columns)),
            f
        )



def load_data():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    create_schema(cur)

    ## FACT Table
    logger.info("Loading Fact table")
    load_csv(cur, "movies_cleaned", "data/processed/movies_cleaned.csv")

    ## LOOKUP Tables
    logger.info("Loading Look up tables")
    load_csv(cur, "directors", "data/processed/normalized/directors.csv")
    load_csv(cur, "writers", "data/processed/normalized/writers.csv")

    ## Fix cast column naming
    logger.info("fix cast column naming")
    cast_df = pd.read_csv("data/processed/normalized/cast.csv")
    cast_df.rename(columns={"cast": "cast_name"}, inplace=True)
    cast_df.to_csv("data/processed/normalized/cast_fixed.csv", index=False)
    load_csv(cur, "cast", "data/processed/normalized/cast_fixed.csv")

    load_csv(cur, "countries", "data/processed/normalized/countries.csv")
    load_csv(cur, "languages", "data/processed/normalized/languages.csv")

    ## Junction Tables
    logger.info("Loading Junction tables")
    load_csv(cur, "movie_directors", "data/processed/normalized/movie_directors.csv")
    load_csv(cur, "movie_writers", "data/processed/normalized/movie_writers.csv")
    load_csv(cur, "movie_actors", "data/processed/normalized/movie_actors.csv")
    load_csv(cur, "movie_country", "data/processed/normalized/movie_country.csv")
    load_csv(cur, "movie_languages", "data/processed/normalized/movie_languages.csv")

    conn.commit()
    cur.close()
    conn.close()