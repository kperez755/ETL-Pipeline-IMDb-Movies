from src.extract import extract_data
import pandas as pd
import time
import os
from typing import Tuple
import re

## Logger
from utils.get_logger import get_logger
import logging

logger = get_logger(
    name='ETL',
    log_file='logs/etl',
    level=logging.INFO
)

def normalize_runtime(runtime_str):

    hours = 0
    minutes = 0

    ## Extracting hours and minutes
    hour_match = re.search(r"(\d+)\s*hour", runtime_str)
    if hour_match:
        hours = int(hour_match.group(1))

    minute_match = re.search(r"(\d+)\s*minute", runtime_str)
    if minute_match:
        minutes = int(minute_match.group(1))

    return hours * 60 + minutes




def normalize_column(
    df: pd.DataFrame, 
    column_name: str, 
    lookup_name: str,
    id_column: str = 'Id'
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    

    logger.info("Normalization has started")
    if column_name not in df.columns:
        raise KeyError(
            f"Column '{column_name}' not found. "
            f"Available columns: {df.columns.tolist()}"
        )
        
    exploded = (
        df[[id_column, column_name]]
        .assign(values=df[column_name].str.split(',\\s*'))
        .explode('values')
        .dropna(subset=['values'])
    )
    exploded['values'] = exploded['values'].str.strip()

    ## Lookup table
    
    lookup = exploded['values'].drop_duplicates().reset_index(drop=True)
    lookup_df = pd.DataFrame({
        f'{lookup_name}_id': lookup.index,
        lookup_name: lookup
    })

    ## Junction table
    junction_df = exploded.merge(
        lookup_df, left_on='values', right_on=lookup_name
    )[[id_column, f'{lookup_name}_id']]
    
    logger.info("Renaming columns for consistency")

    ## Renaming id column 
    junction_df = junction_df.rename(columns={id_column: 'id'})

    return lookup_df, junction_df


def transform():

    logger.info("Transformation started")
    df = extract_data('data/raw/movies.csv')
     
    ## Convert Release Date
    df['Release Date'] = pd.to_datetime(
        df['Release Date'], format='%Y-%m-%d', errors='coerce'
    )
    
    logger.info("Dropping columns")

    ## Drop columns
    columns_to_drop = ['Metascore','Release Date']

    dropped_columns_df = df[columns_to_drop].copy()
    os.makedirs('data/processed', exist_ok=True)
    dropped_columns_df.to_csv('data/processed/dropped_columns.csv', index=False)
    
    
    ## Drop rows with missing Writer
    logger.info("Dropping rows")
    rows_to_drop = df[df['Writer'].isna()].copy()
    rows_to_drop.to_csv('data/processed/dropped_rows.csv', index=False)

    logger.info("Dropping rows")
    rows_to_drop = df[df['Country of Origin'].isna()].copy()
    rows_to_drop.to_csv('data/processed/dropped_rows.csv', index=False)

    logger.info("Dropping rows")
    rows_to_drop = df[df['Cast'].isna()].copy()
    rows_to_drop.to_csv('data/processed/dropped_rows.csv', index=False)

    logger.info("Dropping rows")
    rows_to_drop = df[df['Country of Origin'].isna()].copy()
    rows_to_drop.to_csv('data/processed/dropped_rows.csv', index=False)

    df_cleaned = df.drop(columns=columns_to_drop)
    df_cleaned = df_cleaned.drop(rows_to_drop.index)

    ## Remove duplicate movies
    logger.info("Removing duplicates")
    df_cleaned = df_cleaned.drop_duplicates(subset=['Title'], keep='first')
    df_cleaned = df_cleaned.drop_duplicates(subset=['Id'], keep='first')

    ## Convert Budget
    logger.info("Converting budget")
    df_cleaned['Budget'] = pd.to_numeric(
        df_cleaned['Budget'].astype(str).str.replace(r'[^\d.]', '', regex=True),
        errors='coerce'
    )

    ## Convert Date to numeric
    logger.info("Converting Date to Numeric")
    df_cleaned["Runtime"] = df_cleaned["Runtime"].apply(normalize_runtime)

    ## Convert Worldwide Gross
    logger.info("Converting Worldwide Gross")
    df_cleaned['Worldwide Gross'] = pd.to_numeric(
        df_cleaned['Worldwide Gross'].astype(str).str.replace(r'[^\d.]', '', regex=True),
        errors='coerce'
    )


    ## Fill missing numeric values
    logger.info("filling in missing numeric values")
    df_cleaned['Budget'] = df_cleaned['Budget'].fillna(df_cleaned['Budget'].median())
    df_cleaned['Worldwide Gross'] = df_cleaned['Worldwide Gross'].fillna(df_cleaned['Worldwide Gross'].median())

    time.sleep(3)
    
    ## Normalize
    logger.info("Normalizing data")
    directors_df, movie_directors_df = normalize_column(df_cleaned, 'Director', 'director', 'Id')
    writers_df, movie_writers_df = normalize_column(df_cleaned, 'Writer', 'writer', 'Id')
    actors_df, movie_actors_df = normalize_column(df_cleaned, 'Cast', 'cast', 'Id')
    countries_df, movie_countries_df = normalize_column(df_cleaned, 'Country of Origin', 'country', 'Id')
    languages_df, movie_languages_df = normalize_column(df_cleaned, 'Languages', 'language', 'Id')

    ## Standardize 
    logger.info("Standardizing data")

    df_cleaned.columns = (
        df_cleaned.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    output_dir = "data/processed/normalized"
    os.makedirs(output_dir, exist_ok=True)

    df_cleaned.to_csv(f"{output_dir}/cleaned.csv", index=False)

    directors_df.to_csv(f"{output_dir}/directors.csv", index=False)
    writers_df.to_csv(f"{output_dir}/writers.csv", index=False)
    actors_df.to_csv(f"{output_dir}/cast.csv", index=False)
    countries_df.to_csv(f"{output_dir}/countries.csv", index=False)
    languages_df.to_csv(f"{output_dir}/languages.csv", index=False)

    movie_directors_df.to_csv(f"{output_dir}/movie_directors.csv", index=False)
    movie_writers_df.to_csv(f"{output_dir}/movie_writers.csv", index=False)
    movie_actors_df.to_csv(f"{output_dir}/movie_actors.csv", index=False)
    movie_countries_df.to_csv(f"{output_dir}/movie_country.csv", index=False)
    movie_languages_df.to_csv(f"{output_dir}/movie_languages.csv", index=False)

    df_cleaned = df_cleaned.drop(columns=[
        'director',
        'writer',
        'cast',
        'country_of_origin',
        'languages'
    ])
    
    ## Save cleaned file
    df_cleaned.to_csv("data/processed/movies_cleaned.csv", index=False)