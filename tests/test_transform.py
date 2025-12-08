import pandas as pd
import os
from unittest.mock import patch
from src.transform import transform

def test_transform_full_workflow(tmp_path, monkeypatch):

    ## Fake input DF
    df = pd.DataFrame({
        "Id": [1, 2],
        "Title": ["Movie A", "Movie B"],
        "Metascore": [80, 90],
        "Release Date": ["2020-01-01", "2021-02-02"],
        "Writer": ["John Doe", None],
        "Director": ["Dir A, Dir B", "Dir B"],
        "Cast": ["A1, A2", "A2"],
        "Country of Origin": ["USA, UK", "USA"],
        "Languages": ["English, Spanish", "English"],
        "Budget": ["$1,000", "$2,000"],
        "Runtime": ["1 hour 30 minutes", "2 hours 0 minutes"],
        "Worldwide Gross": ["$10,000", "$20,000"]
    })

    ## Patch extract_data
    with patch("src.transform.extract_data", return_value=df):

        real_makedirs = os.makedirs

        ## Patch output directory string in the module
        def safe_makedirs(path, exist_ok=False):
            rewritten = str(path).replace("data/processed", str(tmp_path))
            return real_makedirs(rewritten, exist_ok=exist_ok)

        monkeypatch.setattr("src.transform.os.makedirs", safe_makedirs)

        real_to_csv = pd.DataFrame.to_csv

        ## Redirect path instead of patching function
        def fixed_to_csv(self, path, index=False):
            rewritten = str(path).replace("data/processed", str(tmp_path))
            real_to_csv(self, rewritten, index=index)

        monkeypatch.setattr(pd.DataFrame, "to_csv", fixed_to_csv)

        ## Run transform
        transform()

    ## Check dropped columns
    dropped_path = tmp_path / "dropped_columns.csv"
    dropped_df = pd.read_csv(dropped_path)
    assert list(dropped_df.columns) == ["Metascore", "Release Date"]

    ## Check dropped rows
    dropped_rows_path = tmp_path / "dropped_rows.csv"
    dropped_rows_df = pd.read_csv(dropped_rows_path)
    assert dropped_rows_df["Writer"].isna().all()

    ## Check cleaned file
    cleaned_path = tmp_path / "movies_cleaned.csv"
    cleaned_df = pd.read_csv(cleaned_path)
    assert "budget" in cleaned_df.columns
    assert "runtime" in cleaned_df.columns
    assert "worldwide_gross" in cleaned_df.columns

    ## Directors normalized
    directors_path = tmp_path / "normalized/directors.csv"
    directors_df = pd.read_csv(directors_path)
    assert set(directors_df["director"]) == {"Dir A", "Dir B"}

    ## Junction table
    junction_path = tmp_path / "normalized/movie_directors.csv"
    junction_df = pd.read_csv(junction_path)
