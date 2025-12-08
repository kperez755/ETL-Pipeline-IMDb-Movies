import pandas as pd
from src.extract import extract_data
from unittest.mock import patch

def test_extract_data_with_patch(tmp_path):
    ## Create temp CSV file
    file = tmp_path / "movies.csv"
    file.write_text("title,year,rating\nInception,2010,8.8\nAvatar,2009,7.9")

    ## Patch logger doesnt print logs
    with patch("src.extract.logger.info"):
        df = extract_data(file)

    ## Assertions
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ["title", "year", "rating"]
    
    ## Check values
    assert df.loc[0, "title"] == "Inception"
    assert df.loc[1, "year"] == 2009
