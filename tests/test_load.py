from unittest.mock import patch, Mock
from src.load import load_data

def test_load_data_workflow():
    ## Fake connection + cursor
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("psycopg2.connect", return_value=mock_conn):
        with patch("src.load.create_schema") as mock_schema:
            with patch("src.load.load_csv") as mock_load:
                with patch("pandas.read_csv") as mock_read:
                    
                    dummy_df = Mock()
                    dummy_df.rename = Mock()
                    dummy_df.to_csv = Mock()
                    mock_read.return_value = dummy_df

                    load_data()

                    # Schema created
                    mock_schema.assert_called_once_with(mock_cursor)

                    # load_csv called multiple times
                    assert mock_load.call_count > 5

                    # Commit + close
                    mock_conn.commit.assert_called_once()
                    mock_cursor.close.assert_called_once()
                    mock_conn.close.assert_called_once()
