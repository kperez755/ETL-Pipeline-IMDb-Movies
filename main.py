from src.extract import extract_data
from src.transform import transform
from src.load import load_data


RAW_DATA = 'data/raw/movies.csv'

def main():
    ## Extract
    df = extract_data(RAW_DATA)

    ## Transform
    transform()

    ## Load
    load_data()

if __name__ == "__main__":
    main()
