import matplotlib.pyplot as plt
import joblib
import pandas as pd
import numpy as np

def plotting():
    df = pd.read_csv('data/processed/movies_cleaned.csv')


    ## Load model
    model = joblib.load("ML/xgb_imdb_rating.pkl")

    ## feature names
    feature_names = ['budget', 'worldwide_gross', 'runtime', 'profit']

    ## Get importances
    importances = model.feature_importances_
    #   


    ## Bar plot or Scatter 
    plt.figure(figsize=(8, 5))
    plt.bar(feature_names, importances)
    # plt.scatter(df['runtime'], df['profit'], alpha=0.3)
    plt.xlabel("Features")
    plt.ylabel("Importance Score")
    plt.title("Features Vs Importance")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
