

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import root_mean_squared_error
from xgboost import XGBRegressor
import joblib
import pandas as pd

def prediction():
    ## Load train dataset
    df = pd.read_csv("data/processed/movies_cleaned.csv")
            
    df['profit'] = df['worldwide_gross'] - df['budget']


    numeric_features = ['budget', 'worldwide_gross', 'runtime', 'profit']

    X = df[numeric_features]
    y = df['average_rating']

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = XGBRegressor(
        n_estimators=300,
        learning_rate=.08,
        max_depth=6,
        subsample=.8,
        colsample_bytree=.8,
        random_state=42
    )

    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    rmse = root_mean_squared_error(y_test, preds)
    print(f"RMSE: {rmse:.3f}")

    joblib.dump(model, "ML/xgb_imdb_rating.pkl")
    print("Model saved!")
