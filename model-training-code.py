import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from google.cloud import storage
from joblib import dump
from sklearn.pipeline import make_pipeline
import logging

logging.basicConfig(filename='bikeshare_training.log', level=logging.INFO,format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

storage_client = storage.Client()
bucket = storage_client.bucket("bikeshare-7131")

def load_data(filename):
    df = pd.read_csv(filename)
    return df

def preprocess_data(df):
    df = df.rename(columns={'weathersit':'weather',
                            'yr':'year',
                            'mnth':'month',
                            'hr':'hour',
                            'hum':'humidity',
                            'cnt':'count'})
    df = df.drop(columns=['instant', 'dteday', 'year'])
    cols = ['season', 'month', 'hour', 'holiday', 'weekday', 'workingday', 'weather']
    for col in cols:
        df[col] = df[col].astype('category')
    df['count'] = np.log(df['count'])
    df_oh = df.copy()
    for col in cols:
        df_oh = one_hot_encoding(df_oh, col)
    X = df_oh.drop(columns=['atemp', 'windspeed', 'casual', 'registered', 'count'], axis=1)
    y = df_oh['count']
    return X, y

def one_hot_encoding(data, column):
    data = pd.concat([data, pd.get_dummies(data[column], prefix=column, drop_first=True)], axis=1)
    data = data.drop([column], axis=1)
    return data

def train_rf_model(x_train, y_train,max_depth=None, n_estimators=100):
    model = RandomForestRegressor(max_depth=max_depth, n_estimators=n_estimators)
    pipeline = make_pipeline(model)
    pipeline.fit(x_train, y_train)
    return pipeline

def save_model_artifact(pipeline):
    artifact_name = 'model.joblib'
    dump(pipeline, artifact_name)
    model_artifact = bucket.blob('artifact/'+artifact_name)
    model_artifact.upload_from_filename(artifact_name)

filename = 'gs://bikeshare-7131/hour.csv'
df = load_data(filename)
X, y = preprocess_data(df)
with open("feature_order.txt", "w") as f:
    f.write(",".join(X.columns))

bucket.blob("artifact/feature_order.txt").upload_from_filename("feature_order.txt")

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)

max_depth = 10
n_estimators = 200
pipeline = train_rf_model(X_train, y_train, max_depth, n_estimators)

y_pred = pipeline.predict(X_test)
save_model_artifact(pipeline)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
logging.info(f'RMSE: {rmse}')
print('RMSE:', rmse)
