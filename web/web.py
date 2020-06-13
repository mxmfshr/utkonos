import streamlit as st
import pandas as pd
import os
import glob
import lightgbm as lgb
import datetime
from sklearn.metrics import roc_auc_score

airflow_home = os.environ['AIRFLOW_HOME']

@st.cache
def load_data():
    data = pd.read_csv(f'{airflow_home}/data/preprocessed/preprocessed_full.csv')
    return data
    
@st.cache
def get_dataset(df):
    cols = ['ChannelID',
        'Cluster',
        'prepay',
        'count_edit',
        'interval_time',
        'order_weekday',
        'weekday',
        'interval_high',
        'CancelFlag',
    ]
    
    df = df.sample(50000)
    
    data = df[cols]
    
    X = data.drop('CancelFlag', axis=1)
    y = data['CancelFlag']
    X.columns = range(len(X.columns))
    y.columns = [0]
    
    return X, y


df = load_data()

st.write('Sample of current data:')
st.dataframe(df.sample(10))
st.write('Data profile:')
st.write(df.describe())

models = [model.split('/')[-1] for model in glob.glob(f'{airflow_home}/model/*.txt')]

model_name = st.selectbox('Model used for inference', models)

model = lgb.Booster(model_file=f'{airflow_home}/model/{model_name}')

X, y = get_dataset(df)
y_pred = model.predict(X)

score = roc_auc_score(y, y_pred)
st.write("ROC AUC Score:")
st.write(score)
