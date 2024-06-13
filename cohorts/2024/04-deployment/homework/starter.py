#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip freeze | grep scikit-learn')


# In[2]:


get_ipython().system('python -V')


# In[9]:


import pickle
import pandas as pd
import numpy as np


# In[4]:


with open('model.bin', 'rb') as f_in:
    dv, model = pickle.load(f_in)


# In[18]:


year = 2023
month = 3
output_file = f'{year}_{month:02d}_output.pkl'


# In[5]:


categorical = ['PULocationID', 'DOLocationID']

def read_data(filename):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    df['ride_id'] = f'{year}/{month:02d}_' + df.index.astype('str')
    
    return df


# In[24]:


def apply_model(df: pd.DataFrame, model_file: str) -> pd.DataFrame:
    with open(model_file, 'rb') as f_in:
        dv, model = pickle.load(f_in)
    
    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = model.predict(X_val)

    df_result = df[['ride_id']].copy()
    df_result['prediction'] = y_pred
    return df_result    
    


# In[14]:


df = read_data(f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet')


# In[25]:


df_result = apply_model(df, 'model.bin')


# In[27]:


np.std(df_result['prediction'])


# In[15]:





# In[29]:


df_result.to_parquet(
    output_file,
    engine='pyarrow',
    compression=None,
    index=False
)


# In[ ]:




