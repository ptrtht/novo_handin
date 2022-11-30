import pandas as pd
from sqlalchemy import create_engine

ph1 = pd.read_csv('./csvs/400E_PH1.csv', sep='\\t', engine='python')
ph2 = pd.read_csv('./csvs/400E_PH2.csv', sep='\\t', engine='python')
temp1 = pd.read_csv('./csvs/400E_Temp1.csv', sep='\\t', engine='python')
temp2 = pd.read_csv('./csvs/400E_Temp2.csv', sep='\\t', engine='python')
binf = pd.read_csv('./csvs/batch_info.csv', sep='\\t', engine='python')
bphase = pd.read_csv('./csvs/batch_phase.csv', sep='\\t', engine='python')


username = "postgres"
password = "Ratherbe01"
url = "novo-sensors-mock.c9dvehkk6hkt.eu-central-1.rds.amazonaws.com"
port = "5432"
db = "postgres"

engine = create_engine(f'postgresql://{username}:{password}@{url}:{port}/{db}')
print("db successfully connected")

# TODO drop all tables for "clean" inserts

ph1["sensor_name"] = "400E_PH1"
ph1.to_sql('ph_sensors', engine, if_exists='append', index=False)
print("ph_sensors inserted || 1")

ph2["sensor_name"] = "400E_PH2"
ph2.to_sql('ph_sensors', engine, if_exists='append', index=False)
print("ph_sensors inserted || 2")


temp1["sensor_name"] = "400E_Temp1"
temp1.to_sql('temp_sensors', engine, if_exists='append', index=False)
print("temp_sensors inserted || 1")

temp2["sensor_name"] = "400E_Temp2"
temp2.to_sql('temp_sensors', engine, if_exists='append', index=False)
print( "temp_sensors inserted || 2")

binf = binf.rename(
    columns={
        'StartDate': 'start_date',
        'EndDate': 'end_date',
        'BatchID': 'batch_id'
             }
)
binf.to_sql("batch_info", engine, if_exists='append', index=False)
print("batch_info inserted")

bphase = bphase.rename(
    columns={
        'StartDate': 'start_date',
        'EndDate': 'end_date',
        'BatchPhase': 'batch_phase'
             }
)
bphase.to_sql("batch_phase", engine, if_exists='append', index=False)
print("batch_phase inserted")


print("Done")