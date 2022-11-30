import pandas as pd
from sqlalchemy import create_engine
import warnings


class DataBaseSetup:
    def __init__(self) -> None:
        self.ph1 = pd.read_csv('./csvs/400E_PH1.csv', sep='\\t', engine='python')
        self.ph2 = pd.read_csv('./csvs/400E_PH2.csv', sep='\\t', engine='python')
        self.temp1 = pd.read_csv('./csvs/400E_Temp1.csv', sep='\\t', engine='python')
        self.temp2 = pd.read_csv('./csvs/400E_Temp2.csv', sep='\\t', engine='python')
        self.binf = pd.read_csv('./csvs/batch_info.csv', sep='\\t', engine='python')
        self.bphase = pd.read_csv('./csvs/batch_phase.csv', sep='\\t', engine='python')

    def create_engine():
        username = "postgres"
        password = "Ratherbe01"
        url = "novo-sensors-mock.c9dvehkk6hkt.eu-central-1.rds.amazonaws.com"
        port = "5432"
        db = "postgres"

        self.engine = create_engine(f'postgresql://{username}:{password}@{url}:{port}/{db}')
        print("db successfully connected")


    def reset_db():
        # TODO drop all tables for "clean" inserts
        warnings.warn("Warning: !_RESET DB IS NOT IMPLEMENTED_!")
        pass
    
    def add_tables():
        self.ph1["sensor_name"] = "400E_PH1"
        self.ph1.to_sql('ph_sensors', engine, if_exists='append', index=False)
        print("ph_sensors inserted || 1")

        self.ph2["sensor_name"] = "400E_PH2"
        self.ph2.to_sql('ph_sensors', engine, if_exists='append', index=False)
        print("ph_sensors inserted || 2")


        self.temp1["sensor_name"] = "400E_Temp1"
        self.temp1.to_sql('temp_sensors', engine, if_exists='append', index=False)
        print("temp_sensors inserted || 1")

        self.temp2["sensor_name"] = "400E_Temp2"
        self.temp2.to_sql('temp_sensors', engine, if_exists='append', index=False)
        print( "temp_sensors inserted || 2")

        self.binf = self.binf.rename(
            columns={
                'StartDate': 'start_date',
                'EndDate': 'end_date',
                'BatchID': 'batch_id'
                    }
        )
        self.binf.to_sql("batch_info", engine, if_exists='append', index=False)
        print("batch_info inserted")

        self.bphase = self.bphase.rename(
            columns={
                'StartDate': 'start_date',
                'EndDate': 'end_date',
                'BatchPhase': 'batch_phase'
                    }
        )
        self.bphase.to_sql("batch_phase", engine, if_exists='append', index=False)
        print("batch_phase inserted")
        print("Done")



if __name__ == "__main__":
    db = DataBaseSetup()
    db.create_engine()
    db.reset_db()
    db.add_tables()