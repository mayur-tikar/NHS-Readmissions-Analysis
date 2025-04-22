
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_engine() :
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')
    return engine

def insert_dataframe_to_db(df, table_name) :
    engine = get_engine()
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Inserted data into table: {table_name}")

def run_query(query):
    engine = get_engine()
    return pd.read_sql_query(query, engine)

