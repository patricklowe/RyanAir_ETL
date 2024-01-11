#!/usr/bin/env python
import pandas as pd
import requests
import json
import psycopg2
import psycopg2.extras as extras


def extract(api_key):
    # ## Extract
    # Gather data from airlabs API about RyanAir flights
    # https://airlabs.co/account
    fields = "&_fields=flight_iata,dep_iata,dep_time_utc,dep_estimated_utc,dep_actual_utc,arr_iata,arr_time_utc,arr_estimated_utc,status,duration,delayed,dep_delayed,arr_delayed"
    method = 'ping'
    params = {'api_key': api_key}
    # Flights based on Airline, FR is IATA code for RyanAir
    schedules_api = 'https://airlabs.co/api/v9/schedules?airline_iata=FR'
    print("Extracting...")
    schedule_data = pd.json_normalize(requests.get(schedules_api+fields+method, params).json(), record_path=['response'])
    return schedule_data 
    
# ## Transform
# Clean data for uploading
import os
#print("Current Directory:" + str(CUR_DIR) + "/dags/scripts/airport_codes.csv")
def convert_iata(df):
    CUR_DIR = os.path.abspath(os.path.dirname(__file__))
    airport_codes = pd.read_csv(str(CUR_DIR)+'/airport_codes.csv')
    df = df.merge(
        airport_codes[['dep_iata','airport_name']],
        on='dep_iata',
        how="left")
    df.rename(
        columns={"airport_name": "departure_airport"},
        inplace=True)
    df = df.drop(columns=['dep_iata'])
    df = df.merge(
        airport_codes[['arr_iata','airport_name']],
        on='arr_iata',
        how="left")
    df.rename(
        columns={"airport_name": "arrival_airport"},
        inplace=True)
    df = df.drop( columns=['arr_iata'])
    return df

def convert_timestamp(df):
    cols = ['dep_time_utc','dep_estimated_utc','dep_actual_utc','arr_time_utc','arr_estimated_utc']
    df[cols] = df[cols].apply(pd.to_datetime)
    # departure date/time in GMT
    df['dep_date'], df['dep_time'] = df['dep_time_utc'].dt.normalize(), df['dep_time_utc'].dt.time
    # updated departure date/time in GMT
    df['dep_date_upd'], df['dep_time_upd'] = df['dep_estimated_utc'].dt.normalize(), df['dep_estimated_utc'].dt.time
    df['dep_date_upd'].fillna(df['dep_date'], inplace=True)
    df['dep_time_upd'].fillna(df['dep_time'], inplace=True)
    
    # actual departure date/time in GMT
    df['dep_date_act'], df['dep_time_act'] = df['dep_actual_utc'].dt.normalize(), df['dep_actual_utc'].dt.time
    df['dep_date_act'].fillna(df['dep_date'], inplace=True)
    df['dep_time_act'].fillna(df['dep_time'], inplace=True)
    # arrival date/time in GMT
    df['arr_date'], df['arr_time'] = df['arr_time_utc'].dt.normalize(), df['arr_time_utc'].dt.time
    # updated arrival date/time in GMT
    df['arr_date_upd'], df['arr_time_upd'] = df['arr_estimated_utc'].dt.normalize(), df['arr_estimated_utc'].dt.time
    df['arr_date_upd'].fillna(df['arr_date'], inplace=True)
    df['arr_time_upd'].fillna(df['arr_time'], inplace=True)
    df.drop( columns=cols, inplace=True)
    return df

def prep_load(df):
    df[['delayed','dep_delayed']] = df[['delayed','dep_delayed']].fillna(0)
    df = df[df['status']=='landed']
    return df

# ## Load
# Load data into Postgres database

def create_table(conn):
    cur = conn.cursor() 
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS public.schedule(
        flight_id BIGSERIAL PRIMARY KEY,
        flight_iata VARCHAR(8),
        status VARCHAR(10),
        departure_airport VARCHAR(255),
        arrival_airport VARCHAR(255),
        dep_date date,
        dep_time time,
        dep_time_upd time,
        dep_time_act time,
        arr_date date,
        arr_time time,
        arr_time_upd time,
        duration real,
        delayed real,
        dep_delayed real,
        arr_date_upd date,
        dep_date_upd date,
        dep_date_act date);
        """)
    except (Exception, psycopg2.DatabaseError) as error: 
        print(error) 
        conn.rollback()
    else:
        conn.commit()

def insert_values(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns)) 
    query = """INSERT INTO %s(%s) VALUES %%s;""" % (table, cols) 
    cursor = conn.cursor() 
    try: 
        extras.execute_values(cursor, query, tuples) 
        conn.commit() 
    except (Exception, psycopg2.DatabaseError) as error: 
        print("Error: %s" % error) 
        conn.rollback() 
        cursor.close() 
        return 1
    cursor.close()

def main():
       
    api_key = "YOUR_AIRLABS_API_KEY"
    data = extract(api_key)
    
    conn = psycopg2.connect(
        host="postgres", # changed from 'localhost' so it would work with docker
        database="ryanair_API",
        user="postgres", #your postgres username
        password="POSTGRES_PASSWORD")
     
    print("Transforming...")
    create_table(conn)
    data = prep_load(convert_timestamp(convert_iata(data)))
    print("Loading...")
    insert_values(conn, data, 'schedule')
    print("Finished.")

if __name__ == "__main__":
    main()