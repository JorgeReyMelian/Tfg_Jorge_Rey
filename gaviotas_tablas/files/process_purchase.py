import pandas as pd
from files.constants import hotel_mapping, apply_event_param_parsing
import json


def filter_purchases(df):
    return df[df['event_name'] == 'purchase']

# Función para extraer los campos de los items del JSON, tras analizar su estructura
def extract_items_from_json(json_str):
    try:
        data = json.loads(json_str)
        fields = data['v'][0]['v']['f']
        return (
            fields[0]['v'],  # room_code
            fields[1]['v'],  # room_name
            fields[5]['v'],  # room_occupancy
            fields[6]['v'],  # check_in
            fields[7]['v'],  # check_out
            fields[9]['v']   # value
        )
    except Exception:
        return None, None, None, None, None, None


def enrich_with_item_data(df):
    cols = ['room_code', 'room_name', 'room_occupancy', 'check_in', 'check_out', 'valor']
    df[cols] = df['items'].apply(lambda x: pd.Series(extract_items_from_json(x)))
    return df


def select_final_columns(df):
    col_pur = ['user_pseudo_id', 'event_name', 'hotelName', 'page_location', 'valor', 'room_name', 'room_code',
               'room_occupancy', 'check_in', 'check_out', 'event_date', 'time_difference']
    return df[col_pur]

# Función para limpiar y transformar los datos, cambiando formato de las fechas y extrayendo otros campos clave
def clean_and_transform(df):
    df = df.dropna(subset=['valor'])

    df['valor'] = df['valor'].astype(str).str.replace(r'[\[\]]', '', regex=True)
    df['valor'] = df['valor'].str.replace("'", "")
    df['valor'] = df['valor'].astype(float)

    df['page_location'] = df['page_location'].astype(str).str.replace(r'[\[\]]', '', regex=True)
    df['page_location'] = df['page_location'].str.replace("'", "")
    df['pagina'] = df['page_location'].str.extract(r"https?://([^/]+)")

    df['hotelName'] = df['hotelName'].dropna().astype(str).str.replace(r'[\[\]]', '', regex=True)
    df['hotelName'] = df['hotelName'].str.replace("'", "")

    df['hotelName'] = df['hotelName'].fillna(df['pagina'].map(hotel_mapping))

    df.drop(columns=['pagina', 'page_location'], inplace=True)

    df['check_in'] = pd.to_datetime(df['check_in'], format='%Y-%m-%d')
    df['check_out'] = pd.to_datetime(df['check_out'], format='%Y-%m-%d')
    df['event_date'] = pd.to_datetime(df['event_date'], format='%Y%m%d')

    df['TotalNights'] = (df['check_out'] - df['check_in']).dt.days
    df['DaysToCheckIn'] = (df['check_in'] - df['event_date']).dt.days

    df['time_difference'].fillna(df['time_difference'].mean(), inplace=True)

    return df


def process_purchase_events(df_sorted):
    data_purchases = filter_purchases(df_sorted)
    df_parsed = apply_event_param_parsing(data_purchases)
    df_enriched = enrich_with_item_data(df_parsed)
    df_event = select_final_columns(df_enriched)
    df_event = clean_and_transform(df_event)
    return df_event


