import pandas as pd
from files.constants import hotel_mapping, apply_event_param_parsing
from files.process_purchase import enrich_with_item_data
import json


# Función para filtrar eventos de "add_to_cart"
def filter_add_to_cart_events(df):
    return df[df['event_name'] == 'add_to_cart']

# Función para seleccionar las columnas finales del DataFrame
def select_final_columns(df):
    col_add = ['user_pseudo_id', 'event_name', 'hotelName', 'page_location', 'room_name', 'room_code',
               'room_occupancy', 'check_in', 'check_out', 'event_date', 'time_difference']
    return df[col_add]

# Función para limpiar y transformar los datos
def clean_and_transform(df):

    # Se extrae el nombre del hotel y de la página
    df['page_location'] = df['page_location'].astype(str).str.replace(r'[\[\]]', '', regex=True)
    df['page_location'] = df['page_location'].str.replace("'", "")
    df['pagina'] = df['page_location'].str.extract(r"https?://([^/]+)")

    df['hotelName'] = df['hotelName'].dropna().astype(str).str.replace(r'[\[\]]', '', regex=True)
    df['hotelName'] = df['hotelName'].str.replace("'", "")

    df['hotelName'] = df['hotelName'].fillna(df['pagina'].map(hotel_mapping))

    df.drop(columns=['pagina', 'page_location'], inplace=True)

    # Se convierten las fechas a formato datetime
    df['check_in'] = pd.to_datetime(df['check_in'], format='mixed', dayfirst=True, errors='coerce')
    df['check_out'] = pd.to_datetime(df['check_out'], format='mixed', dayfirst=True, errors='coerce')
    df['event_date'] = pd.to_datetime(df['event_date'], format='%Y%m%d')

    df['TotalNights'] = (df['check_out'] - df['check_in']).dt.days
    df['DaysToCheckIn'] = (df['check_in'] - df['event_date']).dt.days

    df['time_difference'].fillna(df['time_difference'].mean(), inplace=True)

    return df


def process_add_to_cart_events(df_sorted):
    df_sorted = filter_add_to_cart_events(df_sorted)
    df_parsed = apply_event_param_parsing(df_sorted)
    df_enriched = enrich_with_item_data(df_parsed)
    df_event = select_final_columns(df_enriched)
    df_event = clean_and_transform(df_event)
    return df_event
