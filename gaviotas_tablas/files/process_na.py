import pandas as pd
from files.constants import hotel_mapping, apply_event_param_parsing

# Función para limpiar corchetes y comillas de una columna determinada
def clean_column(df, col):
    df[col] = df[col].dropna().astype(str).str.replace(r'[\[\]]', '', regex=True)
    df[col] = df[col].str.replace("'", "")
    return df


def convert_to_datetime(df, col, fmt='%Y-%m-%d'):
    df[col] = pd.to_datetime(df[col], format=fmt)
    return df

# Función para preparar los eventos de "no_availability", donde se extraen las columnas relevantes para la tabla
def prepare_event_na(df_params):
    cols = ['user_pseudo_id', 'hotelName', 'page_location', 'checkinDate', 'checkoutDate', 
            'room1_occupancy', 'event_date', 'time_difference']
    
    df_event_na = df_params[cols].copy()

    for col in ['hotelName', 'page_location', 'checkinDate', 'checkoutDate', 'room1_occupancy']:
        df_event_na = clean_column(df_event_na, col)

    df_event_na = convert_to_datetime(df_event_na, 'checkinDate')
    df_event_na = convert_to_datetime(df_event_na, 'checkoutDate')
    df_event_na = convert_to_datetime(df_event_na, 'event_date', fmt='%Y%m%d')

    df_event_na['pagina'] = df_event_na['page_location'].str.extract(r"https?://([^/]+)")
    df_event_na['hotelName'] = df_event_na['hotelName'].fillna(df_event_na['pagina'].map(hotel_mapping))

    df_event_na.drop(columns=['pagina', 'page_location'], inplace=True)

    return df_event_na


def compute_event_metrics(df_event_na):
    df_event_na['TotalNights'] = (df_event_na['checkoutDate'] - df_event_na['checkinDate']).dt.days
    df_event_na['DaysToCheckIn'] = (df_event_na['checkinDate'] - df_event_na['event_date']).dt.days
    return df_event_na


def fill_missing_time_diff(df_event_na):
    df_event_na['time_difference'].fillna(df_event_na['time_difference'].mean(), inplace=True)
    return df_event_na


def process_no_availability_events(df_sorted):
    data_na = df_sorted[df_sorted['event_name'] == 'no_availability']

    df_params = apply_event_param_parsing(data_na)
    df_event_na = prepare_event_na(df_params)
    df_event_na = compute_event_metrics(df_event_na)
    df_event_na = fill_missing_time_diff(df_event_na)

    return df_event_na
