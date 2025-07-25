import pandas as pd
import json

def convert_timestamps(df, columns):
    for col in columns:
        df[col] = pd.to_datetime(df[col] // 1000, unit='ms')
    return df

def get_time_difference(df, col1, col2):
    df['time_difference'] = (df[col1] - df[col2])
    df['time_difference'] = df['time_difference'].fillna(pd.Timedelta(seconds=0))
    return df

# Función para extraer ciudad y país de un campo JSON
def extract_city_country(df, column):

    def parse_city_country(record):
        record_dict = json.loads(record)
        city = record_dict['v']['f'][0]['v']
        country = record_dict['v']['f'][1]['v']
        return pd.Series([city, country])
    
    for i in range(2):
        df[i] = df[column].apply(lambda x: parse_city_country(x)[i])

    df.drop(columns=[column], inplace=True)
    df.rename(columns={0: 'city', 1: 'country'}, inplace=True)

    return df

# Función para extraer el tipo y la marca del dispositivo
def extract_disp_brand(df):

    def parse_disp_brand(json_str):
        try:
            data = json.loads(json_str)
            disp = data['v']['f'][0]['v']  
            brand = data['v']['f'][1]['v']  
            return disp, brand
        except Exception:
            return None, None
        
    df[['disp', 'brand']] = df['device'].apply(lambda x: pd.Series(parse_disp_brand(x)))

    return df


def count_events(df):
    df["num_views"] = df.groupby("user_pseudo_id")["event_name"].transform(lambda x: (x == "page_view").sum())
    df["num_clicks"] = df.groupby("user_pseudo_id")["event_name"].transform(lambda x: (x == "click").sum())
    df["num_engagement"] = df.groupby("user_pseudo_id")["event_name"].transform(lambda x: (x == "user_engagement").sum())
    df["num_scrolls"] = df.groupby("user_pseudo_id")["event_name"].transform(lambda x: (x == "scroll").sum())
    df["num_events"] = df.groupby("user_pseudo_id")["event_name"].transform(lambda x: (x.isin(["no_availability", "purchase", "refund", "views", "add_to_cart"]) == False).sum())
    return df

def sort_dataframe(df, column):
    df_sorted = df.sort_values(by=column)
    return df_sorted

def process_data(df):
    
    df = sort_dataframe(df, 'event_timestamp')
    df = convert_timestamps(df, ['event_timestamp', 'user_first_touch_timestamp'])
    df = get_time_difference(df, 'event_timestamp', 'user_first_touch_timestamp')
    df = extract_city_country(df, 'geo')
    df = count_events(df)
    df = extract_disp_brand(df)

    return df