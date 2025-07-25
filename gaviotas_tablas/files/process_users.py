import pandas as pd


# Se lleva a cabo el preprocesado de los datos de usuario
def preprocess_user_data(df_sorted):

    cols = ['user_pseudo_id', 'time_difference', 'city', 'country', 'num_views', 
            'num_events','num_engagement', 'num_scrolls','disp', 'brand', 'event_name', 'event_date']
    df_user = df_sorted[cols]

    df_user['first_event_date'] = df_user['event_date']
    df_user['last_event_date'] = df_user['event_date']

    df_user['time_difference(seconds)'] = df_user['time_difference'].dt.total_seconds()
    df_user.drop(columns=['time_difference'], inplace=True)

    df_user['brand'].fillna('Unknown', inplace=True)
    df_user['disp'].replace('None', 'Unknown', inplace=True)

    df_user['purchase'] = df_user['event_name'] == 'purchase'
    df_user['no_availability'] = df_user['event_name'] == 'no_availability'
    df_user['refund'] = df_user['event_name'] == 'refund'
    df_user['add_to_cart'] = df_user['event_name'] == 'add_to_cart'

    return df_user


# Definimos dos funciones auxiliares para obtener la moda de variables categórias en el dataset agregado
def get_mode(x):
    return x.mode()[0] if not x.mode().empty else 'Unknown'

def get_mode_city(x):
    mode_value = x.mode()
    if not mode_value.empty:
        most_frequent = mode_value.iloc[0]
        if most_frequent and most_frequent.strip(): 
            return most_frequent
    
    first_valid = x[x.notna() & (x != "")].iloc[0] if not x[x.notna() & (x != "")].empty else 'Unknown'
    return first_valid

# Función para agregar los datos de usuario, agrupando por 'user_pseudo_id'
def aggregate_user_data(df_user):
    df_agg = df_user.groupby('user_pseudo_id', as_index=False).agg(
        {
            'city': get_mode_city,          
            'country': get_mode,
            'disp': get_mode,
            'brand': get_mode,
            'purchase': 'max',  
            'no_availability': 'max',
            'refund': 'max',  
            'add_to_cart': 'max',    
            'num_views': 'max',
            'num_events': 'max',
            'num_engagement': 'max',
            'num_scrolls': 'max',
            'time_difference(seconds)': 'max', 
            'first_event_date': 'min',
            'last_event_date': 'max',
            'event_name': 'last'   
        }
    )

    df_agg.drop(columns=['event_name'], inplace=True)
    df_agg['first_event_date'] = pd.to_datetime(df_agg['first_event_date'], format='%Y%m%d', errors='coerce')
    df_agg['last_event_date'] = pd.to_datetime(df_agg['last_event_date'], format='%Y%m%d', errors='coerce')

    return df_agg


def process_user_data(df_sorted):

    df_user = preprocess_user_data(df_sorted)
    df_agg = aggregate_user_data(df_user)

    return df_agg