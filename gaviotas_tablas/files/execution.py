from files.data_processing import process_data
from files.process_users import process_user_data
from files.sql_queries import fetch_data_from_db
from files.postgres_connection import get_db_engine
from files.process_na import process_no_availability_events
from files.process_purchase import process_purchase_events
from files.process_add import process_add_to_cart_events
from files.constants import query1, query2
from files.update_tables import *
import pandas as pd
from joblib import load


#Primera ejecución para inicializar las tablas
def execute_first():
    engine = get_db_engine()
    df = fetch_data_from_db(query1, query2)
    df_sorted = process_data(df)
    df_users = process_user_data(df_sorted)
    df_purchase = process_purchase_events(df_sorted)
    df_add_to_cart = process_add_to_cart_events(df_sorted)
    df_na = process_no_availability_events(df_sorted)

    with engine.begin() as connection:
        df_users.to_sql("tabla_users", con=connection, schema="datatg", if_exists='replace', index=False)
        df_purchase.to_sql("tabla_pur", con=connection, schema="datatg", if_exists='replace', index=False)
        df_na.to_sql("tabla_disp", con=connection, schema="datatg", if_exists='replace', index=False)
        df_add_to_cart.to_sql("tabla_add_to_cart", con=connection, schema="datatg", if_exists='replace', index=False)

    print("Inicialización completada y tablas cargadas.")

# Actualización diaria de las tablas
def execute_daily_update():
    engine = get_db_engine()
    df_users_full, df_processed = process_new_users(engine)
    
    if df_users_full is None or df_processed is None:
        print("No se realizaron actualizaciones: no hay usuarios ni eventos nuevos.")
        return

    df_purchase = update_purchase(engine, df_processed)
    df_na = update_na(engine, df_processed)
    df_add_to_cart = update_add_to_cart(engine, df_processed)
    df_predictions = update_predictions(df_users_full)

    with engine.begin() as connection:
        df_users_full.to_sql("tabla_users", con=connection, schema="datatg", if_exists='replace', index=False)
        df_purchase.to_sql("tabla_pur", con=connection, schema="datatg", if_exists='replace', index=False)
        df_na.to_sql("tabla_disp", con=connection, schema="datatg", if_exists='replace', index=False)
        df_add_to_cart.to_sql("tabla_add_to_cart", con=connection, schema="datatg", if_exists='replace', index=False)
        df_predictions.to_sql("tabla_predictions", con=connection, schema="datatg", if_exists='replace', index=False)

    print("Actualización diaria completada con éxito.")

# Predicción de conversión
def predict(df_model):
    modelo = load('pipeline_smote_xgboost.joblib')

    df_clean = df_model.drop(
        columns=['user_pseudo_id', 'purchase', 'add_to_cart', 'city', 'first_event_date', 'last_event_date'],
        errors='ignore'
    )

    pred = modelo.predict(df_clean)
    proba = modelo.predict_proba(df_clean)[:, 1]

    df_model['prediccion_conversión'] = pred
    df_model['probabilidad_conversion'] = proba

    return df_model

