import pandas as pd
from sqlalchemy import text
from files.postgres_connection import get_db_engine
from files.data_processing import process_data
from files.process_users import process_user_data
from files.process_na import process_no_availability_events
from files.process_purchase import process_purchase_events
from files.process_add import process_add_to_cart_events
from files.execution import predict

engine = get_db_engine()

# Se crea una tabla temporal para almacenar los IDs de los usuarios que relizaron eventos clave en las últimas 24 horas
def create_temp_table(connection):
    connection.execute(text("""
    CREATE TEMP TABLE temp_user_ids AS
    SELECT DISTINCT user_pseudo_id
    FROM datatg.eventos_notnull
    WHERE event_name IN (
        'purchase', 'no_availability', 'refund',
        'cancel_booking_attempt', 'cancel_booking_error', 'add_to_cart'
    )
    AND TO_TIMESTAMP(event_timestamp / 1000) >= CURRENT_DATE - INTERVAL '1 day'
    AND TO_TIMESTAMP(event_timestamp / 1000) < CURRENT_DATE;
    """))



def get_user_ids_from_temp(connection):
    return pd.read_sql_query("SELECT user_pseudo_id FROM temp_user_ids;", con=connection)

def get_tables(engine):    
    df_users = pd.read_sql_table("tabla_users", con=engine, schema="datatg")
    df_pur = pd.read_sql_table("tabla_pur", con=engine, schema="datatg")
    df_na = pd.read_sql_table("tabla_disp", con=engine, schema="datatg")
    df_refund = pd.read_sql_table("tabla_refund", con=engine, schema="datatg")
    df_add_to_cart = pd.read_sql_table("tabla_add_to_cart", con=engine, schema="datatg")

    return df_users, df_pur, df_na, df_refund, df_add_to_cart

#Partiendo de esta tabla temporal, se extraen todos los eventos de los usuaarios pertencientes a la misma
def get_new_events(engine):
    with engine.connect() as connection:
        query = """
        SELECT *
        FROM datatg.eventos_notnull
        WHERE event_timestamp >= CURRENT_DATE - INTERVAL '1 day'
        AND event_timestamp < CURRENT_DATE;
        """
        return pd.read_sql_query(text(query), con=connection)

# Función para realizar el procesado de los nuevos datos a la tabla de usuarios
def process_new_users(engine):
    with engine.begin() as connection:
        connection.execute(text("SET statement_timeout = 600000;"))

        create_temp_table(connection)
        df_new_users = get_user_ids_from_temp(connection)
        user_ids = df_new_users['user_pseudo_id'].tolist()

        if not user_ids:
            print("No hay usuarios nuevos.")
            return None, None
        else:
            print(f"Usuarios nuevos encontrados: {len(user_ids)}")

        # Se eliminan los usuarios que ya existen en la tabla de usuarios, para mayor eficiencia en el procesamiento
        connection.execute(text("""
            DELETE FROM datatg.tabla_users WHERE user_pseudo_id = ANY(:user_ids);
        """), {"user_ids": user_ids})

        query = """
        SELECT *
        FROM datatg.eventos_notnull
        WHERE user_pseudo_id = ANY(:user_ids);
        """

    # Se obtienen todos los eventos de los usuarios nuevos, para ser procesados
        df_all_events = pd.read_sql_query(text(query), con=connection, params={"user_ids": user_ids})

    if df_all_events.empty:
        print("No hay eventos históricos para los nuevos usuarios.")
        return None, None

    # Se procesan los datos obtenidos para obtener una tabla de usuarios nuevos
    df_processed = process_data(df_all_events)
    df_users = process_user_data(df_processed)

    with engine.connect() as conn:
        df_users_orig = pd.read_sql_table("tabla_users", con=conn, schema="datatg")

    df_users_full = pd.concat([df_users_orig, df_users], ignore_index=True)
    return df_users_full, df_processed # La función devuelve la tabla de usuarios nueva, y la nueva tabla de eventos, que será utilizada en el resto de funciones.


# Función para actualizar la tabla de purchase con los nuevos eventos
def update_purchase(engine, df):
    with engine.connect() as connection:
        df_pur_orig = pd.read_sql_table("tabla_pur", con=connection, schema="datatg")

    if df.empty:
        print("No hay eventos de purchase del día anterior.")
        return df_pur_orig

    df_processed_purchase = process_purchase_events(df)
    df_pur_full = pd.concat([df_pur_orig, df_processed_purchase], ignore_index=True)
    return df_pur_full

# Función para actualizar la tabla de no_availability con los nuevos eventos
def update_na(engine, df):
    with engine.connect() as connection:
        df_na_orig = pd.read_sql_table("tabla_disp", con=connection, schema="datatg")

    if df.empty:
        print("No hay eventos de no_availability del día anterior.")
        return df_na_orig

    df_processed_na = process_no_availability_events(df)
    df_na_full = pd.concat([df_na_orig, df_processed_na], ignore_index=True)
    return df_na_full

# Función para actualizar la tabla de add_to_cart con los nuevos eventos
def update_add_to_cart(engine, df):
    with engine.connect() as connection:
        df_add_to_cart_orig = pd.read_sql_table("tabla_add_to_cart", con=connection, schema="datatg")

    if df.empty:
        print("No hay eventos de add_to_cart del día anterior.")
        return df_add_to_cart_orig

    df_processed_add_to_cart = process_add_to_cart_events(df)
    df_add_to_cart_full = pd.concat([df_add_to_cart_orig, df_processed_add_to_cart], ignore_index=True)
    return df_add_to_cart_full

# Función para actualizar la tabla de predicciones con los nuevos usuarios
def update_predictions(df):
    df_prediction = predict(df)
    return df_prediction

def load_tables(engine, df_users, df_pur, df_na, df_add_to_cart):
    with engine.begin() as connection:
        df_users.to_sql("tabla_users", con=connection, schema="datatg", if_exists='replace', index=False)
        df_pur.to_sql("tabla_pur", con=connection, schema="datatg", if_exists='replace', index=False)
        df_na.to_sql("tabla_disp", con=connection, schema="datatg", if_exists='replace', index=False)
        df_add_to_cart.to_sql("tabla_add_to_cart", con=connection, schema="datatg", if_exists='replace', index=False)
