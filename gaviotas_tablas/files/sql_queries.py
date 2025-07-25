import pandas as pd
from sqlalchemy import text
from files.postgres_connection import get_db_engine

# Función para obtener datos de la base de datos
def fetch_data_from_db(query1, query2):
    engine = get_db_engine()
    with engine.connect() as connection:
        connection.execute(text("SET statement_timeout = 600000;"))
        connection.execute(text(query1)) 
        return pd.read_sql(text(query2), connection)