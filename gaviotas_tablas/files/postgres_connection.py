from sqlalchemy import create_engine
import os

#Funición para obtener la conexión a la base de datos, las variables de conexión se dejan ocultas por temas de privacidad
def get_db_engine():
    db_user = os.getenv("DB_USER", "*****")
    db_password = os.getenv("DB_PASSWORD", "*****")
    db_host = os.getenv("DB_HOST", "*******")
    db_port = os.getenv("DB_PORT", "****")
    db_name = os.getenv("DB_NAME", "postgres")

    engine = create_engine(
    f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    f"?options=-c%20statement_timeout=600000"
)

    return engine
