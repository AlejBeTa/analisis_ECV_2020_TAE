from typing import Any

import sqlalchemy
from pydantic import BaseModel
from sqlalchemy.orm import sessionmaker


from src.env import (
    PG_USER,
    PG_PASSWORD,
    PG_HOST,
    PG_DATABASE,
    PG_PORT,
)


class ConexionBaseDatos(BaseModel):
    conexion_db: Any = None
    sesion: Any = None

    def __inicializar(self):
        string_conexion = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
        conexion_db = sqlalchemy.create_engine(string_conexion)
        self.conexion_db = conexion_db

    def __generar_sesion(self):
        instancia_session_maker = sessionmaker(self.conexion_db)
        sesion = instancia_session_maker()
        self.sesion = sesion

    def cerrar_conexion(self):
        """Cierra la conexión con la base de datos."""
        self.sesion.close()
        self.conexion_db.dispose()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__inicializar()
        self.__generar_sesion()
