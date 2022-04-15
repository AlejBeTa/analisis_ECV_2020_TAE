from zipfile import ZipFile

import shutil

import pandas as pd
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import inspect

from src.db_connection.connection import ConexionBaseDatos
from src.utils import get_project_root


Base = declarative_base()


class CreaTablas:

    def __descomprime(self):
        if not self.data_raw_path.joinpath(f'{self.name_compress}.zip').is_file():
            raise FileNotFoundError()
        else:
            if not self.data_interim_path.joinpath(f'{self.name_compress}').is_dir():
                with ZipFile(self.data_raw_path.joinpath(f'{self.name_compress}.zip'), 'r') as zip:
                    zip.extractall(self.data_interim_path)
            else:
                pass

    def __genera_multiples_tablas(self):

        df_name = pd.read_json(self.data_raw_path.joinpath('correspondence_dbname_csv.json'), )
        df_name.columns = ['db_long_name']

        conexion_db = ConexionBaseDatos()

        for name_db in df_name.index:
            if not inspect(conexion_db.conexion_db).has_table(name_db):
                db_long_name = df_name.loc[name_db, "db_long_name"]
                self.__genera_tabla(conexion_db, name_db, db_long_name)

        conexion_db.cerrar_conexion()

        shutil.rmtree(self.data_interim_path.joinpath(f'{self.name_compress}'))

    def __genera_tabla(self, conexion_db, name_db, db_long_name):

        csv_path = self.data_interim_path.joinpath(self.name_compress, db_long_name)
        df = pd.read_csv(csv_path, sep=';', na_values=' ')

        Base.metadata.create_all(conexion_db.conexion_db)

        df.to_sql(
            con=conexion_db.conexion_db, index=False, index_label='DIRECTORIO', name=name_db, if_exists='replace'
        )

    def __verifica_db_completa(self):

        conexion_db = ConexionBaseDatos()

        df_name = pd.read_json(self.data_raw_path.joinpath('correspondence_dbname_csv.json'), )
        df_name.columns = ['db_long_name']

        existecia_tablas = [inspect(conexion_db.conexion_db).has_table(name_db) for name_db in df_name.index]

        conexion_db.cerrar_conexion()

        return all(existecia_tablas)

    def __init__(self, base_path=get_project_root(), name_compress='ECV_files'):

        self.base_path = base_path

        self.name_compress = name_compress

        self.data_raw_path = base_path.joinpath('data', 'raw', )
        self.data_interim_path = base_path.joinpath('data', 'interim')

        if not self.__verifica_db_completa():
            self.__descomprime()
            self.__genera_multiples_tablas()

        else:
            pass

