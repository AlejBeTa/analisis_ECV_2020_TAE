import os
from dotenv import find_dotenv, load_dotenv


load_dotenv(find_dotenv())

PG_USER = os.environ['PG_USER']
PG_PASSWORD = os.environ['PG_PASSWORD']
PG_HOST = os.environ['PG_HOST']
PG_DATABASE = os.environ['PG_DATABASE']
PG_PORT = os.environ['PG_PORT']