"""
shared minio and postgres client factories that will be used in every layer 
"""

import os
from dotenv import load_dotenv

import psycopg2
from minio import Minio

load_dotenv()

def get_minio_client() -> Minio:
  return Minio(
    os.environ["MINIO_ENDPOINT"],
    access_key=os.environ["MINIO_ROOT_USER"],
    secret_key=os.environ["MINIO_ROOT_PASSWORD"],
    secure=False,
  )
  
def get_postgres_connection():
  return psycopg2.connect(
    host=os.environ["POSTGRES_HOST"],
    port=os.environ["POSTGRES_PORT"],
    dbname=os.environ["POSTGRES_DB"],
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    client_encoding="utf8",
  )
  