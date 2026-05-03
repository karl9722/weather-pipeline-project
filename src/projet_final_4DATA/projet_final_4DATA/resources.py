from dagster import resource
from sqlalchemy import create_engine
import os

@resource
def database(_context):
    url = os.getenv("DATABASE_URL")
    return create_engine(url)