from pydantic import BaseSettings, Field
import os
from pathlib import Path
import dotenv

dotenv.load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
STORAGE = os.path.join(BASE_DIR, "storage/storage.json")


class PostgresConfig(BaseSettings):
    dbname: str = Field(..., env='DB_NAME')
    user: str = Field(..., env='DB_USER')
    password: str = Field(..., env='DB_PASSWORD')
    host: str = Field(..., env='DB_HOST')
    port: str = Field(..., env='DB_PORT')


class ESConfig(BaseSettings):
    host: str = Field(..., env='ES_HOST')
    port: str = Field(..., env='ES_PORT')


class BackOffConfig(BaseSettings):
    start_sleep_time: float = Field(..., env='BO_START_SLEEP_TIME')
    factor: int = Field(..., env='BO_FACTOR')
    border_sleep_time: int = Field(..., env='BO_BORDER_SLEEP_TIME')


class MainConfig(BackOffConfig):
    chunk_size: int = Field(..., env='MAIN_CHUNK')
    delay: int = Field(..., env='MAIN_DELAY')
