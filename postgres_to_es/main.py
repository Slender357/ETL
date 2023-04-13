import logging
from time import sleep

from postgres_to_es.tools.config import (
    LOGGING,
    STORAGE,
    ESConfig,
    MainConfig,
    PostgresConfig,
)
from postgres_to_es.tools.extractor import PostgresExtractor
from postgres_to_es.tools.loader import Loader
from postgres_to_es.tools.state import JsonFileStorage, State

MAIN_CONFIG = MainConfig()
ES_CONFIG = ESConfig()
PG_CONFIG = PostgresConfig()

chunk_size = MAIN_CONFIG.chunk_size
delay = MAIN_CONFIG.delay
state = State(JsonFileStorage(STORAGE))


def etl(load: Loader, extract: PostgresExtractor) -> None:
    """
    Функция ETL (Extract, transform, load)
    :param load: Принимает объект Loader
    :param extract: Принимает объект PostgresExtractor
    """
    postgres_extract = extract.extractors()
    for items in postgres_extract:
        load.bulk(items)


if __name__ == "__main__":
    logging.basicConfig(**LOGGING)
    log = logging.getLogger(__name__)
    log.info("start")
    with Loader(ES_CONFIG) as loader:
        with PostgresExtractor(PG_CONFIG, chunk_size, state) as extractor:
            while True:
                etl(loader, extractor)
                log.info(f"sleep {delay} sek")
                sleep(delay)
