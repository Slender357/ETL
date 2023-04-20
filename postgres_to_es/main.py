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

main_config = MainConfig()
es_config = ESConfig()
pg_config = PostgresConfig()

chunk_size = main_config.chunk_size
delay = main_config.delay
state = State(JsonFileStorage(STORAGE))


def etl(load: Loader, extract: PostgresExtractor) -> None:
    """
    Функция ETL (Extract, transform, load)
    :param load: Принимает объект Loader
    :param extract: Принимает объект PostgresExtractor
    """
    postgres_extract = extract.extractors()
    for items, index in postgres_extract:
        # print(items)
        # print(index)
        load.bulk(index, items)


if __name__ == "__main__":
    logging.basicConfig(**LOGGING)
    log = logging.getLogger(__name__)
    log.info("start")
    with Loader(es_config) as loader:
        with PostgresExtractor(pg_config, chunk_size, state) as extractor:
            while True:
                etl(loader, extractor)
                log.info(f"sleep {delay} sek")
                sleep(delay)
