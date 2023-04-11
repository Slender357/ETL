import logging
from time import sleep

from more_itertools import chunked

from etl.tools.config import (
    LOGGING,
    STORAGE,
    ESConfig,
    MainConfig,
    PostgresConfig,
)
from etl.tools.extractor import PostgresExtractor
from etl.tools.loader import Loader
from etl.tools.state import JsonFileStorage, State
from etl.tools.transform import Transform

MAIN_CONFIG = MainConfig()
ES_CONFIG = ESConfig()
PG_CONFIG = PostgresConfig()

chunk_size = MAIN_CONFIG.chunk_size
delay = MAIN_CONFIG.delay
state = State(JsonFileStorage(STORAGE))


def etl(load: Loader, extract: PostgresExtractor, chunk: int) -> None:
    """
    Функция ETL (Extract, transform, load)
    :param load: Принимает объект Loader
    :param extract: Принимает объект PostgresExtractor
    :param chunk: Размер чанка для выгрузки и загрзки
    :return:
    """
    transformer = Transform(extract.extractors())
    chunk_items = chunked(transformer.transform(), chunk, )
    for items in chunk_items:
        load.bulk(list(filter(None, items)))


if __name__ == "__main__":
    logging.basicConfig(**LOGGING)
    log = logging.getLogger(__name__)
    log.info("start")
    with Loader(ES_CONFIG) as loader:
        with PostgresExtractor(PG_CONFIG, chunk_size, state) as extractor:
            while True:
                etl(loader, extractor, chunk_size)
                log.info(f"sleep {delay} sek")
                sleep(delay)
