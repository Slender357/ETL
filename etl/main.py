from time import sleep

from more_itertools import chunked

from etl.tools.config import STORAGE, ESConfig, MainConfig, PostgresConfig
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


def etl(
        load: Loader,
        extract: PostgresExtractor,
        chunk: int,
        delay_sek: int
) -> None:
    transformer = Transform(extract.extractors())
    chunk_items = chunked(transformer.transform(), chunk)
    for items in chunk_items:
        load.bulk(items)
    sleep(delay_sek)


if __name__ == "__main__":
    with Loader(ES_CONFIG) as loader:
        with PostgresExtractor(PG_CONFIG, chunk_size, state) as extractor:
            while True:
                etl(loader, extractor, chunk_size, delay)
