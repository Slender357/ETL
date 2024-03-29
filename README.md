# ETL

## Выбранный метод проверки
Для обеспечения актуализации данных была выбрана схема с выбором значений в временном промежутке.
Промежуток от даты прошлой проверки до времени начала цикла, с сортировкой по uuid 
(так как время модификации, хоть и с малой вероятностью может совпадать). 
Такой подход гарантирует, что при изменении данных во время цикла, изменения не пропадут, а будут применены при следующей итерации. 

## Extractor
При первом запуске устанавливается минимальная дата проверки, это гарантирует что в `Loader` попадут все данные созданные до старта.
Генератор отдает данные пачками по `n` или меньше записей заданой в `batch_size`.
Так как использован метод через 3 запроса `reference->m2m->film_work`, идет фиксация состояний по всем таблицам.
Установка новых состояний происходит после после того как `Loader` прейдет к следующей итерации.

## Transform
Перед отдачей пачки данных из `Loader`, данные проходят валидацию и трансформируются в необходимый для `Elasticsearch` вид.

## Loader
Данные отправляются пачкой `n` или меньше записей заданой в `batch_size`.При успешной загрузке данных скрипт запросит следующую пачку,
и это событие установит новые стейты в `Extractor`


Для запуска приложения необходимо подготовить `.env` файл по примеру `.env.example` и  инициализировать `docker-compose.yaml` через команду `docker-compose up`. 
При создании контейра Postgres будет подгружен `dump-movies_database` из папки `dump` базы данных и наполнит его данными.
Будет созданно 3 `volume` для данных постгресс, хранилища json состояний, данных `Elasticsearch`.
Для проверки работы в корне есть `test.json` для постман.

### P.S.
Образ `Elasticsearch` может не загрузиться в виду ограничений создателя для России. Рекомендуется использовать VPN.
