from elasticsearch import Elasticsearch, helpers
import datetime
from typing import Any, Iterable
from etl.tools.postgr import PostgresExtractor
from etl.tools.transform import Transform

host = 'http://127.0.0.1'
port = '9200'

test = ['025c58cd-1b7e-43be-9ffb-8571a613579b',
        'Star Wars: Episode VI - Return of the Jedi',
        'Luke Skywalker battles horrible Jabba the Hut and cruel Darth Vader to save his comrades in the Rebel Alliance and triumph over the Galactic Empire. Han Solo and Princess Leia reaffirm their love and team with Chewbacca, Lando Calrissian, the Ewoks and the androids C-3PO and R2-D2 to aid in the disruption of the Dark Side and the defeat of the evil emperor.',
        8.3,
        'movie',
        datetime.datetime(2021, 6, 16, 20, 14, 9, 221999, tzinfo=datetime.timezone.utc),
        datetime.datetime(2021, 6, 16, 20, 14, 9, 222016, tzinfo=datetime.timezone.utc),
        [
            {'person_id': '26e83050-29ef-4163-a99d-b546cac208f8', 'person_name': 'Mark Hamill', 'person_role': 'actor'},
            {'person_id': '3214cf58-8dbf-40ab-9185-77213933507e', 'person_name': 'Richard Marquand',
             'person_role': 'director'},
            {'person_id': '3217bc91-bcfc-44eb-a609-82d228115c50', 'person_name': 'Lawrence Kasdan',
             'person_role': 'writer'},
            {'person_id': '5b4bf1bc-3397-4e83-9b17-8b10c6544ed1', 'person_name': 'Harrison Ford',
             'person_role': 'actor'},
            {'person_id': 'a5a8f573-3cee-4ccc-8a2b-91cb9f55250a', 'person_name': 'George Lucas',
             'person_role': 'writer'},
            {'person_id': 'b5d2b63a-ed1f-4e46-8320-cf52a32be358', 'person_name': 'Carrie Fisher',
             'person_role': 'actor'},
            {'person_id': 'efdd1787-8871-4aa9-b1d7-f68e55b913ed', 'person_name': 'Billy Dee Williams',
             'person_role': 'actor'}
        ],
        ['Action', 'Adventure', 'Fantasy', 'Sci-Fi']]
el = Elasticsearch(f"{host}:{port}")

with PostgresExtractor(200,5) as con:
    transformer = Transform(con.extractors())
    resp = helpers.bulk(client=el, actions=transformer.transform(), index='movies', chunk_size=200)
    print(resp)
    # for i in transformer.transform():
    #     print(i)