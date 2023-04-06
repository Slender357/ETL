from etl.tools.postgr import PostgressSotage
from etl.tools.state import State, JsonFileStorage


def extractor():
    conn = PostgressSotage()
    state = State(JsonFileStorage('./state.json'))
    if state.get_state('postgress') is None:
        last_mod = 1

state = State(JsonFileStorage('./state.json'))
print(state.get_state('postgress'))
