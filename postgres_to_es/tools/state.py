import abc
import json
from json import JSONDecodeError
from typing import Any, Dict


class BaseStorage(abc.ABC):
    """Абстрактное хранилище состояния.

    Позволяет сохранять и получать состояние.
    Способ хранения состояния может варьироваться в зависимости
    от итоговой реализации. Например, можно хранить информацию
    в базе данных или в распределённом файловом хранилище.
    """

    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""


class JsonFileStorage(BaseStorage):
    """Реализация хранилища, использующего локальный файл.

    Формат хранения: JSON
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        last_state = self.retrieve_state()
        with open(self.file_path, "w+") as f:
            for key, value in state.items():
                last_state[key] = value
            f.write(json.dumps(last_state))

    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""
        try:
            with open(self.file_path) as f:
                return json.load(f)
        except (FileNotFoundError, JSONDecodeError):
            return {}


class State:
    """Класс для работы с состояниями."""

    def __init__(self, storage: JsonFileStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        self.storage.save_state({key: value})

    def butch_set_state(self, states: dict) -> None:
        self.storage.save_state(states)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        state_di = self.storage.retrieve_state()
        state = state_di.get(key, None)
        return state
