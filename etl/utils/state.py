import abc
import logging
import json
from typing import Any, Optional


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        if self.file_path is None:
            return

        with open(self.file_path, 'w') as fl:
            json.dump(state, fl)

    def retrieve_state(self) -> dict:
        if self.file_path is None:
            return {}

        try:
            with open(self.file_path, 'r') as fl:
                return json.load(fl)
        except FileNotFoundError:
            logging.warning("FileNotFoundError", exc_info=True)
            return {}


class State:
    def __init__(self, storage: BaseStorage):
        self.storage = storage
        self.state = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        self.state[key] = value
        self.storage.save_state(self.state)

    def get_state(self, key: str) -> Any:
        return self.state.get(key)
