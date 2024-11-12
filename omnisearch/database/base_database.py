from abc import ABC, abstractmethod


class DatabaseAPI(ABC):
    def __init__(self, api_key: str):
        self.api_key = api_key

    @abstractmethod
    def search(self, query: str):
        pass

    @abstractmethod
    def parse(self, data):
        pass

    @abstractmethod
    def generate_query(self, query: str):
        pass
