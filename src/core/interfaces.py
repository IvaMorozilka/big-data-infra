from abc import ABC, abstractmethod
from typing import Any, List, Optional, Dict

class IStorageEngine(ABC):
    """
    Интерфейс для движка хранения данных в памяти.
    """

    @abstractmethod
    def set(self, key: str, value: Any) -> None:
        """Сохраняет значение по ключу."""
        pass

    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """Возвращает значение по ключу."""
        pass

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Удаляет значение по ключу. Возвращает True, если ключ был найден и удален."""
        pass

    @abstractmethod
    def get_all_data(self) -> Dict[str, Any]:
        """Возвращает все данные из хранилища."""
        pass

    @abstractmethod
    def load_data(self, data: Dict[str, Any]) -> None:
        """Загружает все данные в хранилище."""
        pass


class IPersistence(ABC):
    """
    Интерфейс для механизма персистентности (создания и загрузки снапшотов).
    """

    @abstractmethod
    def dump(self, data: Dict[str, Any]) -> None:
        """Сохраняет снапшот данных на диск."""
        pass

    @abstractmethod
    def load(self) -> Optional[Dict[str, Any]]:
        """Загружает снапшот данных с диска."""
        pass

class IWriteAheadLog(ABC):
    """
    Интерфейс для механизма WAL.
    """

    @abstractmethod
    def log(self, operation: Dict[str, Any]) -> None:
        """Логирует одну операцию в журнал"""
        pass

    @abstractmethod
    def replay(self) -> List[Dict[str, Any]]:
        """Читает и возвращает все операции из журнала."""
        pass

    @abstractmethod
    def compact(self) -> None:
        """Очищает журнал"""
        pass


class IDatabase(ABC):
    """
    Основной интерфейс базы данных.
    """

    @abstractmethod
    def set(self, key: str, value: Any) -> None:
        """Сохраняет значение по ключу."""
        pass

    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """Возвращает значение по ключу."""
        pass

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Удаляет значение по ключу."""
        pass


class ICollection(ABC):
    """
    Интерфейс для коллекции - логической группы данных с общим префиксом.
    """

    @abstractmethod
    def set(self, key: str, value: Any) -> None:
        """Сохраняет значение в коллекции."""
        pass

    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """Получает значение из коллекции."""
        pass

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Удаляет значение из коллекции."""
        pass

    @abstractmethod
    def get_all(self) -> Dict[str, Any]:
        """Получает все данные из коллекции."""
        pass

    @abstractmethod
    def count(self) -> int:
        """Возвращает количество элементов в коллекции."""
        pass

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Проверяет, существует ли ключ в коллекции."""
        pass