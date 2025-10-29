from typing import Any, Optional, Dict
from src.core.interfaces import IDatabase, ICollection
import logging

logger = logging.getLogger(__name__)


class Collection(ICollection):
    """
    Коллекция - логическая группа данных с общим префиксом.
    Позволяет организовать данные в пространства имен.
    """

    def __init__(self, db: IDatabase, name: str):
        """
        Создает коллекцию с заданным именем.
        
        Args:
            db: Экземпляр базы данных
            name: Имя коллекции (будет использоваться как префикс ключей)
        """
        self.db = db
        self.name = name
        self.prefix = f"{name}:"

    def _make_key(self, key: str) -> str:
        """Создает полный ключ с префиксом коллекции."""
        return f"{self.prefix}{key}"

    def set(self, key: str, value: Any) -> None:
        """Сохраняет значение в коллекции."""
        full_key = self._make_key(key)
        self.db.set(full_key, value)
        logger.debug(f"Collection '{self.name}' SET: {key} = {value}")

    def get(self, key: str) -> Optional[Any]:
        """Получает значение из коллекции."""
        full_key = self._make_key(key)
        value = self.db.get(full_key)
        logger.debug(f"Collection '{self.name}' GET: {key} = {value}")
        return value

    def delete(self, key: str) -> bool:
        """Удаляет значение из коллекции."""
        full_key = self._make_key(key)
        result = self.db.delete(full_key)
        logger.debug(f"Collection '{self.name}' DELETE: {key} - {'успешно' if result else 'не найдено'}")
        return result

    def get_all(self) -> Dict[str, Any]:
        """
        Получает все данные из коллекции.
        """
        all_data = self.db.storage_engine.get_all_data()
        # Фильтруем только ключи с нашим префиксом
        collection_data = {}
        for full_key, value in all_data.items():
            if full_key.startswith(self.prefix):
                # Убираем префикс из ключа
                key = full_key[len(self.prefix):]
                collection_data[key] = value
        return collection_data

    def count(self) -> int:
        """Возвращает количество элементов в коллекции."""
        return len(self.get_all())

    def exists(self, key: str) -> bool:
        """Проверяет, существует ли ключ в коллекции."""
        return self.get(key) is not None

    def __str__(self) -> str:
        return f"Collection(name='{self.name}', items={self.count()})"

    def __repr__(self) -> str:
        return self.__str__()

