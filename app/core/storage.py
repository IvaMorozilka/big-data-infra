from typing import Any, Optional, Dict
from app.core.interfaces import IStorageEngine


class InMemoryStorage(IStorageEngine):
    """
    In-memory хранилище данных на основе хэш-таблицы (dict).
    """

    def __init__(self):
        self._data: Dict[str, Any] = {}

    def set(self, key: str, value: Any) -> None:
        """Сохраняет значение по ключу."""
        self._data[key] = value

    def get(self, key: str) -> Optional[Any]:
        """Возвращает значение по ключу."""
        return self._data.get(key)

    def delete(self, key: str) -> bool:
        """Удаляет значение по ключу. Возвращает True, если ключ был найден и удален."""
        if key in self._data:
            del self._data[key]
            return True
        return False

    def get_all_data(self) -> Dict[str, Any]:
        """Возвращает все данные из хранилища."""
        return self._data.copy()

    def load_data(self, data: Dict[str, Any]) -> None:
        """Загружает все данные в хранилище."""
        self._data = data.copy()

