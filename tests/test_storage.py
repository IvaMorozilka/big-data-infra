import pytest
from app.core.storage import InMemoryStorage


class TestInMemoryStorage:

    def test_set_and_get(self):
        """Тест базовой операции set и get"""
        storage = InMemoryStorage()
        storage.set("key1", "value1")
        assert storage.get("key1") == "value1"

    def test_get_nonexistent_key(self):
        """Тест получения несуществующего ключа"""
        storage = InMemoryStorage()
        assert storage.get("nonexistent") is None

    def test_set_overwrite(self):
        """Тест перезаписи значения"""
        storage = InMemoryStorage()
        storage.set("key1", "value1")
        storage.set("key1", "value2")
        assert storage.get("key1") == "value2"

    def test_delete_existing_key(self):
        """Тест удаления существующего ключа"""
        storage = InMemoryStorage()
        storage.set("key1", "value1")
        result = storage.delete("key1")
        assert result is True
        assert storage.get("key1") is None

    def test_delete_nonexistent_key(self):
        """Тест удаления несуществующего ключа"""
        storage = InMemoryStorage()
        result = storage.delete("nonexistent")
        assert result is False

    def test_get_all_data_empty(self):
        """Тест получения всех данных из пустого хранилища"""
        storage = InMemoryStorage()
        assert storage.get_all_data() == {}

    def test_get_all_data_with_items(self):
        """Тест получения всех данных с несколькими элементами"""
        storage = InMemoryStorage()
        storage.set("key1", "value1")
        storage.set("key2", "value2")
        storage.set("key3", "value3")
        
        all_data = storage.get_all_data()
        assert len(all_data) == 3
        assert all_data["key1"] == "value1"
        assert all_data["key2"] == "value2"
        assert all_data["key3"] == "value3"

    def test_get_all_data_returns_copy(self):
        """Тест что get_all_data возвращает копию, а не ссылку"""
        storage = InMemoryStorage()
        storage.set("key1", "value1")
        
        data1 = storage.get_all_data()
        data1["key2"] = "value2"  # Модифицируем копию
        
        data2 = storage.get_all_data()
        assert "key2" not in data2  # Оригинал не должен измениться

    def test_load_data_empty(self):
        """Тест загрузки пустых данных"""
        storage = InMemoryStorage()
        storage.set("key1", "value1")
        storage.load_data({})
        
        assert storage.get_all_data() == {}

    def test_load_data_with_items(self):
        """Тест загрузки данных с несколькими элементами"""
        storage = InMemoryStorage()
        data = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3"
        }
        storage.load_data(data)
        
        assert storage.get("key1") == "value1"
        assert storage.get("key2") == "value2"
        assert storage.get("key3") == "value3"

    def test_load_data_replaces_existing(self):
        """load_data должна заменять существующие данные"""
        storage = InMemoryStorage()
        storage.set("old_key", "old_value")
        
        new_data = {"new_key": "new_value"}
        storage.load_data(new_data)
        
        assert storage.get("old_key") is None
        assert storage.get("new_key") == "new_value"

    def test_load_data_creates_copy(self):
        """load_data должна создавать копию переданных данных"""
        storage = InMemoryStorage()
        external_data = {"key1": "value1"}
        storage.load_data(external_data)
        
        # Модифицируем внешние данные
        external_data["key2"] = "value2"
        
        # Хранилище не должно измениться
        assert storage.get("key2") is None

    def test_store_various_data_types(self):
        """Тест хранения различных типов данных"""
        storage = InMemoryStorage()
        
        storage.set("string", "text")
        storage.set("int", 42)
        storage.set("float", 3.14)
        storage.set("list", [1, 2, 3])
        storage.set("dict", {"nested": "value"})
        storage.set("bool", True)
        storage.set("none", None)
        
        assert storage.get("string") == "text"
        assert storage.get("int") == 42
        assert storage.get("float") == 3.14
        assert storage.get("list") == [1, 2, 3]
        assert storage.get("dict") == {"nested": "value"}
        assert storage.get("bool") is True
        assert storage.get("none") is None

    def test_multiple_operations_sequence(self):
        """Тест последовательности множественных операций"""
        storage = InMemoryStorage()
        
        # Добавляем данные
        storage.set("key1", "value1")
        storage.set("key2", "value2")
        
        # Обновляем
        storage.set("key1", "updated")
        
        # Удаляем
        storage.delete("key2")
        
        # Добавляем новый
        storage.set("key3", "value3")
        
        # Проверяем финальное состояние
        all_data = storage.get_all_data()
        assert len(all_data) == 2
        assert all_data["key1"] == "updated"
        assert "key2" not in all_data
        assert all_data["key3"] == "value3"

    def test_empty_string_key(self):
        """Тест работы с пустой строкой в качестве ключа"""
        storage = InMemoryStorage()
        storage.set("", "empty_key_value")
        assert storage.get("") == "empty_key_value"