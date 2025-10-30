import pytest
import os
from app.core.database import KVDB
from app.core.storage import InMemoryStorage
from app.core.persistence import Snapshotter
from app.core.wal import FileWal
from app.core.collection import Collection


def create_db(snapshot_path, wal_path, threshold=100):
    """Вспомогательная функция для создания базы данных"""
    return KVDB(
        storage_engine=InMemoryStorage(),
        persistence=Snapshotter(snapshot_path),
        wal=FileWal(wal_path),
        auto_snapshot_threshold=threshold
    )


class TestUnusualScenarios:

    def test_extremely_long_key(self, temp_files):
        """Тест работы с очень длинным ключом"""
        snapshot_path, wal_path = temp_files
        db = create_db(snapshot_path, wal_path)
        
        # Создаем ключ длиной 10000 символов
        long_key = "k" * 10000
        db.set(long_key, "value")
        
        assert db.get(long_key) == "value"
        
        # Проверяем персистентность
        db.shutdown()
        
        db2 = create_db(snapshot_path, wal_path)
        assert db2.get(long_key) == "value"

    def test_extremely_large_value(self, temp_files):
        """Тест работы с очень большим значением"""
        snapshot_path, wal_path = temp_files
        db = create_db(snapshot_path, wal_path)
        
        # Создаем большую строку (1 MB)
        large_value = "x" * (1024 * 1024)
        db.set("large", large_value)
        
        assert db.get("large") == large_value
        assert len(db.get("large")) == 1024 * 1024

    def test_deeply_nested_data_structure(self, temp_files):
        """Тест работы с глубоко вложенной структурой данных"""
        snapshot_path, wal_path = temp_files
        db = create_db(snapshot_path, wal_path)
        
        # Создаем глубоко вложенную структуру
        nested = {"level": 1}
        current = nested
        for i in range(2, 101):
            current["next"] = {"level": i}
            current = current["next"]
        
        db.set("deep", nested)
        retrieved = db.get("deep")
        
        # Проверяем глубину
        current = retrieved
        depth = 0
        while "next" in current:
            depth += 1
            current = current["next"]
        
        assert depth == 99
        assert retrieved["level"] == 1

    def test_circular_reference_in_list(self, temp_files):
        """Тест что списки с циклическими ссылками вызывают ошибку при сериализации"""
        snapshot_path, wal_path = temp_files
        db = create_db(snapshot_path, wal_path, threshold=1)  # Снапшот сразу после записи
        
        # Создаем список с циклической ссылкой
        circular_list = [1, 2, 3]
        circular_list.append(circular_list)
        
        # Должна возникнуть ошибка при создании снапшота
        with pytest.raises((ValueError, IOError)):
            db.set("circular", circular_list)

    def test_special_json_edge_cases(self, temp_files):
        """Тест специальных граничных случаев JSON"""
        snapshot_path, wal_path = temp_files
        db = create_db(snapshot_path, wal_path)
        
        # Различные граничные случаи
        db.set("empty_string", "")
        db.set("zero", 0)
        db.set("negative", -42)
        db.set("float_zero", 0.0)
        db.set("empty_list", [])
        db.set("empty_dict", {})
        db.set("false", False)
        db.set("null", None)
        
        db.shutdown()
        
        # Загружаем и проверяем
        db2 = create_db(snapshot_path, wal_path)
        
        assert db2.get("empty_string") == ""
        assert db2.get("zero") == 0
        assert db2.get("negative") == -42
        assert db2.get("float_zero") == 0.0
        assert db2.get("empty_list") == []
        assert db2.get("empty_dict") == {}
        assert db2.get("false") is False
        assert db2.get("null") is None

    def test_key_with_colon_in_collection(self, temp_files):
        """Тест ключей с двоеточием внутри коллекции"""
        snapshot_path, wal_path = temp_files
        db = create_db(snapshot_path, wal_path)
        
        collection = Collection(db, "users")
        # Ключ содержит двоеточие, что может вызвать путаницу с префиксом
        collection.set("user:123:456", {"data": "test"})
        
        assert collection.get("user:123:456") == {"data": "test"}
        assert collection.count() == 1

    def test_collection_name_with_multiple_colons(self, temp_files):
        """Тест имени коллекции с несколькими двоеточиями"""
        snapshot_path, wal_path = temp_files
        db = create_db(snapshot_path, wal_path)
        
        collection = Collection(db, "namespace:subsystem:collection")
        collection.set("key", "value")
        
        assert collection.get("key") == "value"
        # Проверяем что ключ в базе имеет правильный префикс
        assert db.get("namespace:subsystem:collection:key") == "value"

    def test_alternating_set_delete_same_key(self, temp_files):
        """Тест чередующихся операций set и delete на одном ключе"""
        snapshot_path, wal_path = temp_files
        db = create_db(snapshot_path, wal_path)
        
        # Чередуем операции
        for i in range(10):
            db.set("key", f"value{i}")
            db.delete("key")
        
        # В конце ключ не должен существовать
        assert db.get("key") is None
        
        # Проверяем что все операции залогированы
        wal = FileWal(wal_path)
        operations = wal.replay()
        assert len(operations) == 20  # 10 set + 10 delete

    def test_whitespace_only_keys(self, temp_files):
        """Тест ключей состоящих только из пробелов"""
        snapshot_path, wal_path = temp_files
        db = create_db(snapshot_path, wal_path)
        
        db.set("   ", "three spaces")
        db.set("\t", "tab")
        db.set("\n", "newline")
        db.set(" \t\n ", "mixed")
        
        assert db.get("   ") == "three spaces"
        assert db.get("\t") == "tab"
        assert db.get("\n") == "newline"
        assert db.get(" \t\n ") == "mixed"

    def test_binary_like_strings(self, temp_files):
        """Тест строк похожих на бинарные данные"""
        snapshot_path, wal_path = temp_files
        db = create_db(snapshot_path, wal_path)
        
        binary_like = "\\x00\\x01\\x02\\xff"
        db.set("binary", binary_like)
        
        assert db.get("binary") == binary_like

    def test_very_long_collection_chain(self, temp_files):
        """Тест работы с очень большим количеством коллекций"""
        snapshot_path, wal_path = temp_files
        db = create_db(snapshot_path, wal_path, threshold=1000)
        
        # Создаем 100 коллекций
        collections = []
        for i in range(100):
            col = Collection(db, f"collection{i}")
            col.set("key", f"value{i}")
            collections.append(col)
        
        # Проверяем что все коллекции независимы
        for i, col in enumerate(collections):
            assert col.get("key") == f"value{i}"
            assert col.count() == 1

    def test_empty_snapshot_with_wal_operations(self, temp_files):
        """Тест пустого снапшота с операциями в WAL"""
        snapshot_path, wal_path = temp_files
        
        # Создаем пустой снапшот
        from app.core.persistence import Snapshotter
        snapshotter = Snapshotter(snapshot_path)
        snapshotter.dump({})
        
        # Добавляем операции в WAL вручную
        wal = FileWal(wal_path)
        wal.log({"type": "set", "key": "key1", "value": "value1"})
        wal.log({"type": "set", "key": "key2", "value": "value2"})
        
        # Загружаем базу
        db = create_db(snapshot_path, wal_path)
        
        # Данные должны восстановиться из WAL
        assert db.get("key1") == "value1"
        assert db.get("key2") == "value2"

    def test_massive_collection_get_all(self, temp_files):
        """Тест get_all с большим количеством элементов"""
        snapshot_path, wal_path = temp_files
        db = create_db(snapshot_path, wal_path, threshold=10000)
        
        collection = Collection(db, "items")
        
        # Добавляем 1000 элементов
        for i in range(1000):
            collection.set(f"item{i}", {"index": i})
        
        all_items = collection.get_all()
        assert len(all_items) == 1000
        assert all_items["item0"]["index"] == 0
        assert all_items["item999"]["index"] == 999

    def test_rapid_shutdown_restart_cycles(self, temp_files):
        """Тест быстрых циклов shutdown/restart"""
        snapshot_path, wal_path = temp_files
        
        for i in range(10):
            db = create_db(snapshot_path, wal_path)
            db.set(f"key{i}", f"value{i}")
            db.shutdown()
        
        # Финальная проверка
        final_db = create_db(snapshot_path, wal_path)
        
        for i in range(10):
            assert final_db.get(f"key{i}") == f"value{i}"

    def test_zero_threshold_snapshot(self, temp_files):
        """Тест с нулевым threshold снапшота (граничный случай)"""
        snapshot_path, wal_path = temp_files
        db = create_db(snapshot_path, wal_path, threshold=0)  # Каждая операция создает снапшот
        
        db.set("key1", "value1")
        db.set("key2", "value2")
        
        # С порогом 0, снапшот создается после каждой операции
        # поэтому WAL должен быть пуст
        wal = FileWal(wal_path)
        operations = wal.replay()
        assert len(operations) == 0
        
        # Но данные должны быть в снапшоте
        assert db.get("key1") == "value1"
        assert db.get("key2") == "value2"

