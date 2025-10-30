import pytest
import os
from app.core.database import KVDB
from app.core.storage import InMemoryStorage
from app.core.persistence import Snapshotter
from app.core.wal import FileWal


def create_db(snapshot_path, wal_path, threshold=100):
    return KVDB(
        storage_engine=InMemoryStorage(),
        persistence=Snapshotter(snapshot_path),
        wal=FileWal(wal_path),
        auto_snapshot_threshold=threshold
    )


class TestKVDB:

    def test_set_and_get(self, db_with_low_threshold):
        """Тест базовых операций set и get"""
        db = db_with_low_threshold
        db.set("key1", "value1")
        assert db.get("key1") == "value1"

    def test_get_nonexistent_key(self, db_with_low_threshold):
        """Тест получения несуществующего ключа"""
        db = db_with_low_threshold
        assert db.get("Bob") is None

    def test_delete_existing_key(self, db_with_low_threshold):
        """Тест удаления существующего ключа"""
        db = db_with_low_threshold
        db.set("key1", "value1")
        result = db.delete("key1")
        
        assert result is True
        assert db.get("key1") is None

    def test_delete_nonexistent_key(self, db_with_low_threshold):
        """Тест удаления несуществующего ключа"""
        db = db_with_low_threshold
        result = db.delete("Bob")
        assert result is False

    def test_operations_logged_to_wal(self, temp_files):
        """Тест логирования операций в WAL"""
        snapshot_path, wal_path = temp_files
        db = create_db(snapshot_path, wal_path, threshold=100)
        
        db.set("key1", "value1")
        db.set("key2", "value2")
        db.delete("key1")
        
        # Проверяем что операции есть в WAL
        wal = FileWal(wal_path)
        operations = wal.replay()
        assert len(operations) == 3
        assert operations[0]["type"] == "set"
        assert operations[1]["type"] == "set"
        assert operations[2]["type"] == "delete"

    def test_auto_snapshot_on_threshold(self, temp_files):
        """Тест автоматического создания снапшота при достижении порога"""
        snapshot_path, wal_path = temp_files
        db = create_db(snapshot_path, wal_path, threshold=3)
        
        db.set("key1", "value1")
        db.set("key2", "value2")
        db.set("key3", "value3")  # Должен создаться снапшот
        
        # Проверяем что снапшот создан
        assert os.path.exists(snapshot_path)
        
        # Проверяем что WAL очищен
        wal = FileWal(wal_path)
        operations = wal.replay()
        assert len(operations) == 0

    def test_persistence_across_restarts(self, temp_files):
        """Тест персистентности данных между перезапусками"""
        snapshot_path, wal_path = temp_files
        
        # Первая сессия
        db1 = create_db(snapshot_path, wal_path)
        db1.set("key1", "value1")
        db1.set("key2", "value2")
        db1.shutdown()
        
        # Вторая сессия
        db2 = create_db(snapshot_path, wal_path)
        
        assert db2.get("key1") == "value1"
        assert db2.get("key2") == "value2"

    def test_wal_replay_on_initialization(self, temp_files):
        """Тест воспроизведения WAL при инициализации"""
        snapshot_path, wal_path = temp_files
        
        # Создаем снапшот
        db1 = create_db(snapshot_path, wal_path)
        db1.set("key1", "value1")
        db1.shutdown()
        
        # Добавляем операции в WAL напрямую
        wal = FileWal(wal_path)
        wal.log({"type": "set", "key": "key2", "value": "value2"})
        wal.log({"type": "delete", "key": "key1"})
        
        # Создаем новую базу
        db2 = create_db(snapshot_path, wal_path)
        
        assert db2.get("key1") is None  # Удалено через WAL
        assert db2.get("key2") == "value2"  # Добавлено через WAL

    def test_shutdown_creates_snapshot(self, temp_files):
        """shutdown должен создавать финальный снапшот"""
        snapshot_path, wal_path = temp_files
        db = create_db(snapshot_path, wal_path)
        
        db.set("key1", "value1")
        db.shutdown()
        
        assert os.path.exists(snapshot_path)

    def test_overwrite_value(self, db):
        """Тест перезаписи значения"""
        db.set("key1", "value1")
        db.set("key1", "value2")
        
        assert db.get("key1") == "value2"

    def test_complex_data_types(self, db):
        """Тест работы со сложными типами данных"""
        data = {
            "nested": {
                "key": "value",
                "list": [1, 2, 3]
            }
        }
        
        db.set("complex", data)
        retrieved = db.get("complex")
        
        assert retrieved == data

    def test_multiple_operations_sequence(self, db):
        """Тест последовательности множественных операций"""
        db.set("key1", "value1")
        db.set("key2", "value2")
        db.set("key3", "value3")
        db.delete("key2")
        db.set("key4", "value4")
        
        assert db.get("key1") == "value1"
        assert db.get("key2") is None
        assert db.get("key3") == "value3"
        assert db.get("key4") == "value4"

    def test_operation_count_resets_after_snapshot(self, temp_files):
        """Cчетчик операций должен сбрасывается после снапшота"""
        snapshot_path, wal_path = temp_files
        db = create_db(snapshot_path, wal_path, threshold=3)
        
        # Достигаем порога
        db.set("key1", "value1")
        db.set("key2", "value2")
        db.set("key3", "value3")  # Снапшот создан, счетчик сброшен
        
        assert db.operation_count == 0

    def test_snapshot_threshold_one(self, temp_files):
        """Тест работы с threshold снапшота равным 1"""
        snapshot_path, wal_path = temp_files
        db = create_db(snapshot_path, wal_path, threshold=1)
        
        db.set("key1", "value1")  # Должен сразу создать снапшот
        
        # WAL должен быть пуст
        wal = FileWal(wal_path)
        operations = wal.replay()
        assert len(operations) == 0

    def test_large_number_of_operations(self, temp_files):
        """Тест большого количества операций"""
        snapshot_path, wal_path = temp_files
        db = create_db(snapshot_path, wal_path, threshold=1000)
        
        # Выполняем много операций
        for i in range(500):
            db.set(f"key{i}", f"value{i}")
        
        # Проверяем несколько значений
        assert db.get("key0") == "value0"
        assert db.get("key250") == "value250"
        assert db.get("key499") == "value499"

    def test_crash_recovery_scenario(self, temp_files):
        """Тест сценария восстановления после сбоя"""
        snapshot_path, wal_path = temp_files
        
        # Имитируем работу до сбоя
        db1 = create_db(snapshot_path, wal_path)
        db1.set("key1", "value1")
        db1.set("key2", "value2")
        # Имитируем сбой без shutdown
        del db1
        
        # Восстановление
        db2 = create_db(snapshot_path, wal_path)
        
        # Данные должны восстановиться из WAL
        assert db2.get("key1") == "value1"
        assert db2.get("key2") == "value2"

