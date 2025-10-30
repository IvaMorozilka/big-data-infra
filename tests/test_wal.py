import pytest
import os
import tempfile
from app.core.wal import FileWal


class TestFileWal:

    def test_log_single_operation(self, temp_wal_file):
        """Тест логирования одной операции"""
        wal = FileWal(temp_wal_file)
        operation = {"type": "set", "key": "key1", "value": "value1"}
        
        wal.log(operation)
        
        assert os.path.exists(temp_wal_file)
        operations = wal.replay()
        assert len(operations) == 1
        assert operations[0] == operation

    def test_log_multiple_operations(self, temp_wal_file):
        """Тест логирования нескольких операций"""
        wal = FileWal(temp_wal_file)
        ops = [
            {"type": "set", "key": "key1", "value": "value1"},
            {"type": "set", "key": "key2", "value": "value2"},
            {"type": "delete", "key": "key1"}
        ]
        
        for op in ops:
            wal.log(op)
        
        replayed = wal.replay()
        assert len(replayed) == 3
        assert replayed == ops

    def test_replay_empty_wal(self, temp_wal_file):
        """Тест воспроизведения пустого WAL"""
        wal = FileWal(temp_wal_file)
        operations = wal.replay()
        
        assert operations == []

    def test_replay_nonexistent_file(self):
        """Тест воспроизведения несуществующего файла"""
        with tempfile.TemporaryDirectory() as temp_dir:
            wal_path = os.path.join(temp_dir, "nonexistent.log")
            if os.path.exists(wal_path):
                os.remove(wal_path)
            
            wal = FileWal(wal_path)
            operations = wal.replay()
            
            assert operations == []

    def test_compact(self, temp_wal_file):
        """Тест очистки WAL"""
        wal = FileWal(temp_wal_file)
        wal.log({"type": "set", "key": "key1", "value": "value1"})
        wal.log({"type": "set", "key": "key2", "value": "value2"})
        
        wal.compact()
        
        operations = wal.replay()
        assert operations == []
        assert os.path.exists(temp_wal_file)  # Файл должен существовать

    def test_log_after_compact(self, temp_wal_file):
        """Тест логирования после очистки WAL"""
        wal = FileWal(temp_wal_file)
        
        wal.log({"type": "set", "key": "key1", "value": "value1"})
        wal.compact()
        wal.log({"type": "set", "key": "key2", "value": "value2"})
        
        operations = wal.replay()
        assert len(operations) == 1
        assert operations[0]["key"] == "key2"

    def test_file_creation(self):
        """Тест автоматического создания файла и директории"""
        with tempfile.TemporaryDirectory() as temp_dir:
            wal_path = os.path.join(temp_dir, "logs", "wal.log")
            
            wal = FileWal(wal_path)
            
            assert os.path.exists(wal_path)

    def test_log_complex_operations(self, temp_wal_file):
        """Тест логирования сложных операций"""
        wal = FileWal(temp_wal_file)
        operation = {
            "type": "set",
            "key": "user:123",
            "value": {
                "name": "John Doe",
                "age": 30,
                "tags": ["admin", "user"],
                "metadata": {"created": "2024-01-01"}
            }
        }
        
        wal.log(operation)
        operations = wal.replay()
        
        assert len(operations) == 1
        assert operations[0] == operation

    def test_log_unicode_data(self, temp_wal_file):
        """Тест логирования данных с Unicode символами"""
        wal = FileWal(temp_wal_file)
        operation = {
            "type": "set",
            "key": "greeting",
            "value": "Привет мир 你好 ▄︻デ══━一💥"
        }
        
        wal.log(operation)
        operations = wal.replay()
        
        assert operations[0] == operation

    def test_replay_with_empty_lines(self, temp_wal_file):
        """Тест воспроизведения WAL с пустыми строками"""
        wal = FileWal(temp_wal_file)
        
        # Записываем операцию
        wal.log({"type": "set", "key": "key1", "value": "value1"})
        
        # Добавляем пустые строки вручную
        with open(temp_wal_file, 'a') as f:
            f.write("\n\n\n")
        
        # Записываем еще одну операцию
        wal.log({"type": "set", "key": "key2", "value": "value2"})
        
        operations = wal.replay()
        assert len(operations) == 2
        assert operations[0]["key"] == "key1"
        assert operations[1]["key"] == "key2"

    def test_replay_corrupted_wal(self, temp_wal_file):
        """Тест воспроизведения поврежденного WAL"""
        with open(temp_wal_file, 'w', encoding='UTF-8') as f:
            f.write('{"type": "set", "key": "key1"}\n')
            f.write('{ ▄︻デ══━一💥 }\n')
        
        wal = FileWal(temp_wal_file)
        
        with pytest.raises(ValueError, match="Ошибка декодирования WAL"):
            wal.replay()

    def test_multiple_wal_instances(self, temp_wal_file):
        """Тест работы с несколькими экземплярами WAL на одном файле"""
        wal1 = FileWal(temp_wal_file)
        wal2 = FileWal(temp_wal_file)
        
        wal1.log({"type": "set", "key": "key1", "value": "value1"})
        wal2.log({"type": "set", "key": "key2", "value": "value2"})
        
        operations = wal1.replay()
        assert len(operations) == 2

    def test_log_special_characters(self, temp_wal_file):
        """Тест логирования специальных символов"""
        wal = FileWal(temp_wal_file)
        operation = {
            "type": "set",
            "key": "special",
            "value": 'quotes "test" and\nnewlines\ttabs'
        }
        
        wal.log(operation)
        operations = wal.replay()
        
        assert operations[0] == operation

    def test_log_null_values(self, temp_wal_file):
        """Тест логирования None значений"""
        wal = FileWal(temp_wal_file)
        operation = {
            "type": "set",
            "key": "nullable",
            "value": None
        }
        
        wal.log(operation)
        operations = wal.replay()
        
        assert operations[0]["value"] is None

    def test_large_number_of_operations(self, temp_wal_file):
        """Тест логирования большого количества операций"""
        wal = FileWal(temp_wal_file)
        
        num_operations = 1000
        for i in range(num_operations):
            wal.log({"type": "set", "key": f"key{i}", "value": f"value{i}"})
        
        operations = wal.replay()
        assert len(operations) == num_operations
        assert operations[0]["key"] == "key0"
        assert operations[-1]["key"] == f"key{num_operations-1}"

    def test_compact_idempotent(self, temp_wal_file):
        """Тест - compact является идемпотентной операцией"""
        wal = FileWal(temp_wal_file)
        wal.log({"type": "set", "key": "key1", "value": "value1"})
        
        wal.compact()
        wal.compact()
        wal.compact()
        
        operations = wal.replay()
        assert operations == []

    def test_different_operation_types(self, temp_wal_file):
        """Тест логирования различных типов операций"""
        wal = FileWal(temp_wal_file)
        
        operations = [
            {"type": "set", "key": "key1", "value": "value1"},
            {"type": "delete", "key": "key2"},
            {"type": "set", "key": "key3", "value": {"nested": "data"}},
            {"type": "custom", "data": "something"}
        ]
        
        for op in operations:
            wal.log(op)
        
        replayed = wal.replay()
        assert replayed == operations

