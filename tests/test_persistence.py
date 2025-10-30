import pytest
import os
import json
import tempfile
from app.core.persistence import Snapshotter


class TestSnapshotter:

    def test_dump_simple_data(self, temp_snapshot_file):
        """Тест сохранения простых данных"""
        snapshotter = Snapshotter(temp_snapshot_file)
        data = {"key1": "value1", "key2": "value2"}
        
        snapshotter.dump(data)
        
        assert os.path.exists(temp_snapshot_file)
        with open(temp_snapshot_file, 'r', encoding='utf-8') as f:
            loaded = json.load(f)
        assert loaded == data

    def test_load_existing_snapshot(self, temp_snapshot_file):
        """Тест загрузки существующего снапшота"""
        data = {"key1": "value1", "key2": "value2"}
        with open(temp_snapshot_file, 'w', encoding='utf-8') as f:
            json.dump(data, f)
        
        snapshotter = Snapshotter(temp_snapshot_file)
        loaded = snapshotter.load()
        
        assert loaded == data

    def test_load_nonexistent_snapshot(self, temp_snapshot_file):
        """Тест загрузки несуществующего снапшота"""
        os.remove(temp_snapshot_file)
        snapshotter = Snapshotter(temp_snapshot_file)
        
        loaded = snapshotter.load()
        
        assert loaded is None

    def test_dump_empty_data(self, temp_snapshot_file):
        """Тест сохранения пустых данных"""
        snapshotter = Snapshotter(temp_snapshot_file)
        snapshotter.dump({})
        
        loaded = snapshotter.load()
        assert loaded == {}

    def test_dump_complex_data(self, temp_snapshot_file):
        """Тест сохранения сложных структур данных"""
        snapshotter = Snapshotter(temp_snapshot_file)
        data = {
            "string": "text",
            "number": 42,
            "float": 3.14,
            "list": [1, 2, 3],
            "nested": {
                "inner": "value",
                "deep": {
                    "deeper": [True, False, None]
                }
            }
        }
        
        snapshotter.dump(data)
        loaded = snapshotter.load()
        
        assert loaded == data

    def test_dump_unicode_data(self, temp_snapshot_file):
        """Тест сохранения данных с Unicode символами"""
        snapshotter = Snapshotter(temp_snapshot_file)
        data = {
            "data1": "Привет мир",
            "data2": "你好世界",
            "data3": "▄︻デ══━一💥"
        }
        
        snapshotter.dump(data)
        loaded = snapshotter.load()
        
        assert loaded == data

    def test_dump_overwrites_existing_file(self, temp_snapshot_file):
        """Тест перезаписи существующего файла"""
        snapshotter = Snapshotter(temp_snapshot_file)
        
        snapshotter.dump({"old": "data"})
        snapshotter.dump({"new": "data"})
        
        loaded = snapshotter.load()
        assert loaded == {"new": "data"}
        assert "old" not in loaded

    def test_directory_creation(self):
        """Тест автоматического создания директории"""
        with tempfile.TemporaryDirectory() as temp_dir:
            snapshot_path = os.path.join(temp_dir, "subdir", "snapshot.json")
            
            snapshotter = Snapshotter(snapshot_path)
            snapshotter.dump({"test": "data"})
            
            assert os.path.exists(snapshot_path)

    def test_load_corrupted_json(self, temp_snapshot_file):
        """Тест загрузки поврежденного JSON файла"""
        with open(temp_snapshot_file, 'w', encoding='UTF-8') as f:
            f.write("{ ▄︻デ══━一💥 }")
        
        snapshotter = Snapshotter(temp_snapshot_file)
        
        with pytest.raises(ValueError, match="Ошибка декодирования снапшота"):
            snapshotter.load()

    def test_multiple_dump_load_cycles(self, temp_snapshot_file):
        """Тест множественных циклов сохранения и загрузки"""
        snapshotter = Snapshotter(temp_snapshot_file)
        
        for i in range(5):
            data = {f"key{i}": f"value{i}"}
            snapshotter.dump(data)
            loaded = snapshotter.load()
            assert loaded == data

    def test_dump_large_dataset(self, temp_snapshot_file):
        """Тест сохранения большого объема данных"""
        snapshotter = Snapshotter(temp_snapshot_file)
        # Создаем большой датасет
        data = {f"key{i}": f"value{i}" * 100 for i in range(1000)}
        
        snapshotter.dump(data)
        loaded = snapshotter.load()
        
        assert len(loaded) == 1000
        assert loaded["key0"] == "value0" * 100

    def test_special_characters_in_values(self, temp_snapshot_file):
        """Тест сохранения специальных символов"""
        snapshotter = Snapshotter(temp_snapshot_file)
        data = {
            "quotes": 'He said "hello"',
            "backslash": "path\\to\\file",
            "newlines": "line1\nline2\nline3",
            "tabs": "col1\tcol2\tcol3"
        }
        
        snapshotter.dump(data)
        loaded = snapshotter.load()
        
        assert loaded == data

    def test_null_values(self, temp_snapshot_file):
        """Тест сохранения None значений"""
        snapshotter = Snapshotter(temp_snapshot_file)
        data = {
            "null_value": None,
            "normal_value": "test"
        }
        
        snapshotter.dump(data)
        loaded = snapshotter.load()
        
        assert loaded["null_value"] is None
        assert loaded["normal_value"] == "test"

