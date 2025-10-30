from app.core.collection import Collection
from app.core.database import KVDB
from app.core.persistence import Snapshotter
from app.core.storage import InMemoryStorage
from app.core.wal import FileWal


def create_db(snapshot_path, wal_path, threshold=100):
    return KVDB(
        storage_engine=InMemoryStorage(),
        persistence=Snapshotter(snapshot_path),
        wal=FileWal(wal_path),
        auto_snapshot_threshold=threshold
    )


class TestCollection:

    def test_set_and_get(self, db):
        """Тест базовых операций set и get в коллекции"""
        collection = Collection(db, "users")
        collection.set("user1", {"name": "name1", "age": 24})

        user = collection.get("user1")
        assert user == {"name": "name1", "age": 24}

    def test_get_nonexistent_key(self, db):
        """Тест получения несуществующего ключа из коллекции"""
        collection = Collection(db, "users")
        assert collection.get("Bob") is None

    def test_delete(self, db):
        """Тест удаления из коллекции"""
        collection = Collection(db, "users")
        collection.set("user1", {"name": "name1"})

        result = collection.delete("user1")
        assert result is True
        assert collection.get("name1") is None

    def test_delete_nonexistent(self, db):
        """Тест удаления несуществующего ключа"""
        collection = Collection(db, "users")
        result = collection.delete("Bob")
        assert result is False

    def test_count_empty(self, db):
        """Тест подсчета элементов в пустой коллекции"""
        collection = Collection(db, "users")
        assert collection.count() == 0

    def test_count_with_items(self, db):
        """Тест подсчета элементов в коллекции"""
        collection = Collection(db, "users")
        collection.set("user1", {"name": "name1"})
        collection.set("user2", {"name": "name2"})
        collection.set("user3", {"name": "name3"})

        assert collection.count() == 3

    def test_exists(self, db):
        """Тест проверки существования ключа"""
        collection = Collection(db, "users")
        collection.set("user1", {"name": "name1"})

        assert collection.exists("user1") is True
        assert collection.exists("bob") is False

    def test_get_all_empty(self, db):
        """Тест получения всех данных из пустой коллекции"""
        collection = Collection(db, "users")
        assert collection.get_all() == {}

    def test_get_all_with_items(self, db):
        """Тест получения всех данных из коллекции"""
        collection = Collection(db, "users")
        collection.set("user1", {"name": "name1"})
        collection.set("user2", {"name": "name2"})

        all_data = collection.get_all()
        assert len(all_data) == 2
        assert all_data["user1"] == {"name": "name1"}
        assert all_data["user2"] == {"name": "name2"}

    def test_isolation_between_collections(self, db):
        """Тест изоляции между различными коллекциями"""
        users = Collection(db, "users")
        products = Collection(db, "products")

        users.set("user1", {"name": "name1"})
        products.set("p1", {"price": 99999})

        assert users.count() == 1
        assert products.count() == 1
        assert users.get("p1") is None
        assert products.get("user1") is None

    def test_same_key_different_collections(self, db):
        """Тест использования одинаковых ключей в разных коллекциях"""
        col1 = Collection(db, "collection1")
        col2 = Collection(db, "collection2")

        col1.set("key", "value1")
        col2.set("key", "value2")

        assert col1.get("key") == "value1"
        assert col2.get("key") == "value2"

    def test_prefix_in_database(self, db):
        """Ключи хранятся с префиксом в базе данных"""
        collection = Collection(db, "users")
        collection.set("user1", {"name": "name1"})

        # Прямой доступ к базе с префиксом
        assert db.get("users:user1") == {"name": "name1"}

    def test_overwrite_value(self, db):
        """Тест перезаписи значения в коллекции"""
        collection = Collection(db, "users")
        collection.set("user1", {"age": 30})
        collection.set("user1", {"age": 100})

        assert collection.get("user1") == {"age": 100}

    def test_delete_updates_count(self, db):
        """Удаление обновляет счетчик"""
        collection = Collection(db, "users")
        collection.set("alice", {"name": "Alice"})
        collection.set("bob", {"name": "Bob"})

        assert collection.count() == 2

        collection.delete("alice")
        assert collection.count() == 1

    def test_empty_collection_name(self, db):
        """Тест работы с пустым именем коллекции"""
        collection = Collection(db, "")
        collection.set("key", "value")

        assert collection.get("key") == "value"
        assert db.get(":key") == "value"

    def test_complex_nested_data(self, db):
        """Тест хранения сложных вложенных данных"""
        collection = Collection(db, "users")
        user_data = {
            "name": "Alice",
            "age": 30,
            "address": {
                "street": "Main St",
                "city": "New York",
                "coordinates": {"lat": 40.7128, "lon": -74.0060}
            },
            "tags": ["admin", "developer"],
            "active": True
        }

        collection.set("alice", user_data)
        retrieved = collection.get("alice")

        assert retrieved == user_data

    def test_collection_persistence(self, temp_files):
        """Тест персистентности данных коллекции"""
        snapshot_path, wal_path = temp_files

        # Первая сессия
        db1 = create_db(snapshot_path, wal_path)
        users1 = Collection(db1, "users")
        users1.set("user1", {"name": "name1"})
        db1.shutdown()

        # Вторая сессия
        db2 = create_db(snapshot_path, wal_path)
        users2 = Collection(db2, "users")

        assert users2.get("user1") == {"name": "name1"}
        assert users2.count() == 1

    def test_multiple_collections_persistence(self, temp_files):
        """Тест персистентности нескольких коллекций"""
        snapshot_path, wal_path = temp_files

        # Первая сессия
        db1 = create_db(snapshot_path, wal_path)
        users1 = Collection(db1, "users")
        products1 = Collection(db1, "products")

        users1.set("user1", {"name": "name1"})
        products1.set("laptop", {"price": 99999})
        db1.shutdown()

        # Вторая сессия
        db2 = create_db(snapshot_path, wal_path)
        users2 = Collection(db2, "users")
        products2 = Collection(db2, "products")

        assert users2.get("user1") == {"name": "name1"}
        assert products2.get("laptop") == {"price": 99999}
