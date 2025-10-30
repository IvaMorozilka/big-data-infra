import logging
from typing import Any, Optional
from app.core.interfaces import IDatabase, IStorageEngine, IPersistence, IWriteAheadLog

logger = logging.getLogger(__name__)


class KVDB(IDatabase):
    """
    Key-Value База Данных с поддержкой персистентности и WAL.
    """

    def __init__(
        self,
        storage_engine: IStorageEngine,
        persistence: IPersistence,
        wal: IWriteAheadLog,
        auto_snapshot_threshold: int = 100
    ):
        self.storage_engine = storage_engine
        self.persistence = persistence
        self.wal = wal
        self.auto_snapshot_threshold = auto_snapshot_threshold
        self.operation_count = 0
        
        # Инициализация: загружаем данные из снапшота и применяем WAL
        self._initialize()

    def _initialize(self) -> None:
        """Инициализация базы данных: загрузка снапшота и применение WAL."""
        logger.info("Инициализация базы данных...")
        
        # Загружаем снапшот
        snapshot_data = self.persistence.load()
        if snapshot_data:
            logger.info(f"Загружен снапшот с {len(snapshot_data)} записями")
            self.storage_engine.load_data(snapshot_data)
        else:
            logger.info("Снапшот не найден, начинаем с пустой базы данных")
        
        # Применяем операции из WAL
        operations = self.wal.replay()
        if operations:
            logger.info(f"Применение {len(operations)} операций из WAL")
            for operation in operations:
                self._apply_operation(operation)
            # После применения WAL создаем новый снапшот и очищаем WAL
            self._create_snapshot()
            self.wal.compact()
            logger.info("WAL применен и очищен")
        else:
            logger.info("WAL пуст")

    def _apply_operation(self, operation: dict) -> None:
        """Применяет операцию из WAL к движку хранения."""
        op_type = operation.get('type')
        key = operation.get('key')
        
        if op_type == 'set':
            value = operation.get('value')
            self.storage_engine.set(key, value)
        elif op_type == 'delete':
            self.storage_engine.delete(key)

    def _create_snapshot(self) -> None:
        """Создает снапшот текущего состояния данных."""
        data = self.storage_engine.get_all_data()
        self.persistence.dump(data)
        logger.info(f"Создан снапшот с {len(data)} записями")

    def _maybe_snapshot(self) -> None:
        """Проверяет, нужно ли создать снапшот."""
        self.operation_count += 1
        if self.operation_count >= self.auto_snapshot_threshold:
            logger.info(f"Достигнут порог {self.auto_snapshot_threshold} операций, создаем снапшот")
            self._create_snapshot()
            self.wal.compact()
            self.operation_count = 0

    def set(self, key: str, value: Any) -> None:
        """Сохраняет значение по ключу."""
        # Сначала логируем операцию в WAL
        self.wal.log({'type': 'set', 'key': key, 'value': value})
        # Затем выполняем операцию
        self.storage_engine.set(key, value)
        logger.debug(f"SET: {key} = {value}")
        # Проверяем, нужен ли снапшот
        self._maybe_snapshot()

    def get(self, key: str) -> Optional[Any]:
        """Возвращает значение по ключу."""
        value = self.storage_engine.get(key)
        logger.debug(f"GET: {key} = {value}")
        return value

    def delete(self, key: str) -> bool:
        """Удаляет значение по ключу."""
        # Сначала логируем операцию в WAL
        self.wal.log({'type': 'delete', 'key': key})
        # Затем выполняем операцию
        result = self.storage_engine.delete(key)
        logger.debug(f"DELETE: {key} - {'успешно' if result else 'ключ не найден'}")
        # Проверяем, нужен ли снапшот
        self._maybe_snapshot()
        return result

    def shutdown(self) -> None:
        """Корректное завершение работы: создание финального снапшота."""
        logger.info("Завершение работы базы данных...")
        self._create_snapshot()
        self.wal.compact()
        logger.info("База данных завершила работу")

