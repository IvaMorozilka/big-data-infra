import json
import os
from typing import Any, List, Dict
from app.core.interfaces import IWriteAheadLog


class FileWal(IWriteAheadLog):
    """
    Write-Ahead Log (WAL) для журналирования операций перед их выполнением.
    """

    def __init__(self, file_path: str = "data/wal.log"):
        self.file_path = file_path
        # Создаем директорию, если она не существует
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        # Создаем файл, если он не существует
        if not os.path.exists(self.file_path):
            open(self.file_path, 'w').close()

    def log(self, operation: Dict[str, Any]) -> None:
        """Логирует одну операцию в журнал."""
        try:
            with open(self.file_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(operation, ensure_ascii=False) + '\n')
        except Exception as e:
            raise IOError(f"Ошибка записи в WAL: {e}")

    def replay(self) -> List[Dict[str, Any]]:
        """Читает и возвращает все операции из журнала."""
        operations = []
        
        if not os.path.exists(self.file_path):
            return operations
        
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:  # Пропускаем пустые строки
                        operations.append(json.loads(line))
        except json.JSONDecodeError as e:
            raise ValueError(f"Ошибка декодирования WAL: {e}")
        except Exception as e:
            raise IOError(f"Ошибка чтения WAL: {e}")
        
        return operations

    def compact(self) -> None:
        """Очищает журнал."""
        try:
            with open(self.file_path, 'w') as f:
                pass  # Просто перезаписываем файл пустым содержимым
        except Exception as e:
            raise IOError(f"Ошибка очистки WAL: {e}")

