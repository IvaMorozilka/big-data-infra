import json
import os
from typing import Any, Optional, Dict
from src.core.interfaces import IPersistence


class Snapshotter(IPersistence):
    """
    Механизм сохранения и загрузки снапшотов данных в файл.
    """

    def __init__(self, file_path: str = "data/snapshot.json"):
        self.file_path = file_path
        # Создаем директорию, если она не существует
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)

    def dump(self, data: Dict[str, Any]) -> None:
        """Сохраняет снапшот данных на диск."""
        try:
            with open(self.file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            raise IOError(f"Ошибка сохранения снапшота: {e}")

    def load(self) -> Optional[Dict[str, Any]]:
        """Загружает снапшот данных с диска."""
        if not os.path.exists(self.file_path):
            return None
        
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data
        except json.JSONDecodeError as e:
            raise ValueError(f"Ошибка декодирования снапшота: {e}")
        except Exception as e:
            raise IOError(f"Ошибка загрузки снапшота: {e}")

