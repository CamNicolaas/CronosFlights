from typing import Any, Optional
import asyncio, json, os, aiofiles
from utils.tools import SingletonClass


class AsyncConfigManager(metaclass = SingletonClass):
    def __init__(self,
        config_path:Optional[str] = "general_configs.json"
    ):
        if getattr(self, "_initialized", False):
            return

        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.config_path = os.path.join(base_dir, config_path)

        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f'Error, Config. File Not Found In: {self.config_path}')

        self._config_data:Optional[dict[str, Any]] = None
        self._lock = asyncio.Lock()
        self._initialized = True


    async def _load_config(self) -> None:
        if self._config_data:
            return

        async with self._lock:
            if not self._config_data:
                async with aiofiles.open(self.config_path, mode = "r", encoding = "utf-8") as file:
                    content = await file.read()
                    self._config_data = json.loads(content)


# ---------- Methods Configs ----------
    # --- Return Specific Configs ---
    async def get_configs(self, *keys: str) -> Optional[Any]:
        await self._load_config()
        data = self._config_data

        for key in keys:
            if isinstance(data, dict) and key in data:
                data = data[key]
            else:
                return None
        return data


    # --- Return All Configs ---
    async def get_all_configs(self) -> dict[str, Any]:
        await self._load_config()
        return self._config_data


    # --- Reload Configs ---
    async def reload_config(self) -> None:
        async with self._lock:
            async with aiofiles.open(self.config_path, mode = "r", encoding = "utf-8") as f:
                content = await f.read()
                self._config_data = json.loads(content)
