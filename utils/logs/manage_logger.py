import asyncio, os, re
from typing import Optional
from datetime import datetime
from aiologger import Logger
from aiologger.handlers.files import AsyncFileHandler


class Colors:
    WHITE = '\033[97m'
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    PURPLE = '\033[95m'
    RESET = '\033[0m'


def get_time():
    return datetime.now().strftime("%m-%d-%Y %H:%M:%S")


def remove_ansi(text):
    return re.sub(r'\x1b\[[0-9;]*m', '', text)


class AsyncMessageHandler:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, 
        log_filename:Optional[str] = "",
        logs_folder:Optional[str] = "",
        printer_msg:str = ""
    ):
        
        self.printer_msg = printer_msg
        if hasattr(self, "initialized"):
            return

        self.queue = asyncio.Queue()
        self.logger = Logger.with_default_handlers(name = "CronosLogger")
        if log_filename and logs_folder:
            base_logs_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../logs"))
            log_dir = os.path.join(base_logs_dir, logs_folder)
            log_file_path = os.path.join(log_dir, f"{log_filename}.log")
            os.makedirs(log_dir, exist_ok = True)

            file_handler = AsyncFileHandler(log_file_path)
            self.logger.handlers.clear()
            self.logger.add_handler(file_handler)

        self.task = asyncio.create_task(self._log_worker())
        self.initialized = True


    async def _log_worker(self):
        while True:
            msg, level = await self.queue.get()
            clean_msg = remove_ansi(msg)

            if level == "INFO":
                await self.logger.info(clean_msg)
            elif level == "ERROR":
                await self.logger.error(clean_msg)
            elif level == "WARNING":
                await self.logger.warning(clean_msg)
            elif level == "CRITICAL":
                await self.logger.critical(clean_msg)
            elif level == "SUCCESS":
                await self.logger.info(clean_msg)

            self.queue.task_done()


    def set_printer_msg(self, 
        new_msg:str
    ):
        self.printer_msg = new_msg


# ---------- Logs Message Methods ----------
    # --- manager msg ---
    async def log(self, 
        message:str, 
        level:str = "INFO",
        remove_end:Optional[bool] = False,
        hidden_msg:Optional[str] = None,
        to_file:Optional[bool] = True
    ):

        await asyncio.to_thread(print, f"[{get_time()}]{self.printer_msg} {message}", end="" if remove_end else "\n")
        if to_file:
            await self.queue.put(
                (
                    f"[{get_time()}] [{level}]{self.printer_msg} {message} {(f'| Details: {hidden_msg}' if hidden_msg else '')}", 
                    level
                )
            )


    async def info(self, 
        message:str, 
        **kwargs
    ):
        await self.log(f"{Colors.WHITE}{message}{Colors.RESET}", level = "INFO", **kwargs)

    async def success(self,
        message:str, 
        **kwargs
    ):
        await self.log(f"{Colors.GREEN}{message}{Colors.RESET}", level = "SUCCESS", **kwargs)

    async def warning(self, 
        message:str, 
        **kwargs
    ):
        await self.log(f"{Colors.YELLOW}{message}{Colors.RESET}", level = "WARNING", **kwargs)

    async def error(self, 
        message:str, 
        **kwargs
    ):
        await self.log(f"{Colors.RED}{message}{Colors.RESET}", level = "ERROR", **kwargs)

    async def critical(self, 
        message:str, 
        **kwargs
    ):
        await self.log(f"{Colors.PURPLE}{message}{Colors.RESET}", level = "CRITICAL", **kwargs)

    async def printer(self, 
        message:str, 
        type_msg:str = "info",
        **kwargs
    ):

        methods = {
            "info": self.info,
            "error": self.error,
            "success": self.success,
            "warning": self.warning,
            "critical": self.critical,
        }
        await methods.get(type_msg, self.info)(f'{self.printer_msg if self.printer_msg else ""} {message}', **kwargs)

    async def shutdown(self):
        await self.queue.join()
        await self.logger.shutdown()
