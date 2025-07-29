import json, asyncio, dotenv
from typing import Optional, Callable, Awaitable, Any, Set
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError, KafkaError
from utils.tools import SingletonClass
from utils.exceptions import (
    KafkaManagerError,
    KafkaCannotConnectError,
    KafkaCannotDisconnectError,
    KafkaConsumerError
)

class KafkaConsumerManager(metaclass = SingletonClass):
    def __init__(self,
        topic:str,
        group_id:str,
        max_workers:int = 1000
    ):
        self.topic = topic
        self.group_id = group_id
        self._configs = dotenv.dotenv_values()
        self.client:Optional[AIOKafkaConsumer] = None
        
        self.semaphore:asyncio.Semaphore = None
        self.max_workers = max_workers
        self.tasks:Set[asyncio.Task] = set()
        self._running = False
        self._message:str = f'[Kafka Consumer Manager]'

# ---------- Load Configs ----------
    async def load_configs(self) -> None:
        self.semaphore = asyncio.Semaphore(self.max_workers)

# ---------- Manage Broker ----------
    async def connect_broker(self) -> None:
        try:
            if not self.client:
                await self.load_configs()

                self.client = AIOKafkaConsumer(
                    self.topic,
                    bootstrap_servers = self._configs["KAFKA_HOST"],
                    group_id = self.group_id,
                    auto_offset_reset = "latest",
                    enable_auto_commit = True
                )
                await self.client.start()

        except KafkaError as err:
            raise KafkaCannotConnectError(
            f'{self._message} Error During Connection Broker...',
            context = {
                "error_type": type(err).__name__,
                "error_msg": str(err)
            }
        )

    async def disconnect_broker(self) -> None:
        try:
            if self.client:
                await self.client.stop()
                self.client = None
                self._running = False

        except KafkaError as err:
            raise KafkaCannotDisconnectError(
                f'{self._message} Error During Disconnection Broker...',
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )


# ---------- Main Method ----------
    async def consume_messages(self, 
        handler:Callable[[dict], Awaitable[Any]]
    ) -> None:
        
        try:
            await self.connect_broker()
            self._running = True
        
            async for msg in self.client:
                try:
                    message = json.loads(msg.value.decode("utf-8"))
                except Exception as err:
                    # I must find a better way to process these errors
                    #print(f'{self._message} Error Invalid JSON: {err}')
                    continue

                task = asyncio.create_task(self._process_message(message, handler))
                self.tasks.add(task)
                task.add_done_callback(self.tasks.discard)

                if len(self.tasks) >= self.semaphore._value:
                    await asyncio.sleep(0.01)

                if not self._running:
                    break

        except KafkaError as err:
            raise KafkaConsumerError(
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )
        
        finally:
            await self._shutdown_tasks()
            await self.disconnect_broker()

    async def _shutdown_tasks(self) -> None:
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions = True)
        self.tasks.clear()

    async def _process_message(self, 
        message:dict,
        handler:Callable[[dict], Awaitable[Any]]
    ) -> None:
        
        async with self.semaphore:
            try:
                await handler(message)

            except Exception as err:
                raise KafkaManagerError(
                    context = {
                        "error_type": type(err).__name__,
                        "error_msg": str(err)
                    }
                )

