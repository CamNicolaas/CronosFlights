import asyncio, dotenv
from typing import Optional, Tuple, List
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaError

from utils.tools import SingletonClass
from utils.configs import AsyncConfigManager
from utils.exceptions import (
    KafkaCannotConnectError,
    KafkaCannotDisconnectError,
    KafkaCreaterTopicsError
)



class KafkaTopicsManager(metaclass = SingletonClass):
    def __init__(self):
        self._configs = dotenv.dotenv_values()
        self.client:Optional[AIOKafkaAdminClient] = None
        self._message:str = f'[Kafka Topics Manager]'

# ---------- Manage Broker ----------
    async def connect_broker(self) -> None:
        try:
            if not self.client:
                self.client = AIOKafkaAdminClient(
                    bootstrap_servers = self._configs.get("KAFKA_HOST", "localhost:9092")
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
                await self.client.close()
                self.client = None

        except KafkaError as err:
            raise KafkaCannotDisconnectError(
                f'{self._message} Error During Disconnection Broker...',
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )


# ---------- Settings ----------
    async def _load_topic_creator(self) -> List[dict]:
        config_loader = AsyncConfigManager()
        return await config_loader.get_configs(
            "monitor_configs", "flights", "aerolineas_argentinas", "kafka_topics"
        )


# ---------- Main Method ----------
    async def create_topics_default(self) -> Tuple[bool, str]:
        try:
            await self.connect_broker()

            topic_creator = await self._load_topic_creator()
            existing_topics = await self.client.list_topics()
            new_topics = list()

            for topic in topic_creator:
                topic_name = topic_creator[topic]["name"]
                if topic_name not in existing_topics:
                    new_topics.append(
                        NewTopic(
                            name = topic_name,
                            num_partitions = topic["partitions"],
                            replication_factor = topic["replication_factor"]
                        )
                    )
            if not new_topics:
                return True, f'{self._message} No New Topics To Create. All Topics Already Exist.'

            await self.client.create_topics(new_topics)
            return True, f'{self._message} New Created Topics: {", ".join(t.name for t in new_topics)}'

        except KafkaError as err:
            raise KafkaCreaterTopicsError(
                message = f'{self._message} Fatal Error During Topic Creation...',
                context = {
                    "error_type": type(err).__name__, 
                    "error_msg": str(err)
                }
            )
        finally:
            await self.disconnect_broker()

