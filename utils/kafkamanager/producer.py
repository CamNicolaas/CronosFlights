import json, asyncio, dotenv
from datetime import datetime
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from typing import Optional, Tuple, Union

from utils.tools import SingletonClass
from utils.exceptions import (
    KafkaCannotConnectError,
    KafkaCannotDisconnectError,
    KafkaProducerError
)


class KafkaProducerManager(metaclass = SingletonClass):
    def __init__(self):
        self._configs = dotenv.dotenv_values()
        self.client: Optional[AIOKafkaProducer] = None
        self._message:str = f'[Kafka Producer Manager]'


# ---------- Manage Broker ----------
    async def connect_broker(self) -> None:
        try:
            if not self.client:
                self.client = AIOKafkaProducer(
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
                await self.client.stop()
                self.client = None

        except KafkaError as err:
            raise KafkaCannotDisconnectError(
                f'{self._message} Error During Disconnection Broker...',
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )


# ---------- Main Method ----------
    async def publish_message(self,
        topic:str,
        message:dict,
        key:str = None
    ) -> Tuple[bool, str]:
        
        if not isinstance(message, dict):
            raise ValueError(f'{self._message} Message Must Be A Dictionary')

        try:
            await self.connect_broker()

            await self.client.send_and_wait(
                topic = topic,
                value = json.dumps(message, default = self.serialize_flight).encode("utf-8"),
                key = key.encode("utf-8") if key else None,
            )
            return True, f'{self._message} Message Successfully Send | Topic: "{topic}"'

        except KafkaError as err:
            raise KafkaProducerError(
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )


# ---------- Tools  ----------
    def serialize_flight(self, 
        obj:Union[datetime, object]
    ) -> str:
        
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Error Serializing Message. | Type: {type(obj)}")

