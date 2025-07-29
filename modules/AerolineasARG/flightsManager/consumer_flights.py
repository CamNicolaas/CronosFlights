import asyncio
from pathlib import Path
from typing import Optional, Union, Any, Dict

from utils.DB import AsyncFlightDBManager
from modules.AerolineasARG import FlightDealAnalyzer
from utils import (
    AsyncMessageHandler,
    AsyncConfigManager,
    NotifyDiscord,
    KafkaProducerManager,
    KafkaConsumerManager
)


class AerolineasConsumerFlights:
    def __init__(self,
        log_name:Optional[str] = "ConsumerFlightsCalendar", 
        log_path:Optional[Union[str, Path]] = "./aerolineasARG/FlightsManagers"
    ):
        # --- Configs ---
        self.configs:Dict[str, Dict[str, Any]] = dict()
        # --- Notify System ---
        self.notifyer = NotifyDiscord()
        # --- Kafka Producer ---
        self.kafka_producer = KafkaProducerManager()
        # --- Kafka Consumer ---
        self.kafka_consumer:KafkaConsumerManager = None
        # --- DB Flights ---
        self.db_flights = AsyncFlightDBManager()
        # --- Checker Deals ---
        self.deals_analyzer:FlightDealAnalyzer = None
        # --- Logger ---
        self.logger = AsyncMessageHandler(
            log_filename = log_name,
            logs_folder = log_path,
            printer_msg = "[AerolineasArg][Consumer Flights] Status:"
        )


    async def load_configs(self) -> None:
        await self.db_flights.connect_db()
        
        configs = AsyncConfigManager()
        self.configs = {
            "admin": await configs.get_configs("monitor_configs", "admin_configs", "webhooks"),
            "general": await configs.get_configs("monitor_configs", "flights", "aerolineas_argentinas"),
            "kafka_topic": await configs.get_configs("monitor_configs", "flights", "aerolineas_argentinas", "kafka_topics", "producer_to_etl"),
            "kafka_topic_notifyer": await configs.get_configs("monitor_configs", "flights", "aerolineas_argentinas", "kafka_topics", "etl_to_notifier"),
            "deals_configs": await configs.get_configs("monitor_configs", "flights", "deals_configs"),
        }
        self.kafka_consumer = KafkaConsumerManager(
            topic = self.configs["kafka_topic"]["name"],
            group_id = self.configs["kafka_topic"]["group_id"],
            max_workers = self.configs["kafka_topic"]["max_workers"]
        )
        self.deals_analyzer = FlightDealAnalyzer(
            configs = self.configs["deals_configs"],
            db_flights = self.db_flights
        )
        await self.kafka_producer.connect_broker()
        await self.kafka_consumer.connect_broker()


# ---------- Main Method. ----------
    async def init_consumer(self) -> None:
        try:
            await self.load_configs()
            await self.logger.critical('Init Module Aerolineas Flights Consumer')

            await self.kafka_consumer.consume_messages(self.handler)

        except Exception as err:
            message_error = f'Error Fatal, Kill Process | Type: {type(err).__name__} | Message: {str(err)}'
            # --- Send Status to Discord ---
            _, response_webhook = await self.notifyer.error_admin(
                URL_Webhook = self.configs["admin"]["status_monitors"],
                store_data = self.configs["general"],
                problem_logs = f'[AerolineasArg][Consumer Flights] Status: {message_error}'
            )
            # --- Save Log ---
            await self.logger.error(
                message = message_error,
                hidden_msg = response_webhook
            )
        finally:
            await self.db_flights.disconnect_db()
            await self.kafka_producer.disconnect_broker()
            await self.kafka_consumer.disconnect_broker()
            await self.logger.shutdown()


# ---------- Process Data ----------
    async def handler(self,
        message:Dict[str, Any]
    ) -> None:
        
        flights = message.get("flights")
        if not flights:
            return
        
        tasks = list()
        for flight in flights:
            tasks.append(
                asyncio.create_task(
                    self.manage_flights(message["provider"], flight)
                )
            )
        await asyncio.gather(*tasks, return_exceptions = True)


    async def manage_flights(self,
        provider:str,
        flight:Dict[str, Any]
    ) -> None:
        
        try:
            hash_id, status = await self.db_flights.insert_or_update_calendar_entry(provider, flight)
            if not status:
                return
            
            # --- Check If Any Alerts Are Active ---
            alerts = await self.deals_analyzer.analyze_deals(flight)
            alerts_actives = [key for key, val in alerts.items() if val["active"]]

            if alerts_actives:
                await self.logger.warning(f'New Flight Deal Found | Alerts: {" , ".join(alerts_actives)} | Details: {flight}')
                flight.update(
                    {
                        "provider": provider,
                        "hash_id": hash_id,
                        "alerts": alerts
                    }
                )
                # --- Send Notify to Checker Notifyer ---
                await self.kafka_producer.publish_message(
                    topic = self.configs["kafka_topic_notifyer"]["name"],
                    message = flight,
                    key = flight["iata_origin"]
                )

        except Exception as err:
            await self.logger.critical(
                f'Unknown Fatal Error While Processing When Check & Saving Deals | Type: {type(err).__name__} | Message: {str(err)}'
            )
            raise


if __name__ == "__main__":
    async def main():
        consumer = AerolineasConsumerFlights()
        await consumer.init_consumer()

    asyncio.run(main())
