import asyncio
from pathlib import Path
from typing import Optional, Union, Any, Dict

from utils.DB import AsyncFlightDBManager
from utils import (
    AsyncMessageHandler,
    AsyncConfigManager,
    NotifyDiscord,
    KafkaConsumerManager,
    NotifyFlightsError
)
from utils import (
    AsyncMessageHandler,
    AsyncConfigManager,
    NotifyDiscord
)


class NotifyerFlights:
    def __init__(self,
        log_name:Optional[str] = "ConsumerNotifys", 
        log_path:Optional[Union[str, Path]] = "./aerolineasARG/Notifys"
    ):
        # --- Configs ---
        self.configs:Dict[str, Dict[str, Any]] = dict()
        # --- Notify System ---
        self.notifyer = NotifyDiscord()
        # --- Kafka Consumer ---
        self.kafka_consumer:KafkaConsumerManager = None
        # --- DB Flights ---
        self.db_flights = AsyncFlightDBManager()
        # --- Logger ---
        self.logger = AsyncMessageHandler(
            log_filename = log_name,
            logs_folder = log_path,
            printer_msg = "[AerolineasArg][Notifys Flights] Status:"
        )


    async def load_configs(self) -> None:
        await self.db_flights.connect_db()

        configs = AsyncConfigManager()
        self.configs = {
            "admin": await configs.get_configs("monitor_configs", "admin_configs", "webhooks"),
            "general": await configs.get_configs("monitor_configs", "flights", "aerolineas_argentinas"),
            "kafka_topic_notifyer": await configs.get_configs("monitor_configs", "flights", "aerolineas_argentinas", "kafka_topics", "etl_to_notifier"),
            "webhooks": await configs.get_configs("monitor_configs", "flights", "aerolineas_argentinas", "discord_webhooks")
        }
        self.kafka_consumer = KafkaConsumerManager(
            topic = self.configs["kafka_topic_notifyer"]["name"],
            group_id = self.configs["kafka_topic_notifyer"]["group_id"],
            max_workers = self.configs["kafka_topic_notifyer"]["max_workers"]
        )
        await self.kafka_consumer.connect_broker()


# ---------- Main Method ----------
    async def start_module(self):
        try:
            await self.load_configs()
            await self.kafka_consumer.consume_messages(self.handler)
        
        except Exception as err:
            message_error = f'Error Fatal, Kill Process | Type: {type(err).__name__} | Message: {str(err)}'
            # --- Send Status to Discord ---
            _, response_webhook = await self.notifyer.error_admin(
                URL_Webhook = self.configs["admin"]["status_monitors"],
                store_data = self.configs["general"],
                problem_logs = f'[AerolineasArg][Notifys Flights] Status: {message_error}'
            )
            # --- Save Log ---
            await self.logger.error(
                message = message_error,
                hidden_msg = response_webhook
            )
            raise
        
        finally:
            await self.db_flights.disconnect_db()
            await self.kafka_consumer.disconnect_broker()
            await self.logger.shutdown()


# ---------- Manage Flight Data ----------
    # --- Checker Flight ---
    async def handler(self,
        message:Dict[str, Any]
    ) -> None:

        try:
            flight_hash = message["hash_id"]
            flight_price = message["offers"][0]["price"]
            discord_channel = self.configs["webhooks"].get(message["iata_origin"])
            
            if not await self.db_flights.was_notification_sent(
                flight_hash = flight_hash, 
                price = flight_price,
                notified_channel = discord_channel
            ):
                parser_flight = await self.prepare_data(message)
                # --- Send Notify to Discord ---
                status_webhook, response_webhhok = await self.notifyer.flights_deals(
                    URL_Webhook = discord_channel,
                    flight_data = parser_flight
                )
                # --- Save Notify in DB ---
                await self.db_flights.mark_notification_sent(
                    flight_hash =  flight_hash, 
                    price = flight_price,
                    notified_channel = discord_channel
                )
                await self.logger.success(
                    message = f'Flight Has Been Notified And Saved To Database Successfully | Price: {flight_price} | HashID: {flight_hash}',
                    hidden_msg = f'Discord Status: {status_webhook} | Discord Response: {response_webhhok}'
                )

        except Exception as err:
            raise NotifyFlightsError(
                f'Unknown Error Occurred In Message Handler | Message: {message}',
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )

    # --- Prepair Data Flight ---
    async def prepare_data(self, 
        flight_data:Dict[str, Any]
    ) -> Dict[str, str]:

        def formater_time(
            total_minutes: int
        ) -> str:
            hours = total_minutes // 60
            minutes = total_minutes % 60
            return f"{hours}h {minutes}m"

        try:
            airport_origin = await self.db_flights.get_airport_info(flight_data["iata_origin"])
            airport_departure = await self.db_flights.get_airport_info(flight_data["iata_destination"])

            sections = flight_data.get("sections", [])
            sections_webhook = list()
            for i, section in enumerate(sections):
                flight_number = section["flightNumber"]
                origin_code = section["origin"]
                destination_code = section["destination"]

                section_time_departure = self.db_flights.parse_departure_date(
                    section["departure"], 
                    return_type = "datetime"
                )
                section_time_arrival = self.db_flights.parse_departure_date(
                    section["arrival"], 
                    return_type = "datetime"
                )

                iata_origen = await self.db_flights.get_airport_info(origin_code)
                iata_destino = await self.db_flights.get_airport_info(destination_code)

                sections_webhook.extend([
                    f"✈️ Avión: [{flight_number}](https://www.flightaware.com/live/flight/{flight_number}/history)\n",
                    f"📍 Origen: {origin_code} - {iata_origen.get('city', 'N/D')}\n",
                    f"📍 Destino: {destination_code} - {iata_destino.get('city', 'N/D')}\n",
                    f"🕒 Salida: {self.notifyer.create_timestamp_discord(section_time_departure.strftime('%Y-%m-%d %H:%M'))}\n",
                    f"🕓 Llegada: {self.notifyer.create_timestamp_discord(section_time_arrival.strftime('%Y-%m-%d %H:%M'))}\n\n",
                    ""
                ])
                if i < len(sections) - 1:
                    next_departure = self.db_flights.parse_departure_date(
                        sections[i + 1]["departure"], 
                        return_type = "datetime"
                    )
                    wait_minutes = int((next_departure - section_time_arrival).total_seconds() // 60)
                    wait_str = formater_time(wait_minutes)
                    sections_webhook.append(f"⏳ Escala N°{i+1} - Espera {wait_str}\n")

            offer = flight_data["offers"][0]
            flight_class = offer.get("class", None)
            price = offer.get("price", "N/A")
            seat_available = offer.get("seatAvailability", None)
            url_deal = self.create_url_offer(
                provider = flight_data["provider"],
                iata_origin = flight_data["iata_origin"],
                iata_destination = flight_data["iata_destination"],
                time_departure = flight_data["time_departure"]
            )

            alert_description = "".join([
                alert.get("description", "")
                for alert in flight_data.get("alerts", {}).values()
                if alert.get("active") and "description" in alert
            ])


            departure_date = self.db_flights.parse_departure_date(
                flight_data["time_departure"], 
                return_type = "datetime"
            )
            departure_date_discord = self.notifyer.create_timestamp_discord(departure_date)

            arrival_date = self.db_flights.parse_departure_date(
                flight_data["time_arrival"], 
                return_type = "datetime"
            )
            arrival_date_discord = self.notifyer.create_timestamp_discord(arrival_date)

            return {
                "title": f'Origen: {airport_origin["city"]} Destino: {airport_departure["city"]} {f'| Clase: {flight_class}' if flight_class else None}',
                "url_deal": url_deal,
                "airline": flight_data["airline"],
                "provider": flight_data["provider"],
                "description": alert_description,
                "origin": f'[{airport_origin["iata_code"]}](https://www.google.com/maps?q={airport_origin["latitude"]},{airport_origin["longitude"]}) - {airport_origin["city"]}\n{departure_date_discord}',
                "destination": f'[{airport_departure["iata_code"]}](https://www.google.com/maps?q={airport_departure["latitude"]},{airport_departure["longitude"]}) - {airport_departure["city"]}\n{arrival_date_discord}',
                "scale": "`Si.`" if flight_data.get("scale") else "`No.`",
                "duration": f'`{formater_time(fly_duration)}`' if (fly_duration := flight_data.get("total_duration", None)) else None,
                "seat_available": f'`{seat_available} asiento(s)`' if seat_available else None,
                "price": f'`${price}`',
                "sections": "".join(sections_webhook)
            }

        except Exception as err:
            raise NotifyFlightsError(
                f'Error Occurred While Preparing Flight Data | Flight Data: {flight_data}',
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )

    # --- Gen URL Provider ---
    def create_url_offer(self,
        provider:str,
        iata_origin:str,
        iata_destination:str,
        time_departure:str
    ) -> str:

        format_date = self.db_flights.parse_departure_date(
            time_departure, 
            return_type = "date"
        )
        providers_dict = {
            "AerolineasARG": (
                "https://www.aerolineas.com.ar/flights-offers?"
                "adt=1&inf=0&chd=0&flexDates=false&cabinClass=Economy&flightType=ONE_WAY"
                f"&leg={iata_origin}-{iata_destination}-{format_date.strftime("%Y%m%d")}"
            )
            #Other Providers Coming Soon...
            #"Despegar": (
            #    "https://www.despegar.com.ar/shop/flights/results/oneway/"
            #    f"{iata_origin}/{iata_destination}/{str(format_date)}/1/0/0?from=SB&di=1&reSearch=true"
            #)
        }
        return providers_dict.get(provider, None)



if __name__ == "__main__":
    async def main():
        monitor = NotifyerFlights()
        await monitor.start_module()

    asyncio.run(main())
