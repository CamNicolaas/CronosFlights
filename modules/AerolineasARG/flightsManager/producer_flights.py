import asyncio, random
from pathlib import Path
from itertools import product
from asyncio import Condition
from timeit import default_timer
from datetime import date, timedelta
from typing import Optional, Union, Any, Dict, Tuple, List

from utils.DB import DBTokensManager, AsyncFlightDBManager
from modules.AerolineasARG import AerolineasScraper, FlightQueryParams
from utils import (
    AsyncMessageHandler,
    AsyncConfigManager,
    NotifyDiscord,
    KafkaProducerManager,

    # --- Exceptions ---
    ExpiredTokenAPI,
    RequestsBlocked,
    InvalidPastDate,
    GdsResponseError,
    SiteServiceDown,
    InvalidRequests
)


class AerolineasProducerScraperFlights(AerolineasScraper):
    def __init__(self,
        log_name:Optional[str] = "ProducerFlightsCalendar", 
        log_path:Optional[Union[str, Path]] = "./aerolineasARG/FlightsManagers"
    ):
        super().__init__()

        # --- Configs ---
        self.configs:Dict[str, Dict[str, Any]] = dict()
        # --- Notify System ---
        self.notifyer = NotifyDiscord()
        # --- Kafka Producer ---
        self.kafka_producer = KafkaProducerManager()
        # --- Tasks Attribute ---
        self.semaphore: asyncio.Semaphore = None
        self.queue = asyncio.Queue()
        # --- DB Tokens & Flights ---
        self.db_flights = AsyncFlightDBManager()
        self.db_tokens = DBTokensManager()
        # --- Tokens Attribute ---
        self.active_bearerTokens = list()
        self._token_condition = Condition()
        # --- Logger ---
        self.logger = AsyncMessageHandler(
            log_filename = log_name,
            logs_folder = log_path,
            printer_msg = "[AerolineasArg][Producer Flights] Status:"
        )

# ---------- Load Configs ----------
    async def load_configs(self) -> None:
        configs = AsyncConfigManager()
        self.configs = {
            "admin": await configs.get_configs("monitor_configs", "admin_configs", "webhooks"),
            "general": await configs.get_configs("monitor_configs", "flights", "aerolineas_argentinas"),
            "kafka_topic": await configs.get_configs("monitor_configs", "flights", "aerolineas_argentinas", "kafka_topics", "producer_to_etl"),
            "airports_list": await configs.get_configs("monitor_configs", "flights", "aerolineas_argentinas", "airports_scraping")
        }
        self.max_workers = self.configs["general"].get("max_threads_monitor_month", 1)
        self.semaphore = asyncio.Semaphore(self.max_workers)

        await self.db_tokens.connect_db()
        await self.db_flights.connect_db()
        await self.kafka_producer.connect_broker()


# ---------- Main Method. ----------
    async def init_producer(self) -> None:
        try:
            # --- Configs ---
            await self.load_configs()
            await self.logger.critical('Init Module Aerolineas Flights Producer')

            # --- Load Bearer Tokens ---
            asyncio.create_task(self.load_bearerTokens())
            await self.wait_load_random_tokens()
            await self.logger.success('Valid Bearer Token Loaded. Starting process...')

            while True:
                start_time = default_timer()

                await self.run_tasks()

                await self.logger.critical(
                    f'Finish Cycle, Restart Process... | Lap time: {type(self).format_time_task(default_timer() - start_time)}'
                )

        except Exception as err:
            message_error = f'Error Fatal, Kill Process | Type: {type(err).__name__} | Message: {str(err)}'
            # --- Send Status to Discord ---
            _, response_webhook = await self.notifyer.error_admin(
                URL_Webhook = self.configs["admin"]["status_monitors"],
                store_data = self.configs["general"],
                problem_logs = message_error
            )
            # --- Save Log ---
            await self.logger.error(
                message = message_error,
                hidden_msg = response_webhook
            )

        finally:
            await self.db_tokens.disconnect_db()
            await self.db_flights.disconnect_db()
            await self.kafka_producer.disconnect_broker()
            await self.logger.shutdown()

    # --- Prepair & Run All Tasks ---
    async def run_tasks(self) -> None:
        # --- Dates & Routes to Fetch Flights ---
        routes = type(self).create_routes(self.configs["airports_list"].items())
        dates = type(self).gen_flight_calendar_dates(self.configs["general"]["max_month_scraping"])
        # --- Prepair Data Tasks ---
        await self.enqueue_tasks(routes, dates)

        workers = [
            asyncio.create_task(self._worker(i))
            for i in range(self.max_workers)
        ]
        await self.logger.info(f'Started {self.max_workers} Workers...')

        try:
            await self.queue.join()

        except Exception as err:
            await self.logger.critical(
                f'Error While Joining Queue | Type: {type(err).__name__} | Message: {str(err)}'
            )
            raise

        finally:
            for w in workers:
                w.cancel()
            await asyncio.gather(
                *workers, 
                return_exceptions = True
            )
            await self.logger.info('All Workers Completed...')

    # --- Create Tasks ---
    async def enqueue_tasks(self, 
        routes:List[Tuple[str, str]], 
        dates:List[date]
    ) -> None:

        count = 0
        for origin, destination in routes:
            for departure_date in dates:
                await self.queue.put((origin, destination, departure_date))
                count += 1
        await self.logger.info(f'Enqueued {count} Tasks | Routes: {len(routes)} - Month: {len(dates)}')

    # --- Run Task ---
    async def _worker(self, 
        worker_id:int
    ) -> None:

        while True:
            try:
                item = await self.queue.get()
            except asyncio.CancelledError:
                break
            
            # --- Flight Data ---
            origin, destination, departure_date = item
            try:
                await self.fetch_calendar(origin, destination, departure_date)

                await self.logger.success(
                    f'[Worker {worker_id}] Finished {origin}->{destination} on {departure_date}',
                    to_file = False
                )
            except Exception as err:
                await self.logger.critical(
                    f'[Worker {worker_id}] Failed Task: {origin}->{destination} on {departure_date}| Type: {type(err).__name__} | Message: {str(err)}'
                )
                raise
            except asyncio.CancelledError:
                break

            finally:
                self.queue.task_done()


# ---------- Scraping Data ----------
    # --- Fetch Method  ---
    async def fetch_calendar(self, 
        origin:str, 
        destination:str, 
        departure_date:date
    ) -> None:
        
        bearer_token = await self.wait_load_random_tokens()
        async with self.semaphore:
            try:
                flights_status, flights_response = await self.get_flights_month_calendar(
                    FlightQueryParams(
                        date = departure_date.strftime("%Y%m%d"),
                        fly_from = origin,
                        fly_to = destination
                    ),
                    bearer_token = bearer_token
                )
                if not isinstance(flights_response, dict):
                    print(flights_response)
                
                if (
                    not flights_status
                    or not isinstance(flights_response, dict)
                    or not flights_response.get("success", False)
                ):
                    return

                # --- Kafka Producer Publish ---
                await self.kafka_producer.publish_message(
                    topic = self.configs["kafka_topic"]["name"],
                    message = flights_response,
                    key = flights_response["params"]["fly_from"]
                )

            except (RequestsBlocked, InvalidPastDate, ExpiredTokenAPI, GdsResponseError, SiteServiceDown, InvalidRequests) as err:
                await self.logger.error(
                    f'Error Scraping Flight Calendar. Task: {origin}->{destination} on {departure_date} | Type: {type(err).__name__} | Message: {str(err)}',
                    to_file = False
                )

            except Exception as err:
                await self.logger.critical(
                    f'Fatal Unknown Error While Scraping Flight Calendar. Task: {origin}->{destination} on {departure_date} | Type: {type(err).__name__} | Message: {str(err)}'
                )
                raise

            finally:
                await asyncio.sleep(self.configs["general"]["delay_error"])
                return


# ---------- Tokens Methods ----------
    # --- Load Bearer Tokens ---
    async def load_bearerTokens(self) -> None:
        flag_message = True
        
        while True:
            db_response = await self.get_active_tokens(flag_message)
            if not db_response:
                flag_message = False
                await asyncio.sleep(self.configs["general"]["delay"])
                continue
            
            if type(self).checker_tokens(self.active_bearerTokens, db_response):
                async with self._token_condition:
                    self.active_bearerTokens = db_response
                    self._token_condition.notify_all()
                await self.logger.info(f'Updated Bearer Tokens: {len(db_response)} active.')
            await asyncio.sleep(10)

    # --- Get All Tokens Active ---
    async def get_active_tokens(self,
        flag_message:bool
    ) -> Union[List[Dict], List]:
        
        db_status, db_response = await self.db_tokens.get_tokens(only_active = True)
        if not db_status or not db_response:
            if flag_message:
                await self.logger.warning('Waiting New Tokens For Testing...')
            return []
        return db_response

    # --- Checker Dif. Tokens List ---
    @staticmethod
    def checker_tokens(current, new):
        return sorted(current, key=lambda x: x["bearer_token"]) != sorted(new, key=lambda x: x["bearer_token"])

    # --- Wait Random Token ---
    async def wait_load_random_tokens(self) -> str:
        async with self._token_condition:
            while not self.active_bearerTokens:
                await self._token_condition.wait()

            return random.choice(self.active_bearerTokens)["bearer_token"]


# ---------- Tools Methods ----------
    # --- Create Calendar To Scrape ---
    @staticmethod
    def gen_flight_calendar_dates(
        months_ahead:int = 6
    ) -> List[date]:
    
        today = date.today()
        dates_list = list()

        if today.day < 16:
            dates_list.append(date(today.year, today.month, 16))
        else:
            tomorrow = today + timedelta(days = 1)
            if tomorrow.month == today.month:
                dates_list.append(tomorrow)

        for i in range(months_ahead):
            month = today.month + 1 + i
            year = today.year + (month - 1) // 12
            month = (month - 1) % 12 + 1
            dates_list.append(date(year, month, 16))

        return dates_list

    # --- Create & Discard Routes ---
    @staticmethod
    def create_routes(
        airports_list:List[str]
    ) -> List[Tuple[str, str]]:
        
        # --- Discard Routes ---
        iata_codes = {
            airport: city
            for city, airports in airports_list
            if "_" not in city
            for airport in airports
        }
        return [
            (origin, destination)
            for origin, destination in product(iata_codes.keys(), repeat = 2)
            if origin != destination and iata_codes[origin] != iata_codes[destination]
        ]

    # --- Time Tasks Cycle ---
    @staticmethod
    def format_time_task(
        seconds:float
    ) -> str:
        hours, rem = divmod(seconds, 3600)
        minutes, seconds = divmod(rem, 60)
        return f"{int(hours)}h {int(minutes)}m {seconds:.3f}s"


if __name__ == "__main__":
    async def main():
        monitor = AerolineasProducerScraperFlights()
        await monitor.init_producer()

    asyncio.run(main())
