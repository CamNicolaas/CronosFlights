import asyncio
from pathlib import Path
from typing import Optional, Union, Dict, Any

from utils.DB import AsyncFlightDBManager
from utils import (
    AsyncMessageHandler,
    AsyncConfigManager
)


class UpdaterFlightsStats():
    def __init__(self,
        log_name:Optional[str] = "UpdaterFlightsStats", 
        log_path:Optional[Union[str, Path]] = "./aerolineasARG/Tools"
    ):
        # --- Configs ---
        self.configs:Dict[str, Dict[str, Any]] = dict()
        # --- DB Manager ---
        self.db_flights = AsyncFlightDBManager()
        # --- Logs ---
        self.logger = AsyncMessageHandler(
            log_filename = log_name,
            logs_folder = log_path,
            printer_msg = "[AerolineasArg][Updater Stats] Status:"
        )


    async def load_configs(self) -> None:
        await self.db_flights.connect_db()
        
        configs = AsyncConfigManager()
        self.configs = {
            "admin": await configs.get_configs("monitor_configs", "admin_configs", "webhooks"),
            "general": await configs.get_configs("monitor_configs", "flights", "aerolineas_argentinas")
        }

    async def start(self):
        try:
            await self.load_configs()
            await self.logger.critical("Status: Init Updater Flights Stats!!")

            await self.db_flights.update_calendar_stats()
            await self.logger.critical("Status: New Flights Stats Was Recorded")

        except Exception as err:
            message_error = f'Error Fatal, Kill Process | Type: {type(err).__name__} | Message: {str(err)}'
            # --- Send Status to Discord ---
            _, response_webhook = await self.notifyer.error_admin(
                URL_Webhook = self.configs["admin"]["status_monitors"],
                store_data = self.configs["general"],
                problem_logs = f'[AerolineasArg][Updater Stats] Status: {message_error}'
            )
            # --- Save Log ---
            await self.logger.error(
                message = message_error,
                hidden_msg = response_webhook
            )
        finally:
            await self.db_flights.disconnect_db()
            await self.logger.shutdown()



if __name__ == "__main__":
    async def main():
        updater = UpdaterFlightsStats()
        await updater.start()
    
    asyncio.run(main())
