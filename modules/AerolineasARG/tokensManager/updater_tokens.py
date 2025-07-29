import asyncio
from pathlib import Path
from typing import Optional, Union, Any, Dict, Tuple, List

from utils.DB import DBTokensManager
from modules.AerolineasARG import (
    FlightQueryParams,
    AerolineasScraper,
    random_routes
)
from utils import (
    random_date,
    AsyncMessageHandler,
    AsyncConfigManager,
    NotifyDiscord,
    
    # --- Exceptions ---
    ExpiredTokenAPI,
    RequestsBlocked,
    InvalidPastDate,
    GdsResponseError,
    SiteServiceDown,
)



class UpdaterBearerTokens(AerolineasScraper):
    def __init__(self,
        log_name:Optional[str] = "UpdaterTokens", 
        log_path:Optional[Union[str, Path]] = "./aerolineasARG/TokensManager"
    ):
        super().__init__()

        # --- Notify System ---
        self.notifyer = NotifyDiscord()
        # --- DB Tokens ---
        self.db_tokens = DBTokensManager()
        # --- Tasks Token Checker ---
        self.tasks_tokens:Dict[str, asyncio.Task] = dict()
        # --- Configs ---
        self.configs:Dict[str, Dict[str, Any]] = dict()
        # --- Logger ---
        self.logger = AsyncMessageHandler(
            log_filename = log_name, 
            logs_folder = log_path, 
            printer_msg = "[AerolineasArg][Updater Tokens] Status:"
        )


# ---------- Load Configs ----------
    async def load_configs(self) -> None:
        configs = AsyncConfigManager()
        self.configs = {
            "admin": await configs.get_configs("monitor_configs", "admin_configs", "webhooks"),
            "general": await configs.get_configs("monitor_configs", "flights", "aerolineas_argentinas")
        }
        # --- Init DB Tokens ---
        await self.db_tokens.connect_db()


# ---------- Main Method ----------
    async def init_monitor(self) -> None:
        try:
            # --- Configs ---
            await self.load_configs()
            # --- Init Monitor ---
            await self.logger.critical('Init Module Aerolineas Tokens Updater')

            await self.checker_active_tokens()

        except Exception as err:
            message_error = f'Error Fatal, Kill Process | Type: {type(err).__name__} | Message: {str(err)}'
            # --- Send Status to Discord ---
            _, response_webhook = await self.notifyer.error_admin(
                URL_Webhook = self.configs["admin"]["status_monitors"],
                store_data = self.configs["general"],
                problem_logs = f'[AerolineasArg][Updater Tokens] Status: {message_error}'
            )
            # --- Save Log ---
            await self.logger.critical(
                message = message_error,
                hidden_msg = response_webhook
            )
            raise

        finally:
            await self.db_tokens.disconnect_db()
            await self.logger.shutdown()


# ---------- Process Data ----------
    # --- Checker Tokens ---
    async def checker_active_tokens(self) -> None:
        flag_message = True

        while True:
            db_response = await self.get_active_tokens(flag_message)
            if not db_response:
                flag_message = False
                await asyncio.sleep(self.configs["general"]["delay"])
                continue

            for token_data in db_response:
                bearer_token = token_data["bearer_token"]
                if bearer_token not in self.tasks_tokens or self.tasks_tokens[bearer_token].done():
                    task = asyncio.create_task(self.manager_active_tokens(bearer_token))
                    self.tasks_tokens[bearer_token] = task

            await asyncio.sleep(self.configs["general"]["delay"])

    # --- Manager/Checker if Token Active ---
    async def manager_active_tokens(self,
        bearer_token:str
    ) -> None:

        try:
            status_token = await self.validate_token(bearer_token)
            if not status_token:
                await self.updater_invalid_token(bearer_token)
            return
            
        finally:
            self.tasks_tokens.pop(bearer_token, None)

    # --- Testing Token ---
    async def validate_token(self,
        bearer_token:str
    ) -> bool:
        
        count_retrys:int = 0
        max_retrys = self.configs["general"]["tokens_max_error"]

        for _ in range(max_retrys):
            try:
                status, response_flights = await self.check_token(bearer_token)
                if not status:
                    await self.logger.error(
                        message = f'{response_flights} | Token: ...{bearer_token[-25:]}',
                        to_file = False
                    )
                else:
                    await self.logger.success(
                        f'Token Valid | Token: ...{bearer_token[-25:]}', 
                        to_file = False
                    )
                return True

            except (RequestsBlocked, InvalidPastDate, GdsResponseError, SiteServiceDown) as err:
                await self.logger.error(
                    f'Error Checking "Token Bearer", Token: ...{bearer_token[-25:]} | Type: {type(err).__name__} | Message: {str(err)}',
                    to_file = False
                )
                await asyncio.sleep(self.configs["general"]["delay_error"])

            except ExpiredTokenAPI as err:
                count_retrys += 1
                await self.logger.warning(
                    f'Token Checker Failed ({count_retrys}/{max_retrys}): ...{bearer_token[-25:]} | Type: {type(err).__name__} | Message: {str(err)}'
                )
                if count_retrys >= max_retrys:
                    return False
                await asyncio.sleep(self.configs["general"]["delay_error"])

            except Exception as err:
                await self.logger.critical(
                    f'Fatal Unknown Error While Checking Token | Type: {type(err).__name__} | Message: {str(err)}'
                )
                raise


# ---------- DB Tokens Methods ----------
    # --- Marker Invalid Tokens ---
    async def updater_invalid_token(self,
        bearer_token:str
    ) -> None:
        
        _, db_response = await self.db_tokens.update_token(
            bearer_token = bearer_token
        )
        await self.logger.warning(
            message = db_response,
            hidden_msg = f'Token Marked As Invalid: {bearer_token}'
        )

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


# ---------- Scraping Data ----------
    # --- Fetch Method  ---
    async def check_token(self,
        bearer_token:str
    ) -> Tuple[bool, str]:
        
        fly_from = random_routes()
        return await self.get_flights_one_way(
            FlightQueryParams(
                date = random_date(max_dias = 60),
                fly_from = fly_from,
                fly_to = random_routes(diferent_to = fly_from),
            ),
            bearer_token = bearer_token
        )



if __name__ == "__main__":
    async def main():
        tokens = UpdaterBearerTokens()
        await tokens.init_monitor()

    asyncio.run(main())
