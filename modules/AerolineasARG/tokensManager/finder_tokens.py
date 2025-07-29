import asyncio
from pathlib import Path
from typing import Optional, Union, Any, Dict, Tuple

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
    RequestsBlocked,
    InvalidPastDate,
    GdsResponseError,
    SiteServiceDown,
    TokenAlreadyExists
)



class FinderBearerTokens(AerolineasScraper):
    def __init__(self,
        log_name:Optional[str] = "FinderTokens", 
        log_path:Optional[Union[str, Path]] = "./aerolineasARG/TokensManager"
    ):
        super().__init__()
        # --- Notify System ---
        self.notifyer = NotifyDiscord()
        # --- DB Tokens ---
        self.db_tokens = DBTokensManager()
        # --- Configs ---
        self.configs:Dict[str, Dict[str, Any]] = dict()
        # --- Logger ---
        self.logger = AsyncMessageHandler(
            log_filename = log_name, 
            logs_folder = log_path, 
            printer_msg = "[AerolineasArg][Finder Tokens] Status:"
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
            await self.logger.critical("Init Module Aerolineas Tokens Finder")

            await self.search_new_tokens()

        except Exception as err:
            message_error = f'Error Fatal, Kill Process | Type: {type(err).__name__} | Message: {str(err)}'
            # --- Send Status to Discord ---
            _, response_webhook = await self.notifyer.error_admin(
                URL_Webhook = self.configs["admin"]["status_monitors"],
                store_data = self.configs["general"],
                problem_logs = f'[AerolineasArg][Finder Tokens] Status: {message_error}'
            )
            # --- Save Log ---
            await self.logger.error(
                message = message_error,
                hidden_msg = response_webhook
            )
            raise
        
        finally:
            await self.db_tokens.disconnect_db()
            await self.logger.shutdown()


# ---------- Handler New Tokens ----------
    # --- Manager ---
    async def search_new_tokens(self) -> None:
        flag_message = True

        while True:
            try:
                await self._try_get_new_token()
                flag_message = True

            except TokenAlreadyExists:
                if flag_message:
                    flag_message = False
                    await self.logger.warning("Waiting For New Tokens...", to_file = False)
                await asyncio.sleep(10)

            except (RequestsBlocked, InvalidPastDate, GdsResponseError, SiteServiceDown) as err:
                await self.logger.error(f'Error Searching "Bearer Token" | Type: {type(err).__name__} | Message: {str(err)}')
                await asyncio.sleep(self.configs["general"]["delay_error"])

            except Exception as err:
                await self.logger.critical(
                    f'Unhandled Error While Searching Token | Type: {type(err).__name__} | Message: {str(err)}'
                )
                raise

    # --- Get New Token ---
    async def _try_get_new_token(self) -> None:
        status, response_token = await self.find_new_token()
        
        if not status:
            await self.logger.error(
                message = response_token,
                to_file = False
            )
            await asyncio.sleep(self.configs["general"]["delay_error"])
            return

        exists_token, _ = await self.db_tokens.check_token_exists(response_token)
        if exists_token:
            raise TokenAlreadyExists

        _, message_saved = await self.db_tokens.set_token(response_token)
        await self.logger.success(
            message = f'{message_saved}: ...{response_token[-25:]}',
            hidden_msg = f'Bearer Token: {response_token}'
        )


# ---------- Scraping Data ----------
    # --- Fetch Tokens  ---
    async def find_new_token(self) -> Tuple[bool, str]:
        fly_from = random_routes()

        return await self.get_new_bearer_token(
            FlightQueryParams(
                date = random_date(max_dias = 60),
                fly_from = fly_from,
                fly_to = random_routes(diferent_to = fly_from)
            )   
        )



if __name__ == "__main__":
    async def main():
        tokens = FinderBearerTokens()
        await tokens.init_monitor()

    asyncio.run(main())
