from datetime import datetime
from datetime import datetime, timezone
from typing import Dict, Tuple
from discord_webhook import AsyncDiscordWebhook, DiscordEmbed

from utils.tools import SingletonClass


class NotifyDiscord(metaclass = SingletonClass):
    def __init__(self) -> None:
        self.basic_data = {
            "imagen": "https://i.imgur.com/Gbke7s1.png",
            "footer": " | By Cronos Monitors",
            "color": "EC0E5F"
        }


# ---------- Admin Notifys ----------
    # --- Message Error ---
    async def error_admin(self, 
        URL_Webhook:str, 
        store_data:Dict[str, str], 
        problem_logs:str
    ) -> Tuple[bool, str]:
        
        webhook = AsyncDiscordWebhook(
            url = URL_Webhook,
            avatar_url = self.basic_data["imagen"],
            username = store_data['name'].capitalize(),
            rate_limit_retry = True
        )
        embed = DiscordEmbed(
            title = f'Error Occurred in: {store_data["name"]}', 
            color = self.basic_data["color"]
        )
        embed.set_author(
            name = store_data["name"],
            icon_url = self.basic_data["imagen"],
        )
        embed.set_footer(
            text = store_data['name'].capitalize() + self.basic_data["footer"],
            icon_url = self.basic_data["imagen"]
        )
        embed.set_timestamp()

        embed.set_thumbnail(
            url = self.basic_data["imagen"]
        )
        embed.add_embed_field(
            name = ':book: Information Log:', 
            value = f'```{problem_logs}```',
            inline = False
        )
        try:
            webhook.add_embed(embed)
            await webhook.execute()
            return True, f'Webhook Error Was Reported Correctly | Content: {problem_logs}'
        
        except Exception as err:
            return False, f'Webhook Error Reporting Failures, Could Not Delivered | Content: {problem_logs} | Type: {type(err).__name__} | Message: {str(err)}'


# ---------- Public Notifys ----------
    # --- Simple Webhook Deals ---
    async def flights_deals(self,
        URL_Webhook:str, 
        flight_data:str
    ):
        webhook = AsyncDiscordWebhook(
            url = URL_Webhook,
            avatar_url = self.basic_data["imagen"],
            username = "Flights Deals Finder",
            rate_limit_retry = True
        )
        embed = DiscordEmbed(
            title = f':airplane: Nueva Oferta Encontrada {flight_data["title"]}',
            description = flight_data["description"],
            url = flight_data["url_deal"],
            color = self.basic_data["color"]
        )
        embed.set_author(
            name = "Flights Deals Finder",
            icon_url = self.basic_data["imagen"],
        )
        embed.set_footer(
            text =  f'Deals Finder {self.basic_data["footer"]}',
            icon_url = self.basic_data["imagen"]
        )
        embed.set_timestamp()

        embed.set_thumbnail(
            url = self.basic_data["imagen"]
        )
        embed.add_embed_field(
            name = f'', 
            value = f':bangbang: [***Comprar Pasaje***]({flight_data["url_deal"]}) :bangbang:',
            inline = False
        )
        embed.add_embed_field(
            name = ':money_with_wings: Precio:', 
            value = flight_data["price"]
        )
        if flight_data.get("duration", False):
            embed.add_embed_field(
                name = ':hourglass_flowing_sand: Duracion Total:', 
                value = flight_data["duration"]
            )
        embed.add_embed_field(
            name = ':question: Escala:', 
            value = flight_data["scale"]
        )
        embed.add_embed_field(
            name = ':airplane: Aerolinea:', 
            value = f'`{flight_data["airline"]}`'
        )
        embed.add_embed_field(
            name = ':books: Proveedor:', 
            value = f'`{flight_data["provider"]}`'
        )
        if flight_data.get("seat_available", False):
            embed.add_embed_field(
                name = ':seat: Lugares disponibles:', 
                value = flight_data["seat_available"]
            )
        embed.add_embed_field(
            name = ':airplane_departure: Origen:', 
            value = flight_data["origin"]
        )
        embed.add_embed_field(
            name = ':airplane_arriving: Destino:', 
            value = flight_data["destination"]
        )
        embed.add_embed_field(
            name = 'Escalas: ', 
            value = flight_data["sections"],
            inline = False
        )
        try:
            webhook.add_embed(embed)
            await webhook.execute()
            return True, f'Webhook Delivered Successfully.'
        
        except Exception as err:
            return False, f'Error Delivering Deals Webhook | Type: {type(err).__name__} | Message: {str(err)}'



# ---------- Tools ----------
    @staticmethod
    def create_timestamp_discord(
        time:datetime
    ):
        if isinstance(time, str):
            dt = datetime.fromisoformat(time)
        elif isinstance(time, datetime):
            dt = time
        else:
            raise TypeError("Error Time Parameter Must Be str Or datetime")

        dt = dt.astimezone(timezone.utc)
        return f'<t:{int(dt.timestamp())}>'