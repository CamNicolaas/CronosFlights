from typing import Union, Any, Dict

from utils.DB import AsyncFlightDBManager
from utils.exceptions import DealsAnalyzerManager
from utils.configs.manage_configs import AsyncConfigManager
from utils import (
    CheckerDealsExtremeError,
    CheckerDealsLowestMonthError
)


class FlightDealAnalyzer:
    def __init__(self,
        configs:Dict[str, Dict[str, Any]],
        db_flights:AsyncFlightDBManager
    ):
        self.configs = configs
        self.db_flights = db_flights
        self._message:str = '[Flights Deals Analyzer]'


# ---------- Main Method ----------
    async def analyze_deals(self, 
        flight_data:Dict
    ) -> Dict[str, Dict]:
        
        try:
            offers = flight_data.get("offers")
            if not offers:
                raise DealsAnalyzerManager(
                    f'{self._message} Error, The Flight Cannot Have No Offers',
                    context = {
                        "error_type": type(err).__name__,
                        "error_msg": str(err)
                    }
                )
            result = {
                "extreme_deal": {"active": False},
                "lowest_price_month": {"active": False}
            }
            iata_origin = flight_data["iata_origin"]
            iata_destination = flight_data["iata_destination"]
            offer = offers[0]
            seat_class = offer["class"]
            price = offer["price"]
            
            result.update(
                {
                    "extreme_deal": await self._check_extreme_deal(
                        iata_origin, 
                        iata_destination,
                        flight_data["time_departure"],
                        seat_class, 
                        price
                    ),
                    "lowest_price_month": await self._check_lowest_month(
                        iata_origin, 
                        iata_destination,
                        price, 
                        flight_data["time_departure"]
                    )
                }
            )
            return result

        except Exception as err:
            raise DealsAnalyzerManager(
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )


# ---------- Checker Methods ----------
    async def _check_extreme_deal(self,
        iata_origin:str, 
        iata_destination:str,
        time_departure:str, #'departure': '2025-07-22T21:45:00'
        seat_class:str, 
        price:int
    ) -> Dict[str, Union[bool, Any]]:
        
        try:
            stats = await self.db_flights.get_stats_for_route(
                iata_origin,
                iata_destination,
                time_departure,
                seat_class
            )
            if not stats:
                return {"active": False}

            q1 = stats["q1"]
            q3 = stats["q3"]
            iqr = stats["iqr"]
            
            if not all(isinstance(v, (int, float)) for v in [q1, q3, iqr]):
                return {"active": False}

            min_price_real = await self.db_flights.get_min_price(
                iata_origin = iata_origin,
                iata_destination = iata_destination,
                departure_date = time_departure,
                type_date = "month",
                return_full = False
            )
            if min_price_real is not None and price > min_price_real * self.configs["min_real_price"]:
                return {"active": False}

            percentage_off = round((q1 - price) / q1 * 100, 2)
            threshold = q1 - self.configs["extreme_threshold"] * iqr

            if (
                iqr > 0 
                and price < threshold 
                and percentage_off >= self.configs["extreme_min_percentage_off"]
            ) or percentage_off >= self.configs["extreme_percentage_off"]:

                return {
                    "active": True,
                    "level": "extreme",
                    "price": price,
                    "q1": q1,
                    "q3": q3,
                    "iqr": iqr,
                    "threshold": int(threshold),
                    "percentage_off": percentage_off,
                    "description": (
                        f"🔥 *Nuevo Precio Extremadamente Bajo: ${price}.*\n"
                        f"Normalmente Se Encuentra Por Arriba De: ${q1}.\n"
                        f"🧠 Ahorro Aproximado: {percentage_off}%."
                    )
                }
            if (
                iqr > 0 and
                price < q1 * self.configs["deal_q1_discount"] and
                percentage_off >= self.configs["deal_min_percentage_off"]
            ):    
                return {
                    "active": True,
                    "level": "deal",
                    "price": price,
                    "q1": q1,
                    "q3": q3,
                    "iqr": iqr,
                    "threshold": int(threshold),
                    "percentage_off": percentage_off,
                    "description": (
                        f"💸 *Nuevo Precio Bajo: ${price}.*\n"
                        f"Valor de Referencia Promedio: ${q1}.\n"
                        f"💰 Ahorro Estimado: {percentage_off}%."
                    )
                }
            if iqr == 0 and (
                price < q1 * self.configs["no_iqr_discount"] and 
                percentage_off >= self.configs["deal_min_percentage_off"]
            ):
                return {
                    "active": True,
                    "level": "deal",
                    "price": price,
                    "q1": q1,
                    "q3": q3,
                    "iqr": iqr,
                    "threshold": int(threshold),
                    "percentage_off": percentage_off,
                    "description": (
                        f"💸 *Nuevo Precio Destacado: ${price}.*\n"
                        f"Valor Habitual Estimado: ${q1}.\n"
                        f"🔻 Ahorro Aproximado: {percentage_off}%."
                    )
                }
            return {"active": False}
        
        except Exception as err:
            raise CheckerDealsExtremeError(
                f'{self._message} Error Could Not Analyze Extreme Deal...',
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )


    async def _check_lowest_month(self,
        iata_origin:str, 
        iata_destination:str, 
        price:int, 
        departure_date:str #'departure': '2025-07-22T21:45:00'
    ) -> Dict[str, Union[bool, Any]]:

        try:
            lowest_price = await self.db_flights.get_min_price(
                iata_origin = iata_origin,
                iata_destination = iata_destination,
                departure_date = departure_date,
                type_date = "month",
                return_full = False
            )
            if lowest_price is not None and price < lowest_price:
                return {
                    "active": True,
                    "price": price,
                    "monthly_min_price": lowest_price,
                    "description": (
                        f'*Nuevo Precio Minimo Mensual: ${price}. '
                        f'El Precio Anterior: ${lowest_price}*'
                    )
                }
            return {"active": False}

        except Exception as err:
            raise CheckerDealsLowestMonthError(
                f'{self._message} Error Could Not Analyze Lowest Month...',
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )

