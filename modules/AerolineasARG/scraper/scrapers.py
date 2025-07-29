import asyncio, re
from dataclasses import dataclass
from typing import Dict, Any, Union, Tuple

from utils.fetchsmethods import FetchsMethodsClass
from utils.exceptions import (
    ScrapersError,
    UnauthorizedTokenAPI,
    ExpiredTokenAPI,
    InvalidPastDate,
    InvalidFormatDate,
    InvalidRequests,
    CannotGetBearerTokenError,
    UnknownError,
    RequestsBlocked,
    GdsResponseError,
    SiteServiceDown
)



@dataclass(slots = True)
class FlightQueryParams:
    date:str
    fly_from:str
    fly_to:str
    adults:int = 1
    infants:int = 0
    children:int = 0
    cabin_class:str = "Economy"

    def to_dict(self) -> dict:
        return {
            "date": self.date,
            "fly_from": self.fly_from,
            "fly_to": self.fly_to,
            "adults": self.adults,
            "infants": self.infants,
            "children": self.children,
            "cabin_class": self.cabin_class
        }


class AerolineasScraper(FetchsMethodsClass):
    def __init__(self):
        super().__init__()


# ---------- Scraping Data ----------
    # --- Get BearerTokens ---
    async def get_new_bearer_token(self,
        params:FlightQueryParams
    ) -> Tuple[bool, str]:

        status_code, response = await self.fetch_GET(
            url = self.gen_url_flyghts(
                params = params.to_dict(),
                use_api = False
            ),
            #proxy = "http://xxx, Use Proxy
            timeout_seconds = 10,
            return_json = False,
            headers = self.gen_new_headers()
        )
        if not status_code:
            return False, f'Requests Error, "Bearer Token" Could Not Be Retrieved... | {response}'
        
        type(self).check_status_code(
            status_code, 
            response, 
            params = params.to_dict()
        )
        return type(self).parsing_token_bearer(response)

    # --- Get Flight Details ---
    async def get_flights_one_way(self,
        params:FlightQueryParams,
        bearer_token:str
    ) -> Tuple[bool, Union[Dict, str]]:

        status_code, response = await self.fetch_GET(
            url = self.gen_url_flyghts(
                params.to_dict()
            ),
            #proxy = "http://xxx, Use Proxy
            timeout_seconds = 10,
            return_json = True,
            headers = self.gen_new_headers(
                {
                    "authorization": f'Bearer {bearer_token}'
                }
            ),
            header_order = [
                "accept",
                "accept-encoding",
                "accept-language",
                "authorization",
                "origin",
                "priority",
                "referer",
                "sec-ch-ua",
                "sec-ch-ua-mobile",
                "sec-ch-ua-platform",
                "sec-fetch-dest",
                "sec-fetch-mode",
                "sec-fetch-site",
                "traceparent",
                "tracestate",
                "user-agent",
                "x-channel-id"
            ]
        )
        if not status_code:
            return False, f'Requests Error, Retrieving Flight Details... | {response}'
        
        type(self).check_status_code(status_code, response, params.to_dict())
        return type(self).parsing_flyghts(response, params.to_dict())

    # --- Get Monthly Details ---
    async def get_flights_month_calendar(self,
        params:FlightQueryParams,
        bearer_token:str
    ) -> Tuple[bool, Union[Dict, str]]:
        
        status_code, response = await self.fetch_GET(
            url = self.gen_url_flyghts(
                params.to_dict(),
                flex_dates = "true"
            ),
            #proxy = "http://xxx, Use Proxy
            timeout_seconds = 10,
            return_json = True,
            headers = self.gen_new_headers(
                {
                    "authorization": f'Bearer {bearer_token}'
                }
            ),
            header_order = [
                "accept",
                "accept-encoding",
                "accept-language",
                "authorization",
                "origin",
                "priority",
                "referer",
                "sec-ch-ua",
                "sec-ch-ua-mobile",
                "sec-ch-ua-platform",
                "sec-fetch-dest",
                "sec-fetch-mode",
                "sec-fetch-site",
                "traceparent",
                "tracestate",
                "user-agent",
                "x-channel-id"
            ]
        )
        if not status_code:
            return False, f'Request Error, Retrieving Calendar Details... | {response}'

        type(self).check_status_code(status_code, response, params.to_dict())
        return type(self).parsing_flyghts_calendar(response, params.to_dict())


# ---------- Parsing Data ----------
    # --- Flights One Way ---
    @staticmethod
    def parsing_flyghts(
        response:Dict[str, Any],
        params:Dict[str, Any]
    ) -> Tuple[bool, Dict]:
        
        try:
            all_flights = list()
            for flight in response["brandedOffers"].get("0", list()):
                flight_info = flight["legs"][0]
                flight_offers = flight["offers"]
                flight_segments = flight_info["segments"]

                all_flights.append(
                    {
                        "airline": "AerolineasARG",
                        "iata_origin": flight_segments[0]["origin"],
                        "iata_destination": flight_segments[-1]["destination"],
                        "time_departure": flight_segments[0]["departure"],
                        "time_arrival": flight_segments[-1]["arrival"],
                        "scale": len(flight_info["segments"]) > 1,
                        "total_duration": flight_info["totalDuration"],
                        "sections": [
                            {
                                "flightNumber": section["airline"] + str(section["flightNumber"]),
                                "departure": section["departure"],
                                "arrival": section["arrival"],
                                "origin": section["origin"],
                                "destination": section["destination"],
                                "equipment": section["equipment"]
                            }
                            for section in flight_segments
                        ],
                        "offers": [
                            {
                                "offerId": offer["offerId"],
                                "class": offer["brand"].get("name", "Class Not Found"),
                                "seatAvailability": offer["seatAvailability"]["seats"],
                                "price": int(offer["fare"]["total"])
                            }
                            for offer in flight_offers
                        ]
                    }
                )
            return True, {
                "success": bool(all_flights),
                "provider": "AerolineasARG",
                #"timestamp": datetime.now(),
                "params": params,

                "shopping_id": response.get("searchMetadata").get("shoppingId"),
                "flights": all_flights
            }

        except (KeyError, IndexError, TypeError) as err:
            raise ScrapersError(
                f'Error Parsing Detailed Flight Info. Data Structure Might Changed... | Response: {response}',
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            ) from err

    # --- Calendar ---
    @staticmethod
    def parsing_flyghts_calendar(
        response:Dict[str, Any],
        params:Dict[str, Any]
    ) -> Tuple[bool, Dict]:
        
        try:
            all_flights = list()
            for flight in response["calendarOffers"].get("0", list()):
                if not flight.get("leg"):
                    all_flights.append(
                        {
                            "active": False,
                            "airline": "AerolineasARG",
                            "iata_origin": params["fly_from"],
                            "iata_destination": params["fly_to"],
                            "time_departure": flight["departure"],
                        }
                    )
                    continue 

                flight_info = flight["leg"]
                flight_offers = flight["offerDetails"]
                flight_segments = flight_info["segments"]

                all_flights.append(
                    {
                        "active": True,
                        "airline": "AerolineasARG",
                        "iata_origin": flight_segments[0]["origin"],
                        "iata_destination": flight_segments[-1]["destination"],
                        "time_departure": flight_segments[0]["departure"],
                        "time_arrival": flight_segments[-1]["arrival"],
                        "scale": flight_info["stops"] >= 1,
                        "total_duration": flight_info["totalDuration"],
                        "sections": [
                            {
                                "flightNumber": section["airline"] + str(section["flightNumber"]),
                                "departure": section["departure"],
                                "arrival": section["arrival"],
                                "origin": section["origin"],
                                "destination": section["destination"],
                                "equipment": section["equipment"]
                            }
                            for section in flight_segments
                        ],
                        "offers": [
                            {
                                "class": flight_offers["cabinClass"],
                                "seatAvailability": flight_offers["seatAvailability"]["seats"],
                                "price": int(flight_offers["fare"]["total"])
                            }
                        ]
                    }
                )
            return True, {
                "success": bool(all_flights),
                "provider": "AerolineasARG",
                "params": params,

                "shopping_id": response.get("searchMetadata").get("shoppingId"),
                "flights": all_flights
            }

        except (KeyError, IndexError, TypeError) as err:
            raise ScrapersError(
                f'Error Parsing Flight Schedule. Data Structure May Have Changed... | Response: {response}',
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            ) from err

    # --- Bearer Tokens ---
    @staticmethod
    def parsing_token_bearer( 
        response:str
    ) -> str:
        match = re.search(r'window\.__ACCESS_TOKEN__\s*=\s*"([^"]+)"', response)
        if not match:
            raise CannotGetBearerTokenError(
                f'Error, Couldn´t Get "Bearer Token" From Resp., Structure May Have Changed...',
                context = {
                    "error_type": f'The HTML Has Changed And Is Not Compatible With Current Search Format For "Bearer Token".',
                    "error_msg": f''
                }
            )
        return True, match.group(1)


# ---------- Tools ----------
    # --- Gen. Query URL ---
    @staticmethod
    def gen_url_flyghts( 
        params:Dict[str, Any],
        use_api:bool = True,
        flex_dates:str = "false"
    ) -> str:

        api_url = "https://api.aerolineas.com.ar/v1/flights/"
        site_url = "https://www.aerolineas.com.ar/flights-"

        missing_params = [param for param in ['date', 'fly_from', 'fly_to'] if not params.get(param, None)]
        if missing_params:
            raise ScrapersError(
                f'Missing required parameters to create Query Valid... "{", ".join(missing_params)}"'
            )
        return f'{api_url if use_api else site_url}offers?adt={params["adults"]}&inf={params["infants"]}&chd={params["children"]}&flexDates={flex_dates}&cabinClass={params["cabin_class"]}&flightType=ONE_WAY&leg={params["fly_from"]}-{params["fly_to"]}-{params["date"]}'

    # --- Checker Status Code ---
    @staticmethod
    def check_status_code(
        status_code:int, 
        response:Dict[str, Any],
        params:Dict[str, Any]
    ) -> bool:

        def raise_unknown_error():
            raise UnknownError(
                f'Unknown Fatal Error, API Returns Unknown Response Code...  | Code: {status_code} | Content: {response}',
                context = params
            )

        if status_code == 200:
            return True

        if isinstance(response, dict):
            error_desc = response.get("description", "").lower()
            error_msg = response.get("errorMessage", "").lower()
        else:
            error_desc = f'Error Description Not Found, Response Invalid | Status Code: {status_code}'
            error_msg = f'Error Msg Not Found, Response Invalid | Status Code: {status_code}'

        match status_code:
            case 400:
                if not error_desc and 'gds.flight.error.invalidrequest' in error_msg:
                    raise InvalidRequests(
                        f'Error Invalid Requests, Service May Not Be Available For This Search... | Code: {status_code}. | Content: {response}',
                        context = params
                    )

                match error_desc:
                    case "shopping.search.legs.past-date":
                        raise InvalidPastDate(context = params)
                    case "shopping.search.legs.format-date" | "shopping.search.legs.pattern":
                        raise InvalidFormatDate(context = params)
                raise_unknown_error()

            case 401:
                if error_msg == "unauthorized":
                    raise UnauthorizedTokenAPI()
                elif error_msg == "core.gateway.access-denied":
                    raise ExpiredTokenAPI(
                        context = {
                            "status_code": status_code, 
                            "response": response
                        }
                    )
                raise_unknown_error()

            case 403 | 429:
                raise RequestsBlocked(context = params)

            case 500:
                raise GdsResponseError(
                    f'Error GDS Returned Server Error... | Code: {status_code} | Content: {response}',
                    context = params
                )

            case 502:
                raise SiteServiceDown(
                    f'Error, Site Appears be Actually Down...... | Code: {status_code} | Content: {response}',
                    context = params
                )

            case _:
                raise_unknown_error()
