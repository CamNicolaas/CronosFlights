import dotenv, asyncpg, hashlib, os
from asyncpg import Record
from datetime import datetime, date
from typing import Optional, Union, Any, Dict, Tuple, Literal, List
import numpy as np
import pandas as pd

from utils.tools import SingletonClass
from utils.exceptions import (    
    DBFlightsError,
    DBFlightsConnectionError,
    DBFlightsAirportsError,
    DBFlightsFindNotifysError,
    DBFlightsMarkNotifysError,
    DBFlightCheckerError,

    GenerateIQRError,
    ReturnIQRError,
    ReturnMinMonthlyError,
    ReturnMinDailyError
)



class AsyncFlightDBManager(metaclass = SingletonClass):
    def __init__(self):
        self._configs = dotenv.dotenv_values()
        self.pool:Optional[asyncpg.Pool] = None
        self._message:str = f'[DB Flights Manager]'


# ---------- Manage DB Flights ----------
    async def connect_db(self):
        try:
            if not self.pool:
                # Maybe it be a good idea add the min and max ​​to settings or .env. idk
                self.pool = await asyncpg.create_pool(
                    dsn = self._configs["DB_POSTGRES"],
                    min_size = 1,
                    max_size = 10
                )

        except asyncpg.PostgresConnectionError as err:
            raise DBFlightsConnectionError(
                f'{self._message} Error During Connection...',
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )

    async def disconnect_db(self):
        try:
            if self.pool:
                await self.pool.close()
                self.pool = None

        except asyncpg.PostgresError as err:
            raise DBFlightsError(
                f'{self._message} Error During Disconnection...',
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )


# ---------- Fetch Data Flights ----------
    async def get_airport_info(self, 
        iata_code:str
    ) -> Optional[Dict]:

        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT * FROM airports
                    WHERE iata_code = $1
                """, iata_code.upper())
                return dict(row) if row else None
        
        except asyncpg.PostgresError as err:
            raise DBFlightsAirportsError(
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )


    async def was_notification_sent(self,
        flight_hash:str,
        price:int,
        notified_channel:Optional[str] = None
    ) -> bool:
        
        try:
            base_query = """
                SELECT 1 FROM notifications_sent
                WHERE flight_hash = $1 AND price = $2
            """
            args = [
                flight_hash, 
                price
            ]

            optional_filters = [
                ("notified_channel", notified_channel)
            ]
            for index, (column, value) in enumerate(optional_filters, start = 3):
                if value:
                    base_query += f" AND {column} = ${index}"
                    args.append(value)

            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(base_query, *args)
                return bool(row)

        except asyncpg.PostgresError as err:
            raise DBFlightsFindNotifysError(
                f'{self._message} Error Searching Flight By Hash And Price | Hash: {flight_hash} | Price: {price}',
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )

    async def mark_notification_sent(self,
        flight_hash:str,
        price:int,
        notified_channel:Optional[str] = ""
    ) -> None:
        
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO notifications_sent (flight_hash, price, notified_channel)
                    VALUES ($1, $2, $3)
                    ON CONFLICT DO NOTHING
                """, flight_hash, price, notified_channel)

        except asyncpg.PostgresError as err:
            raise DBFlightsMarkNotifysError(
                f'{self._message} Error Marking Flight| Hash: {flight_hash} | Price: {price}',
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )


    async def check_flight_exists(self,
        conn:asyncpg.Pool, 
        hash_id:str
    ) -> Optional[asyncpg.Record]:
        
        try:
            return await conn.fetchrow("""
                SELECT * FROM flights_calendar 
                WHERE hash_id = $1
            """, hash_id)
        
        except asyncpg.PostgresError as err:
            raise DBFlightCheckerError(
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )

    async def _insert_sections_flight(self,
        conn:asyncpg.Pool,
        hash_id:str,
        sections:List[Dict[str, Any]]
    ) -> None:

        for index, secction in enumerate(sections):
            time_departure = self.parse_departure_date(
                secction["departure"], 
                return_type = "datetime"
            )
            time_arrival = self.parse_departure_date(
                secction["arrival"], 
                return_type = "datetime"
            )

            await conn.execute("""
                INSERT INTO flight_sections (
                    flight_hash, section_index, flight_number, departure, arrival, origin, destination, equipment
                )
                VALUES($1, $2, $3, $4, $5, $6, $7, $8)
            """, hash_id, index, secction["flightNumber"], time_departure, time_arrival, secction["origin"], 
                secction["destination"], secction["equipment"]
            )

    async def _insert_price_history(self,
        conn: asyncpg.Pool,
        hash_id:str,
        iata_origin:str,
        iata_destination:str,
        time_departure:datetime,
        time_arrival:datetime,
        provider:str,
        airline:str,
        seat_class:str,
        scale:bool,
        total_duration:int,
        price:int
    ) -> str:

        return await conn.execute("""
            INSERT INTO price_history_calendar (
                flight_hash, iata_origin, iata_destination, time_departure, time_arrival,
                provider, airline, offer_class, scale, total_duration, price, recorded_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
            ON CONFLICT (flight_hash, offer_class, price) DO NOTHING
        """,
            hash_id, iata_origin, iata_destination, time_departure, time_arrival,
            provider, airline, seat_class, scale, total_duration, price
        )


# ---------- Main Querys Flights ----------
    async def insert_or_update_calendar_entry(self, 
        provider:str,
        flight_data:Dict[str, Any]
    ) -> Tuple[str, bool]:

        async with self.pool.acquire() as conn:
            try:
                async with conn.transaction():
                    iata_origin = flight_data["iata_origin"]
                    iata_destination = flight_data["iata_destination"]
                    time_departure = flight_data["time_departure"]

                    flight_signature, hash_id = self.generate_flight_hash(
                        iata_origin, 
                        iata_destination, 
                        time_departure, 
                        provider, 
                        flight_data["airline"]
                    )
                    existing = await self.check_flight_exists(conn, hash_id)
                    if not flight_data["active"]:
                        return await self._handle_inactive_flight(conn, hash_id, existing)

                    return await self._handle_active_flight(
                        conn, 
                        provider, 
                        flight_signature, 
                        hash_id, 
                        existing,
                        flight_data
                    )

            except Exception as err:
                raise DBFlightsError(
                    f'{self._message} Unknown Error Occurred When Trying Add Or Query For Existence Flight.. | Type: {type(err).__name__} | Msg: {str(err)} | Flight: {flight_data}',
                    context = {
                        "error_type": type(err).__name__,
                        "error_msg": str(err)
                    }
                )

    async def _handle_inactive_flight(self,
        conn:asyncpg.Pool,
        hash_id:str,
        existing:bool
    ) -> Tuple[str, bool]:
        
        if existing:
            await conn.execute("""
                UPDATE flights_calendar 
                    SET available = FALSE, 
                    last_updated = NOW()
                WHERE hash_id = $1
            """, hash_id
            )
        
        return hash_id, False

    async def _handle_active_flight(self,
        conn:asyncpg.Pool,
        provider:str,
        flight_signature:str,
        hash_id:str,
        existing:Optional[Dict[str, Any]],
        flight_data:Dict[str, Any]
    ) -> Tuple[str, bool]:
        
        scale = flight_data["scale"]
        total_duration = flight_data["total_duration"]
        offer = flight_data["offers"][0] if flight_data.get("offers") and len(flight_data["offers"]) > 0 else {}

        seat_class = offer.get("class")
        price = offer.get("price")

        time_departure = self.parse_departure_date(
            flight_data["time_departure"], 
            return_type = "datetime"
        )
        time_arrival = self.parse_departure_date(
            flight_data["time_arrival"], 
            return_type = "datetime"
        )

        if existing:
            if any([
                existing["price"] != price,
                existing["scale"] != scale,
                existing["class"] != seat_class,
                existing["time_departure"] != time_departure,
                existing["time_arrival"] != time_arrival
            ]):
                await conn.execute("""
                    UPDATE flights_calendar SET
                        available = TRUE,
                        time_departure = $1,         
                        time_arrival = $2,  
                        scale = $3,
                        price = $4,
                        total_duration = $5,        
                        class = $6,
                        last_updated = NOW()
                    WHERE hash_id = $7
                """, time_departure, time_arrival, scale, price, total_duration, seat_class , hash_id
                )
                # --- Del Old Sections --.
                await conn.execute("""
                    DELETE FROM flight_sections 
                    WHERE flight_hash = $1
                """, hash_id
                )
                # --- Load New Flight Secctions ---
                await self._insert_sections_flight(
                    conn = conn,
                    hash_id = hash_id,
                    sections = flight_data["sections"]
                )
                if existing["price"] != price:
                    history_result = await self._insert_price_history(
                        conn, hash_id, flight_data["iata_origin"], flight_data["iata_destination"], time_departure, time_arrival,
                        provider, flight_data["airline"], seat_class, scale, total_duration, price
                    )      
                    return hash_id, history_result != "INSERT 0 0"
            return hash_id, False

        
        result = await conn.execute("""
            INSERT INTO flights_calendar (
                available, hash_id, flight_signature, provider, airline, iata_origin, iata_destination,
                time_departure, time_arrival, scale, price, total_duration, class, last_updated
            )
            VALUES (TRUE, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
            ON CONFLICT (iata_origin, iata_destination, time_departure, class) DO NOTHING
        """, hash_id, flight_signature, provider, flight_data["airline"], flight_data["iata_origin"],
            flight_data["iata_destination"], time_departure, time_arrival, scale, price, total_duration, seat_class
        )
        if result == "INSERT 0 0":
            return hash_id, False

        # --- Add Flight Secctions ---
        await self._insert_sections_flight(
            conn = conn,
            hash_id = hash_id,
            sections = flight_data["sections"]
        )

        if price and seat_class:
            await self._insert_price_history(
                conn, hash_id, flight_data["iata_origin"], flight_data["iata_destination"], time_departure, time_arrival,
                provider, flight_data["airline"], seat_class, scale, total_duration, price
            )  
        return hash_id, True


# ---------- Deals Analize Methods ----------
    # --- IQR Updater ---
    async def update_calendar_stats(self) -> None:
        async with self.pool.acquire() as conn:
            try:
                async with conn.transaction():
                    await conn.execute("""
                        DELETE FROM price_stats
                        WHERE q1 = q3 OR iqr = 0
                    """)

                    rows = await conn.fetch("""
                        WITH stats AS (
                            SELECT
                                iata_origin,
                                iata_destination,
                                class AS offer_class,
                                TO_CHAR(time_departure, 'YYYY-MM') AS period,
                                COUNT(*) AS sample_size,
                                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY price) AS median_price,
                                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price) AS q1,
                                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price) AS q3,
                                AVG(price)::INT AS avg_price,
                                MIN(price) AS min_price,
                                MAX(price) AS max_price,
                                STDDEV(price) AS stddev_price
                            FROM flights_calendar
                            WHERE available = TRUE AND price IS NOT NULL
                            GROUP BY iata_origin, iata_destination, class, period
                            HAVING COUNT(*) > 3
                        ),
                        min_flights AS (
                            SELECT DISTINCT ON (
                                iata_origin, iata_destination, class, TO_CHAR(time_departure, 'YYYY-MM')
                            )
                                iata_origin,
                                iata_destination,
                                class AS offer_class,
                                TO_CHAR(time_departure, 'YYYY-MM') AS period,
                                hash_id AS min_price_flight_hash
                            FROM flights_calendar
                            WHERE available = TRUE AND price IS NOT NULL
                            ORDER BY iata_origin, iata_destination, class,
                                    TO_CHAR(time_departure, 'YYYY-MM'),
                                    price ASC
                        )
                        SELECT
                            s.iata_origin,
                            s.iata_destination,
                            s.offer_class,
                            s.period,
                            s.sample_size,
                            s.median_price,
                            s.q1,
                            s.q3,
                            s.avg_price,
                            s.min_price,
                            s.max_price,
                            s.stddev_price,
                            m.min_price_flight_hash
                        FROM stats s
                        LEFT JOIN min_flights m
                        ON s.iata_origin = m.iata_origin
                        AND s.iata_destination = m.iata_destination
                        AND s.offer_class = m.offer_class
                        AND s.period = m.period
                    """)

                    for row in rows:
                        q1 = int(row["q1"])
                        q3 = int(row["q3"])
                        iqr = q3 - q1

                        await conn.execute("""
                            INSERT INTO price_stats (
                                iata_origin, iata_destination, period, offer_class,
                                sample_size, median_price,
                                avg_price, min_price, max_price,
                                stddev_price, q1, q3, iqr,
                                min_price_flight_hash, last_updated
                            ) VALUES (
                                $1, $2, $3, $4,
                                $5, $6,
                                $7, $8, $9,
                                $10, $11, $12, $13,
                                $14, NOW()
                            )
                            ON CONFLICT (iata_origin, iata_destination, period, offer_class)
                            DO UPDATE SET
                                sample_size = EXCLUDED.sample_size,
                                median_price = EXCLUDED.median_price,
                                avg_price = EXCLUDED.avg_price,
                                min_price = EXCLUDED.min_price,
                                max_price = EXCLUDED.max_price,
                                stddev_price = EXCLUDED.stddev_price,
                                q1 = EXCLUDED.q1,
                                q3 = EXCLUDED.q3,
                                iqr = EXCLUDED.iqr,
                                min_price_flight_hash = EXCLUDED.min_price_flight_hash,
                                last_updated = NOW()
                        """, row["iata_origin"], row["iata_destination"], row["period"],
                            row["offer_class"], row["sample_size"], int(row["median_price"]),
                            row["avg_price"], row["min_price"], row["max_price"],
                            float(row["stddev_price"]) if row["stddev_price"] is not None else 0.0,
                            q1, q3, iqr, row["min_price_flight_hash"])

            except Exception as err:
                raise GenerateIQRError(
                    f'{self._message} Unknown Error Occurred While Trying To Generate New IQR Statistics...',
                    context = {
                        "error_type": type(err).__name__,
                        "error_msg": str(err)
                    }
                )


    # --- Get Stats ---
    async def get_stats_for_route(self,
        iata_origin:str,
        iata_destination:str,
        departure_date:Union[str, date, datetime],
        seat_class:str = "Economy"
    ) -> Optional[Tuple[int, int, int]]:

        try:
            departure_date = self.parse_departure_date(
                departure_date, 
                return_type = "date"
            )
            async with self.pool.acquire() as conn:
                query = """
                    SELECT * FROM price_stats
                    WHERE iata_origin = $1
                    AND iata_destination = $2
                    AND offer_class = $3
                    AND period = $4
                """
                values = [
                    iata_origin,
                    iata_destination,
                    seat_class,
                    departure_date.strftime("%Y-%m")
                ]
                row = await conn.fetchrow(query, *values)
                return dict(row) if row else None

        except asyncpg.PostgresError as err:
            raise ReturnIQRError(
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )


    # --- Min Price Monthly ---
    async def get_min_price_monthly(self,
        iata_origin:str,
        iata_destination:str,
        departure_date:Union[str, date, datetime],
        seat_class:Optional[str] = None,
        provider:Optional[str] = None,
        airline:Optional[str] = None,
        return_full:bool = False
    ) -> Union[int, Dict, None]:

        try:
            departure_date = type(self).parse_departure_date(
                departure_date, 
                return_type = "date"
            )
            month_start = departure_date.replace(day = 1)
            month_end = (
                month_start.replace(month = 1, year = month_start.year + 1)
                if month_start.month == 12
                else month_start.replace(month = month_start.month + 1)
            )
            conditions = [
                "iata_origin = $1",
                "iata_destination = $2",
                "available = TRUE",
                "time_departure >= $3",
                "time_departure < $4"
            ]
            values = [
                iata_origin.upper(),
                iata_destination.upper(),
                month_start,
                month_end
            ]

            for optional_field, column in [
                (seat_class, "class"),
                (provider, "provider"),
                (airline, "airline")
            ]:
                if optional_field:
                    values.append(optional_field)
                    conditions.append(f"{column} = ${len(values)}")

            query = f"""
                SELECT {'*' if return_full else 'MIN(price) AS min_price'}
                FROM flights_calendar
                WHERE {' AND '.join(conditions)}
            """
            if return_full:
                query += " ORDER BY price ASC LIMIT 1"

            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(query, *values)
                if not row:
                    return None
                return dict(row) if return_full else int(row['min_price']) if row['min_price'] is not None else None

        except asyncpg.PostgresError as err:
            raise ReturnMinMonthlyError(
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )
        except Exception as err:
            raise ReturnMinMonthlyError(
                f'{self._message} Unknown Error Occurred While Trying to Get Monthly Min Price...',
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )

    # --- Min Price Daily ---
    async def get_min_price_date(self,
        iata_origin:str,
        iata_destination:str,
        departure_date:Union[str, date, datetime],
        seat_class:Optional[str] = None,
        provider:Optional[str] = None,
        airline:Optional[str] = None,
        exclude_airlines:Optional[List[str]] = None,
        return_full:bool = False
    ) -> Union[int, Dict, None]:
        
        try:
            departure_day = self.parse_departure_date(
                departure_date, 
                return_type = "date"
            )
            conditions = [
                "iata_origin = $1",
                "iata_destination = $2",
                "time_departure >= $3",
                "time_departure <= $4",
                "available = TRUE",
                "price IS NOT NULL"
            ]
            values = [
                iata_origin.upper(),
                iata_destination.upper(),
                datetime.combine(departure_day, datetime.min.time()),
                datetime.combine(departure_day, datetime.max.time())
            ]

            for optional_field, column_name in [
                (seat_class, "class"),
                (provider, "provider"),
                (airline, "airline")
            ]:
                if optional_field:
                    values.append(optional_field)
                    conditions.append(f"{column_name} = ${len(values)}")

            if exclude_airlines:
                placeholders = list()
                for airline_name in exclude_airlines:
                    values.append(airline_name)
                    placeholders.append(f"${len(values)}")
                conditions.append(f"airline NOT IN ({', '.join(placeholders)})")

            query = f"""
                SELECT {'*' if return_full else 'MIN(price) AS min_price'}
                FROM flights_calendar
                WHERE {' AND '.join(conditions)}
            """
            if return_full:
                query += " ORDER BY price ASC LIMIT 1"

            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(query, *values)
                if not row:
                    return None
                return dict(row) if return_full else int(row["min_price"]) if row["min_price"] is not None else None

        except Exception as err:
            raise ReturnMinDailyError(
                f'{self._message} Error getting min price for date...',
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )




    async def get_min_price(self,
        iata_origin:str,
        iata_destination:str,
        departure_date:Union[str, date, datetime],
        type_date:Literal["day", "month"],
        seat_class:Optional[str] = "Economy",
        provider:Optional[str] = None,
        airline:Optional[str] = None,
        exclude_airlines:Optional[List[str]] = None,
        return_full:bool = False
    ) -> Union[int, Dict, None]:

        try:
            dep_date = self.parse_departure_date(
                departure_date, 
                return_type = "date"
            )
            if type_date == "month":
                date_start = dep_date.replace(day = 1)
                if date_start.month == 12:
                    date_end = date_start.replace(year = date_start.year + 1, month = 1)
                else:
                    date_end = date_start.replace(month = date_start.month + 1)
            elif type_date == "day":
                date_start = datetime.combine(dep_date, datetime.min.time())
                date_end = datetime.combine(dep_date, datetime.max.time())
            else:
                raise ValueError(f'Invalid Format Day, use: "day" or "month".')
            
            conditions = [
                "iata_origin = $1",
                "iata_destination = $2",
                "available = TRUE",
                "time_departure >= $3",
                "time_departure < $4"
            ]
            values = [
                iata_origin.upper(),
                iata_destination.upper(),
                date_start,
                date_end
            ]

            for optional_value, column in [
                (seat_class, "class"),
                (provider, "provider"),
                (airline, "airline")
            ]:
                if optional_value:
                    values.append(optional_value)
                    conditions.append(f"{column} = ${len(values)}")

            if exclude_airlines:
                placeholders = []
                for airline_name in exclude_airlines:
                    values.append(airline_name.upper())
                    placeholders.append(f"${len(values)}")
                conditions.append(f"airline NOT IN ({', '.join(placeholders)})")

            query = f"""
                SELECT {'*' if return_full else 'MIN(price) AS min_price'}
                FROM flights_calendar
                WHERE {' AND '.join(conditions)}
            """
            if return_full:
                query += " ORDER BY price ASC LIMIT 1"

            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(query, *values)

                if not row:
                    return None

                return dict(row) if return_full else int(row["min_price"]) if row["min_price"] is not None else None

        except Exception as err:
            raise ReturnMinDailyError(
                f"{self._message} Error getting min price...",
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )


# ---------- Tools Methods ----------
    @staticmethod
    def generate_flight_hash(
        iata_origin:str,
        iata_destination:str,
        time_departure:Union[str, date, datetime],
        provider:str,
        airline:str
    ) -> Tuple[str, str]:
        
        date_only = AsyncFlightDBManager.parse_departure_date(
            time_departure, 
            return_type = "date"
        )
        raw = f'{provider}_{airline}_{iata_origin.upper()}_{iata_destination.upper()}_{date_only}'
        return raw, hashlib.sha256(raw.encode()).hexdigest()

    @staticmethod
    def parse_departure_date(
        value:Union[str, date, datetime],
        *,
        return_type:Literal["date", "datetime"] = "datetime"
    ) -> Union[date, datetime]:
        
        if isinstance(value, datetime):
            return value if return_type == "datetime" else value.date()

        if isinstance(value, date):
            return (
                datetime.combine(value, datetime.min.time())
                if return_type == "datetime"
                else value
            )

        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value)
                return dt if return_type == "datetime" else dt.date()
            except ValueError:
                raise ValueError(f'Invalid format for date/datetime string: {value}')

        raise TypeError(f'Unsupported type for datetime parsing: {type(value).__name__}')

