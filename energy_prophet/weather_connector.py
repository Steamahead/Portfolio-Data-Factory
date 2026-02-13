import requests
import pandas as pd
import pyodbc
import logging
import time
import datetime


class WeatherConnector:
    def __init__(self, sql_conn_str):
        self.sql_conn_str = sql_conn_str

        # SIATKA 'POWER CLUSTERS' (URE 09.2025)
        self.locations = {
            # --- TIER 1: GIGANCI OZE ---
            'Darlowo': {'lat': 54.42, 'lon': 16.41},
            'Konin': {'lat': 52.22, 'lon': 18.25},
            'Slupsk': {'lat': 54.46, 'lon': 17.02},
            'Bialogard': {'lat': 54.00, 'lon': 15.98},
            'Sztum': {'lat': 53.92, 'lon': 19.03},
            # --- TIER 2: KLUCZOWE REGIONY ---
            'Zuromin': {'lat': 53.06, 'lon': 19.91},
            'Stargard': {'lat': 53.33, 'lon': 15.03},
            'Wejherowo': {'lat': 54.60, 'lon': 18.24},
            'Zagan': {'lat': 51.61, 'lon': 15.31},
            'Legnica': {'lat': 51.20, 'lon': 16.16},
            'Zamosc': {'lat': 50.72, 'lon': 23.25},
            # --- TIER 3: POPYT ---
            'Warszawa': {'lat': 52.22, 'lon': 21.01},
            'Katowice': {'lat': 50.26, 'lon': 19.02},
            'Lodz': {'lat': 51.75, 'lon': 19.45},
            'Krakow': {'lat': 50.06, 'lon': 19.94},
            'Suwalki': {'lat': 54.11, 'lon': 22.93},
        }

    def _get_location_type(self, city):
        """Categorize location by primary OZE type or demand center"""
        wind_locations = ['Darlowo', 'Slupsk', 'Bialogard', 'Sztum', 'Zuromin']
        solar_locations = ['Konin', 'Stargard', 'Wejherowo', 'Zagan', 'Legnica', 'Zamosc']
        demand_locations = ['Warszawa', 'Katowice', 'Lodz', 'Krakow', 'Suwalki']

        if city in wind_locations: return 'WIND'
        if city in solar_locations: return 'SOLAR'
        if city in demand_locations: return 'DEMAND'
        return 'MIXED'

    def run_etl(self, target_date):
        """Pobiera pogodę dla klastrów energetycznych"""
        try:
            logging.info(f"   Fetching weather data for {len(self.locations)} strategic clusters...")
            df = self._fetch_weather(target_date)
            if not df.empty:
                self._save_to_sql(df)
                logging.info(f"   Weather data saved: {len(df)} rows")
            else:
                logging.warning("   No weather data fetched.")
        except Exception as e:
            logging.error(f"   Weather ETL Failed: {e}")
            raise e

    def _fetch_weather(self, date):
        """Batch fetch - wszystkie 16 lokalizacji w JEDNYM request do Open-Meteo."""
        all_data = []
        date_str = date.strftime('%Y-%m-%d')

        # --- LOGIKA: API vs ARCHIVE ---
        days_diff = (datetime.date.today() - date).days
        if days_diff > 10:
            base_url = "https://archive-api.open-meteo.com/v1/archive"
        else:
            base_url = "https://api.open-meteo.com/v1/forecast"

        # Przygotuj listy współrzędnych (kolejność zachowana - dict jest ordered w Python 3.7+)
        cities = list(self.locations.keys())
        lats = [str(self.locations[c]['lat']) for c in cities]
        lons = [str(self.locations[c]['lon']) for c in cities]

        params = {
            "latitude": ",".join(lats),
            "longitude": ",".join(lons),
            "start_date": date_str,
            "end_date": date_str,
            "hourly": "temperature_2m,wind_speed_10m,wind_direction_10m,direct_radiation,cloud_cover",
            "timezone": "Europe/Warsaw"
        }

        # --- RETRY LOGIC (1 request zamiast 16) ---
        max_retries = 3
        for attempt in range(max_retries):
            try:
                r = requests.get(base_url, params=params, timeout=30)
                r.raise_for_status()
                results = r.json()

                # Odpowiedź to tablica obiektów (jeden per lokalizacja)
                if not isinstance(results, list):
                    results = [results]

                for idx, data in enumerate(results):
                    city = cities[idx]
                    coords = self.locations[city]
                    hourly = data.get('hourly', {})
                    times = hourly.get('time', [])
                    temps = hourly.get('temperature_2m', [])
                    winds = hourly.get('wind_speed_10m', [])
                    wind_dirs = hourly.get('wind_direction_10m', [])
                    solar = hourly.get('direct_radiation', [])
                    clouds = hourly.get('cloud_cover', [])

                    for i, t_str in enumerate(times):
                        dt = pd.to_datetime(t_str)
                        all_data.append({
                            'location': city,
                            'location_type': self._get_location_type(city),
                            'dtime': dt,
                            'business_date': dt.strftime('%Y-%m-%d'),
                            'hour': dt.hour,
                            'lat': coords['lat'],
                            'lon': coords['lon'],
                            'temp_c': temps[i] if i < len(temps) else None,
                            'wind_kph': winds[i] if i < len(winds) else None,
                            'wind_direction': wind_dirs[i] if i < len(wind_dirs) else None,
                            'solar_rad': solar[i] if i < len(solar) else None,
                            'cloud_cover': clouds[i] if i < len(clouds) else None
                        })

                logging.info(f"   ✓ Batch weather fetch OK: {len(cities)} cities, {len(all_data)} rows")
                break  # Sukces

            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5
                    logging.warning(
                        f"   ⚠️ Batch weather fetch failed. Retrying in {wait_time}s... (Attempt {attempt + 1}/{max_retries}): {e}")
                    time.sleep(wait_time)
                else:
                    logging.error(f"   ❌ Batch weather fetch failed after {max_retries} attempts: {e}")

        if not all_data:
            return pd.DataFrame()
        return pd.DataFrame(all_data)

    def _connect_with_retry(self, max_retries=3):
        """Połączenie SQL z retry logic."""
        for attempt in range(max_retries):
            try:
                conn_str = self.sql_conn_str
                if 'Connection Timeout' not in conn_str:
                    conn_str += ';Connection Timeout=30'
                return pyodbc.connect(conn_str)
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5
                    logging.warning(f"   ⚠️ SQL connection failed. Retrying in {wait_time}s... (Attempt {attempt + 1}/{max_retries}): {e}")
                    time.sleep(wait_time)
                else:
                    raise

    def _save_to_sql(self, df):
        if df.empty:
            return

        # DDL - Enhanced table for Power BI visualization
        create_table_sql = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'weather_data')
        CREATE TABLE weather_data (
            id              INT IDENTITY(1,1) PRIMARY KEY,
            location        VARCHAR(50) NOT NULL,
            location_type   VARCHAR(10) NOT NULL,
            dtime           DATETIME NOT NULL,
            business_date   DATE NOT NULL,
            hour            TINYINT NOT NULL,
            lat             DECIMAL(6,4),
            lon             DECIMAL(6,4),
            temp_c          DECIMAL(5,2),
            wind_kph        DECIMAL(5,2),
            wind_direction  SMALLINT,
            solar_rad       DECIMAL(8,2),
            cloud_cover     TINYINT,
            created_at      DATETIME DEFAULT GETDATE(),
            UNIQUE (location, dtime)
        );
        """

        # UPSERT with new columns
        merge_sql = """
        MERGE INTO weather_data AS T
        USING (SELECT ? as loc, ? as loc_type, ? as dt, ? as biz_date, ? as hr,
                      ? as lat, ? as lon, ? as tmp, ? as wnd, ? as wnd_dir, ? as sol, ? as cld) AS S
        ON T.location = S.loc AND T.dtime = S.dt
        WHEN MATCHED THEN UPDATE SET
            location_type = S.loc_type, business_date = S.biz_date, hour = S.hr,
            lat = S.lat, lon = S.lon, temp_c = S.tmp, wind_kph = S.wnd,
            wind_direction = S.wnd_dir, solar_rad = S.sol, cloud_cover = S.cld,
            created_at = GETDATE()
        WHEN NOT MATCHED THEN INSERT
            (location, location_type, dtime, business_date, hour, lat, lon,
             temp_c, wind_kph, wind_direction, solar_rad, cloud_cover)
            VALUES (S.loc, S.loc_type, S.dt, S.biz_date, S.hr, S.lat, S.lon,
                    S.tmp, S.wnd, S.wnd_dir, S.sol, S.cld);
        """

        with self._connect_with_retry() as conn:
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            conn.commit()

            for _, r in df.iterrows():
                dtime_val = r['dtime'].to_pydatetime()
                vals = [
                    r['location'],
                    r['location_type'],
                    dtime_val,
                    r['business_date'],
                    r['hour'],
                    r['lat'],
                    r['lon'],
                    r['temp_c'],
                    r['wind_kph'],
                    r['wind_direction'],
                    r['solar_rad'],
                    r['cloud_cover']
                ]
                vals = [None if pd.isna(v) else v for v in vals]

                cursor.execute(merge_sql, *vals)
            conn.commit()
