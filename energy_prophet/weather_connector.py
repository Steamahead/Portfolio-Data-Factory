import requests
import pandas as pd
import pyodbc
import logging
import time


class WeatherConnector:
    def __init__(self, sql_conn_str):
        self.sql_conn_str = sql_conn_str

        # SIATKA 'POWER CLUSTERS' (URE 09.2025)
        # Pokrywa Top 5 Powiatów Wiatrowych i Top 5 Solarnych w Polsce.
        self.locations = {
            # --- TIER 1: GIGANCI OZE (Bezpośrednie pokrycie Top 5) ---
            # Wiatr #1 (Sławno) + Wiatr #5 (Kołobrzeg - proxy)
            'Darlowo': {'lat': 54.42, 'lon': 16.41},

            # Solar #1 (Konin) + Solar #2 (Turek - 15km obok) -> "Wielkopolska Dolina Energii"
            'Konin': {'lat': 52.22, 'lon': 18.25},

            # Wiatr #2 (Słupsk) + Solar #5 (Słupsk) -> Hybrydowe serce północy
            'Slupsk': {'lat': 54.46, 'lon': 17.02},

            # Wiatr #3 (Białogard)
            'Bialogard': {'lat': 54.00, 'lon': 15.98},

            # Wiatr #4 (Sztum/Powiśle) + Kisielice
            'Sztum': {'lat': 53.92, 'lon': 19.03},

            # --- TIER 2: KLUCZOWE REGIONY (Top 10) ---
            'Zuromin': {'lat': 53.06, 'lon': 19.91},  # Wiatr: Ukryty gigant Mazowsza
            'Stargard': {'lat': 53.33, 'lon': 15.03},  # Solar #3: Zachodniopomorskie
            'Wejherowo': {'lat': 54.60, 'lon': 18.24},  # Solar #4: Pomorze
            'Zagan': {'lat': 51.61, 'lon': 15.31},  # Solar: Lubuskie ("Polski Teksas")
            'Legnica': {'lat': 51.20, 'lon': 16.16},  # Solar: Dolny Śląsk
            'Zamosc': {'lat': 50.72, 'lon': 23.25},  # Solar: Stabilizacja wschodnia

            # --- TIER 3: POPYT (LUDZIE I PRZEMYSŁ) ---
            'Warszawa': {'lat': 52.22, 'lon': 21.01},  # Biura (Load szczytowy)
            'Katowice': {'lat': 50.26, 'lon': 19.02},  # Przemysł (Load bazowy)
            'Lodz': {'lat': 51.75, 'lon': 19.45},  # Logistyka
            'Krakow': {'lat': 50.06, 'lon': 19.94},  # Południe
            'Suwalki': {'lat': 54.11, 'lon': 22.93},  # "Biegun Zimna" (Peak zimowy)
        }

    def run_etl(self, target_date):
        """Pobiera pogodę dla klastrów energetycznych"""
        try:
            logging.info(f"   ☁️ Fetching weather data for {len(self.locations)} strategic clusters...")
            df = self._fetch_weather(target_date)
            if not df.empty:
                self._save_to_sql(df)
                logging.info(f"   ✓ Weather data saved: {len(df)} rows")
            else:
                logging.warning("   ⚠️ No weather data fetched.")
        except Exception as e:
            logging.error(f"   ❌ Weather ETL Failed: {e}")
            raise e

    def _fetch_weather(self, date):
        all_data = []
        date_str = date.strftime('%Y-%m-%d')
        # Używamy forecast API dla danych bieżących (najbardziej aktualne)
        url = "https://api.open-meteo.com/v1/forecast"

        for city, coords in self.locations.items():
            params = {
                "latitude": coords['lat'],
                "longitude": coords['lon'],
                "start_date": date_str,
                "end_date": date_str,
                "hourly": "temperature_2m,wind_speed_10m,direct_radiation,cloud_cover",
                "timezone": "Europe/Warsaw"
            }

            try:
                r = requests.get(url, params=params)
                r.raise_for_status()
                data = r.json()

                hourly = data.get('hourly', {})
                times = hourly.get('time', [])
                temps = hourly.get('temperature_2m', [])
                winds = hourly.get('wind_speed_10m', [])
                solar = hourly.get('direct_radiation', [])
                clouds = hourly.get('cloud_cover', [])

                for i, t_str in enumerate(times):
                    all_data.append({
                        'location': city,
                        'dtime': t_str,
                        'temp_c': temps[i],
                        'wind_kph': winds[i],
                        'solar_rad': solar[i],
                        'cloud_cover': clouds[i]
                    })
                time.sleep(0.1)  # Grzeczność wobec API

            except Exception as e:
                logging.error(f"Error fetching {city}: {e}")

        if not all_data:
            return pd.DataFrame()
        return pd.DataFrame(all_data)

    def _save_to_sql(self, df):
        if df.empty:
            return

        # DDL - Tabela na dane pogodowe
        create_table_sql = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'weather_data')
        CREATE TABLE weather_data (
            location VARCHAR(50),
            dtime DATETIME,
            temp_c FLOAT,
            wind_kph FLOAT,
            solar_rad FLOAT,
            cloud_cover FLOAT,
            updated_at DATETIME DEFAULT GETDATE(),
            PRIMARY KEY (location, dtime)
        );
        """

        # UPSERT - Aktualizuj jeśli istnieje, wstaw jeśli nowe
        merge_sql = """
        MERGE INTO weather_data AS T
        USING (SELECT ? as loc, ? as dt, ? as tmp, ? as wnd, ? as sol, ? as cld) AS S
        ON T.location = S.loc AND T.dtime = S.dt
        WHEN MATCHED THEN UPDATE SET
            temp_c = S.tmp, wind_kph = S.wnd, solar_rad = S.sol, cloud_cover = S.cld, updated_at = GETDATE()
        WHEN NOT MATCHED THEN INSERT
            (location, dtime, temp_c, wind_kph, solar_rad, cloud_cover)
            VALUES (S.loc, S.dt, S.tmp, S.wnd, S.sol, S.cld);
        """

        with pyodbc.connect(self.sql_conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            conn.commit()

            for _, r in df.iterrows():
                # Bezpieczne wstawianie (obsługa NULL/None)
                vals = [r['location'], r['dtime'], r['temp_c'], r['wind_kph'], r['solar_rad'], r['cloud_cover']]
                vals = [None if pd.isna(v) else v for v in vals]

                cursor.execute(merge_sql, vals[0], vals[1], vals[2], vals[3], vals[4], vals[5])
            conn.commit()