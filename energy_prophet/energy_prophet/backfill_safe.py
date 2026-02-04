import os
import sys
import datetime
import logging
import time

# --- FIX IMPORTÓW ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

try:
    from energy_prophet.pse_connector import PSEConnector
    from energy_prophet.weather_connector import WeatherConnector  # <--- ODKOMENTOWANE
except ImportError:
    sys.path.append(os.path.join(parent_dir, 'energy_prophet'))
    from pse_connector import PSEConnector
    from weather_connector import WeatherConnector  # <--- ODKOMENTOWANE

# KONFIGURACJA
START_DATE = datetime.date(2024, 11, 1)
END_DATE = datetime.date.today() - datetime.timedelta(days=1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')


def run_backfill():
    print("🚀 STARTING FULL BACKFILL (ENERGY + WEATHER)")

    # --- USTAWIENIE CONNECTION STRING ---
    # WPISZ SWOJE HASŁO PONIŻEJ!
    conn_str = "Driver={ODBC Driver 18 for SQL Server};Server=sadza-portfolio-server.database.windows.net;Database=PortfolioMasterDB;Uid=Portfolio_db_admin;Pwd=Jestem_szczesliwy1!;Encrypt=yes;TrustServerCertificate=no;"

    os.environ['SqlConnectionString'] = conn_str
    # ------------------------------------

    # Inicjalizacja obu konektorów
    pse = PSEConnector()
    weather = WeatherConnector(conn_str)  # <--- NOWOŚĆ

    print(f"\n🌊 Rozpoczynam pełny backfill: {START_DATE} -> {END_DATE}")

    current_date = START_DATE

    while current_date <= END_DATE:
        print(f"\n📅 Processing: {current_date}")
        try:
            # 1. PSE (Dane energetyczne)
            print("   ⚡ Fetching PSE data...")
            pse.run_etl(current_date)

            # 2. POGODA (Dane meteo dla 16 klastrów)
            print("   ☁️ Fetching Weather data...")
            weather.run_etl(current_date)

            # Pauza dla kultury (Open-Meteo prosi o niebombardowanie)
            time.sleep(5)

        except Exception as e:
            print(f"   ⚠️ Error for {current_date}: {e}")

        current_date += datetime.timedelta(days=1)

    print("\n🏁 FULL BACKFILL COMPLETED SUCCESSFULLY!")


if __name__ == "__main__":
    run_backfill()