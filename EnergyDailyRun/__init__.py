import datetime
import logging
import os
import azure.functions as func

# Importujemy OBA konektory: stary (PSE) i nowy (Weather)
from energy_prophet.pse_connector import PSEConnector
from energy_prophet.weather_connector import WeatherConnector


def main(myTimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    try:
        # 1. Konfiguracja wstępna
        today = datetime.date.today()
        # Pobieramy Connection String raz, żeby przekazać go do WeatherConnector
        conn_str = os.environ.get('SqlConnectionString')

        logging.info(f"Starting Energy Prophet ETL for execution date: {today}")

        # --- ETAP 1: PSE (Dane rynkowe) ---
        logging.info("--- PHASE 1: PSE DATA ---")
        try:
            pse = PSEConnector()
            pse.run_etl(today)
            logging.info("✓ PSE ETL finished.")
        except Exception as e:
            logging.error(f"❌ PSE ETL Failed: {e}")
            # Nie przerywamy (catch-all), żeby spróbować pobrać chociaż pogodę

        # --- ETAP 2: POGODA (Nowość - Dane meteo dla klastrów OZE) ---
        logging.info("--- PHASE 2: WEATHER DATA ---")
        if conn_str:
            try:
                weather = WeatherConnector(conn_str)
                weather.run_etl(today)
                logging.info("✓ Weather ETL finished.")
            except Exception as e:
                logging.error(f"❌ Weather ETL Failed: {e}")
                # Logujemy błąd, ale nie zabijamy całej funkcji
        else:
            logging.error("❌ Skipping Weather ETL: Missing SqlConnectionString environment variable.")

        logging.info("ALL ETL TASKS COMPLETED.")

    except Exception as e:
        # Ten blok łapie błędy krytyczne (np. brak bibliotek, awaria systemu)
        logging.error(f"CRITICAL FUNCTION FAILURE: {str(e)}")
        raise e