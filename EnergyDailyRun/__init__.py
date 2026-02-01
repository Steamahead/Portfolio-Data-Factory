import datetime
import logging
import azure.functions as func
# ZMIANA: Importujemy klasę, a nie starą funkcję
from energy_prophet.pse_connector import PSEConnector


def main(myTimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    # --- NOWA LOGIKA ETL ---
    try:
        # 1. Inicjalizacja konektora
        connector = PSEConnector()

        # 2. Uruchomienie ETL dla dzisiejszej daty
        # (Konektor sam cofnie się o 1 dzień dla cen i o 7 dni dla rozliczeń)
        today = datetime.date.today()
        logging.info(f"Starting Energy Prophet ETL for execution date: {today}")

        connector.run_etl(today)

        logging.info("Energy Prophet ETL finished successfully.")

    except Exception as e:
        logging.error(f"CRITICAL ETL FAILURE: {str(e)}")
        raise e  # Rzuć błąd, żeby Azure odnotował fail