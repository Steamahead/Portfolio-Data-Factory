import datetime
import logging
import azure.functions as func

# Importujemy Twoją główną funkcję
# UWAGA: Upewnij się, że shiller_index ma plik __init__.py!
from shiller_index.shiller_logic import run_shiller_analysis


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info(f'🚀 Azure Function: Shiller Index Run started at {utc_timestamp}')

    try:
        # --- URUCHOMIENIE TWOJEJ LOGIKI ---
        results = run_shiller_analysis()

        count = len(results)
        logging.info(f"✅ Success! Processed {count} tickers. Data saved to SQL.")

    except Exception as e:
        logging.error(f"❌ CRITICAL ERROR in Shiller Function: {str(e)}")
        # Tu można dodać wysyłanie maila/powiadomienia o błędzie

    logging.info('Azure Function run completed.')