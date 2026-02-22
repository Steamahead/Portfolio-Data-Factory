import datetime
import logging
import azure.functions as func

from cee_fx_volatility.main import run


def main(myTimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

    if myTimer.past_due:
        logging.info("CeeFxDailyRun timer is past due!")

    logging.info(f"CEE FX Volatility pipeline started at {utc_timestamp}")

    try:
        result = run()

        logging.info(
            f"CEE FX pipeline completed: "
            f"FX={result['fx_uploaded']} records, "
            f"News={result['news_uploaded']} headlines "
            f"({result['news_classified']} classified)"
        )

        errors = result.get("fx_errors", []) + result.get("news_errors", [])
        if errors:
            for err in errors[:10]:
                logging.warning(f"Pipeline error: {err}")

        if not result["success"]:
            logging.error("CEE FX pipeline finished with no data uploaded")

    except Exception as e:
        logging.error(f"CRITICAL: CEE FX pipeline failed: {e}")
        raise
