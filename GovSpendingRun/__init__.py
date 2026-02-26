import datetime
import logging
import azure.functions as func

from gov_spending_radar.main import run


def main(myTimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

    if myTimer.past_due:
        logging.info("GovSpendingRun timer is past due!")

    logging.info(f"Gov Spending Radar pipeline started at {utc_timestamp}")

    try:
        result = run()

        logging.info(
            f"Gov Spending Radar completed: "
            f"notices={result['notices_uploaded']}, "
            f"contractors={result['contractors_uploaded']}, "
            f"classifications={result['classifications_uploaded']}"
        )

        if result.get("errors"):
            for err in result["errors"][:10]:
                logging.warning(f"Pipeline error: {err}")

        if not result["success"]:
            logging.error("Gov Spending Radar finished with errors")

    except Exception as e:
        logging.error(f"CRITICAL: Gov Spending Radar failed: {e}")
        raise
