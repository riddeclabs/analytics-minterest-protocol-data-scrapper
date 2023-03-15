import logging

from pipelines import run_markets_pipeline, run_users_pipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


if __name__ == "__main__":
    logging.info("Running markets pipeline")
    run_markets_pipeline()
    logging.info("Markets data has been successfully saved to DB")

    logging.info("Running users pipeline")
    users_count = run_users_pipeline()
    logging.info(f"{users_count} users were successfully saved to DB")
