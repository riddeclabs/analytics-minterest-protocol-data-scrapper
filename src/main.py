import logging

import pipelines

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


if __name__ == "__main__":
    pipelines.run_raw_markets_pipeline()
    pipelines.run_raw_oracle_prices_pipeline()
    pipelines.run_raw_users_pipeline()

    pipelines.run_curated_markets_pipeline()
    pipelines.run_curated_oracle_prices_pipeline()
    pipelines.run_curated_users_pipeline()
    pipelines.run_curated_user_markets_pipeline()
