import json

from utils import env

if env.get_optional("SECRETS"):
    for key, value in json.loads(env.get_required("SECRETS")).items():
        env.set_env(key, value)

INDEXER_DB_SQL_CONNECTION_STRING = env.get_required("INDEXER_DB_SQL_CONNECTION_STRING")
ANALYTICS_DB_SQL_CONNECTION_STRING = env.get_required(
    "ANALYTICS_DB_SQL_CONNECTION_STRING"
)
API_URL = env.get_required("API_URL")
MAX_TREADS_FOR_USERS_FETCHING = int(
    env.get_optional("MAX_TREADS_FOR_USERS_FETCHING", "10")
)
RETRIES_COUNT_FOR_USERS_FETCHING = int(
    env.get_optional("RETRIES_COUNT_FOR_USERS_FETCHING", "50")
)
ATHENA_BUCKET = env.get_required("ATHENA_BUCKET")
ATHENA_DB = env.get_required("ATHENA_DB")
AWS_REGIONS = env.get_optional("AWS_REGIONS", "eu-central-1")
IS_MANTLE_NETWORK = "mantle" in API_URL
