import json

from utils import env
from base64 import b64decode

if env.get_optional("SECRETS"):
    for key, value in json.loads(env.get_required("SECRETS")).items():
        env.set_env(key, value)

INDEXER_DB_SQL_CONNECTION_STRING = env.get_required("INDEXER_DB_SQL_CONNECTION_STRING")
ANALYTICS_DB_SQL_CONNECTION_STRING = env.get_required(
    "ANALYTICS_DB_SQL_CONNECTION_STRING"
)
API_URL = env.get_required("API_URL")
MAX_TREADS_FOR_USERS_FETCHING = int(
    env.get_optional("MAX_TREADS_FOR_USERS_FETCHING", "5")
)
RETRIES_COUNT_FOR_USERS_FETCHING = int(
    env.get_optional("RETRIES_COUNT_FOR_USERS_FETCHING", "50")
)
S3_BUCKET = env.get_required("S3_BUCKET")
AWS_REGIONS = env.get_optional("AWS_REGIONS", "eu-central-1")
IS_MANTLE_NETWORK = "mantle" in API_URL
IS_TAIKO_NETWORK = "taiko" in API_URL
IS_ADAPTIFI_NETWORK = "adaptifi" in API_URL
IS_ETHEREUM_NETWORK = (
    not IS_MANTLE_NETWORK and not IS_TAIKO_NETWORK and not IS_ADAPTIFI_NETWORK
)

GOOGLE_CLOUD_CREDENTIALS = json.loads(
    b64decode(env.get_required("GOOGLE_CLOUD_CREDENTIALS"))
)

CS_ISSUES_TRACKER_GOOGLE_SHEETS_ID = "1rO-O1V9MJFEkFoTI6Or_s5ACiaLbICq_5F5jpUks_Lo"
MINTEREST_NFT_OVERVIEW_GOOGLE_SHEETS_ID = "1Z4XZc78IvmCe87PfTJ7rp-T6yMdul2HqvVnEaCueS04"
MOSSBETS_USER_DATA_GOOGLE_SHEETS_ID = "1EC5gE1KFwkQX58IV6M5PEjWLhjEeysV5wcFKMTXj8fs"
