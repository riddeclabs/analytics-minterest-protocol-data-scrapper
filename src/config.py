import json

from utils import env

if env.get_optional("SECRETS"):
    for key, value in json.loads(env.get_required("SECRETS")).items():
        env.set_env(key, value)


INDEXER_DB_NAME = env.get_required("INDEXER_DB_NAME")
API_URL = env.get_required("API_URL")
SQL_CONNECTION_STRING = env.get_required("SQL_CONNECTION_STRING")
MAX_TREADS_FOR_USERS_FETCHING = env.get_optional("MAX_TREADS_FOR_USERS_FETCHING", 10)
