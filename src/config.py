import json

from utils import env

if env.get_optional("SECRETS"):
    for key, value in json.loads(env.get_required("SECRETS")).items():
        env.set_env(key, value)


API_URL = env.get_optional("API_URL", "https://app.minterest.com/api")
SQL_CONNECTION_STRING = env.get_required("SQL_CONNECTION_STRING")
