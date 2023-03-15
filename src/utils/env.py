import os

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())


def get_required(env_name):
    env = os.getenv(env_name)

    if not env:
        raise AttributeError(
            f"Environment variable {env_name} is required but not found"
        )

    return env


def get_optional(env_name, default=None):
    return os.getenv(env_name, default)


def set_env(env_name, value):
    os.environ[env_name] = value
