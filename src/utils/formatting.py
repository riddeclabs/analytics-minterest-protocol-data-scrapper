import re


def to_snake_case(value: str):
    return (
        re.sub(r"(?:(?<=[a-z])(?=[A-Z]))|[^a-zA-Z0-9]", " ", value)
        .replace(" ", "_")
        .lower()
        .strip("_")
    )
