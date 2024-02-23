from sqlalchemy import types
from sqlalchemy.dialects.postgresql import JSONB


class Types:
    DateTime = types.DateTime
    Date = types.Date
    String = types.String
    JSON = JSONB
    Int = types.Integer
    Double = types.Double
