from enum import Enum


class AdaptTimeOption(Enum):
    INVOCATION = 'INVOCATION'
    INGESTION = 'INGESTION'
    ORIGINAL = 'ORIGINAL'