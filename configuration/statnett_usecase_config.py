from pydantic import BaseModel, ValidationError

class Configuration(BaseModel):
    url: str
    limit: int
    interval: int
    delay: int
    backfill_date: str
    tz: str
    class Config:
        arbitrary_types_allowed = True