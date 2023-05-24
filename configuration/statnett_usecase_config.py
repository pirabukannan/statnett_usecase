from pydantic import BaseModel, ValidationError

class Configuration(BaseModel):
    url: str
    limit: int
    interval: int
    backfill_interval: int
    delay: int
    backfill_date: str
    backfill_ind: str
    tz: str
    class Config:
        arbitrary_types_allowed = True