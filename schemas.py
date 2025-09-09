from pydantic import BaseModel, Field
from typing import Optional

class InstantlyAccount(BaseModel):
    """
    Pydantic model to represent an email account from the Instantly.AI API.
    This helps with data validation and type hinting.
    """
    id: str
    account_id: Optional[str] = Field(None, alias='account_id')
    email: str
    provider: str
    warmup_status: str = Field(..., alias='warmup_status')
    connection_status: str = Field(..., alias='connection_status')
    daily_send_limit: int = Field(..., alias='daily_limit')
    sent_today: int = Field(..., alias='sent_count_today')
    warmup_score: Optional[float] = Field(None, alias='warmup_score')
    sending_gap_in_minutes: int = Field(..., alias='sending_gap')
    
    class Config:
        populate_by_name = True
        # Allow extra fields from the API response that are not defined in the model
        extra = 'ignore'
