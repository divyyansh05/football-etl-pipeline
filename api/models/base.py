from pydantic import BaseModel
from typing import Generic, TypeVar, List, Optional, Any

T = TypeVar("T")

class ResponseSingle(BaseModel, Generic[T]):
    """Standard wrapper for a single object response."""
    data: T

class ResponseList(BaseModel, Generic[T]):
    """Standard wrapper for a list response with pagination metadata."""
    data: List[T]
    total: int
    limit: int
    offset: int

class ErrorResponse(BaseModel):
    """Standard error response format."""
    error: str
    detail: Optional[str] = None
