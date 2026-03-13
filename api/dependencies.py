"""
Dependencias compartilhadas da API.

verify_api_key: valida o header X-API-Key nos endpoints publicos (DaaS).
"""

import os

from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyHeader

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=True)


def verify_api_key(api_key: str = Depends(api_key_header)) -> str:
    expected = os.getenv("CLIENT_API_KEY", "l2c-secret-key-123")
    if api_key != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API Key invalida ou ausente.",
        )
    return api_key
