from __future__ import annotations

import hmac
from typing import Annotated

from fastapi import Header, HTTPException, Request, status


def verify_import_token(provided: str | None, expected: str) -> bool:
    if provided is None:
        return False
    return hmac.compare_digest(provided.encode("utf-8"), expected.encode("utf-8"))


async def require_import_token(
    request: Request,
    x_import_token: Annotated[str | None, Header()] = None,
) -> None:
    expected = request.app.state.settings.import_api_token
    if not verify_import_token(x_import_token, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="token de importação inválido",
        )

