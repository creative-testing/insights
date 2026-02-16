"""
Authentication dependencies for FastAPI endpoints
Provides JWT-based tenant isolation
Supports both Bearer token (header) and HttpOnly cookie

SECURITY: Includes "Gatekeeper" pattern to detect zombie users
(authenticated in Supabase but not synced to local PostgreSQL)
"""
from typing import Optional
from uuid import UUID
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError
from sqlalchemy.orm import Session
from sqlalchemy import select

from ..utils.jwt import verify_token
from ..database import get_db
from ..models.tenant import Tenant

# HTTPBearer scheme for extracting "Bearer <token>" from Authorization header
# auto_error=False allows us to fallback to cookie if header is missing
http_bearer = HTTPBearer(auto_error=False)


def _extract_token(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials]
) -> Optional[str]:
    """
    Extract JWT token from Authorization header OR HttpOnly cookie

    Priority:
    1. Authorization: Bearer <token> header (for API clients)
    2. access_token cookie (for browser-based dashboard)

    Returns:
        Token string or None
    """
    # Try header first (Bearer token)
    if credentials and credentials.scheme.lower() == "bearer" and credentials.credentials:
        return credentials.credentials

    # Fallback to cookie (for dashboard)
    cookie_token = request.cookies.get("access_token")
    return cookie_token


def get_current_tenant_id(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(http_bearer),
    db: Session = Depends(get_db)
) -> UUID:
    """
    Extract and validate JWT, return tenant_id for multi-tenant isolation

    This dependency:
    1. Extracts JWT from Authorization header OR cookie
    2. Verifies signature, expiration, audience
    3. GATEKEEPER: Verifies tenant actually exists in PostgreSQL
    4. Returns tenant_id (UUID) for filtering queries

    Raises:
        HTTPException 401: If token is missing, invalid, or expired
        HTTPException 412: If tenant doesn't exist (zombie user - auth OK but sync failed)
    """
    token = _extract_token(request, credentials)

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"}
        )

    try:
        payload = verify_token(token)
        tenant_id = UUID(payload["tid"])

        # GATEKEEPER: Verify tenant actually exists in PostgreSQL
        # This catches "zombie users" who authenticated via Supabase
        # but whose sync to local DB failed
        tenant_exists = db.execute(
            select(Tenant.id).where(Tenant.id == tenant_id)
        ).scalar()

        if not tenant_exists:
            raise HTTPException(
                status_code=status.HTTP_412_PRECONDITION_FAILED,
                detail="Account not synchronized. Please login again.",
                headers={"X-Sync-Status": "missing"}
            )

        return tenant_id

    except JWTError as e:
        # Expected: expired tokens, invalid signatures — don't pollute Sentry
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid authentication credentials: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except (KeyError, ValueError) as e:
        # Malformed token payload — client error, not a server crash
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Malformed token payload: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )


def get_current_user_id(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(http_bearer)
) -> UUID:
    """
    Extract and validate JWT, return user_id

    Optional dependency for endpoints that need user-specific logic
    """
    token = _extract_token(request, credentials)

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"}
        )

    try:
        payload = verify_token(token)
        user_id = UUID(payload["sub"])
        return user_id

    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid authentication credentials: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except (KeyError, ValueError) as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Malformed token payload: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )
