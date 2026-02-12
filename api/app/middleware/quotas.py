"""
Quota Checking Helpers (Log-Only for MVP)

Checks subscription quotas before expensive operations (like data refresh).
For MVP: logs warnings but doesn't block requests.

Future: enforce quotas by raising HTTP 429 errors.
"""
import logging
from datetime import datetime, timezone
from uuid import UUID
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from ..models import Subscription, RefreshJob

logger = logging.getLogger(__name__)


def check_refresh_quota(tenant_id: UUID, db: Session, enforce: bool = False) -> dict:
    """
    Check if tenant has exceeded their daily refresh quota

    Args:
        tenant_id: Tenant UUID
        db: Database session
        enforce: If True, raises HTTPException when quota exceeded (Future)

    Returns:
        dict with quota info: {
            "quota_exceeded": bool,
            "plan": str,
            "refresh_count_today": int,
            "quota_refresh_per_day": int
        }
    """
    # Get subscription
    subscription = db.execute(
        select(Subscription).where(Subscription.tenant_id == tenant_id)
    ).scalar_one_or_none()

    if not subscription:
        logger.warning(f"No subscription found for tenant {tenant_id}")
        return {
            "quota_exceeded": False,
            "plan": "free",
            "refresh_count_today": 0,
            "quota_refresh_per_day": 1,
        }

    # Count refresh jobs created today
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    refresh_count_today = db.execute(
        select(func.count(RefreshJob.id))
        .where(
            RefreshJob.tenant_id == tenant_id,
            RefreshJob.created_at >= today_start
        )
    ).scalar()

    quota_exceeded = refresh_count_today >= subscription.quota_refresh_per_day

    quota_info = {
        "quota_exceeded": quota_exceeded,
        "plan": subscription.plan.value,
        "refresh_count_today": refresh_count_today,
        "quota_refresh_per_day": subscription.quota_refresh_per_day,
    }

    if quota_exceeded:
        logger.warning(
            f"[QUOTA] Tenant {tenant_id} exceeded daily refresh quota: "
            f"plan={quota_info['plan']}, "
            f"used={refresh_count_today}/{subscription.quota_refresh_per_day}"
        )

        if enforce:
            # Future: raise HTTP 429
            from fastapi import HTTPException
            raise HTTPException(
                status_code=429,
                detail=f"Daily refresh quota exceeded ({refresh_count_today}/{subscription.quota_refresh_per_day}). Please upgrade your plan."
            )

    return quota_info
