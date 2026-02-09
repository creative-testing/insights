"""
Router pour la gestion des comptes publicitaires et informations utilisateur

🔒 LIMITES GLOBALES: L'API a la priorité sur le CRON
- API peut utiliser jusqu'à 10 workers (nouvel user qui attend)
- Le CRON s'efface automatiquement si l'API est occupée
"""
import sentry_sdk
from uuid import UUID
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import select
from typing import Dict, Any, Optional, List
import asyncio

from ..database import get_db, SessionLocal
from ..dependencies.auth import get_current_tenant_id, get_current_user_id
from .. import models
from ..models.refresh_job import RefreshJob, JobStatus
from ..services.refresher import sync_account_data, RefreshError
from ..config import settings
from ..utils.jwt import create_access_token
from ..utils.job_limiter import can_api_proceed, MAX_API_WORKERS

router = APIRouter()


def _utcnow():
    """Helper pour datetime UTC"""
    return datetime.now(timezone.utc)


async def _run_refresh_job(job_id: UUID, fb_account_id: str, tenant_id: UUID):
    """
    Exécute le refresh en background et met à jour le statut du job.

    Cette fonction tourne en background via FastAPI BackgroundTasks.
    Elle crée sa propre session DB pour éviter les conflits.
    """
    db = SessionLocal()
    try:
        # 1. Marquer le job comme RUNNING
        job = db.get(RefreshJob, job_id)
        if not job:
            return
        job.status = JobStatus.RUNNING
        job.started_at = _utcnow()
        db.commit()

        # 2. Exécuter la sync (30s-15min selon la taille du compte)
        await sync_account_data(
            ad_account_id=fb_account_id,
            tenant_id=tenant_id,
            db=db
        )

        # 3. Marquer comme OK
        job = db.get(RefreshJob, job_id)
        if job:
            job.status = JobStatus.OK
            job.finished_at = _utcnow()
            db.commit()

    except Exception as e:
        sentry_sdk.capture_exception(e)
        # 4. En cas d'erreur, marquer comme ERROR
        job = db.get(RefreshJob, job_id)
        if job:
            job.status = JobStatus.ERROR
            job.error = str(e)[:1000]  # Limiter à 1000 chars
            job.finished_at = _utcnow()
            db.commit()
    finally:
        db.close()


@router.get("/me")
async def get_me(
    current_tenant_id: UUID = Depends(get_current_tenant_id),
    current_user_id: UUID = Depends(get_current_user_id),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Retourne les informations de l'utilisateur connecté

    🔒 Protected endpoint - requires valid JWT (header or cookie)

    Returns:
        {
            "tenant_id": "uuid",
            "user_id": "uuid",
            "email": "user@example.com",
            "name": "User Name"
        }
    """
    user = db.get(models.User, current_user_id)

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    return {
        "tenant_id": str(current_tenant_id),
        "user_id": str(current_user_id),
        "email": user.email,
        "name": user.name,
    }


@router.get("/")
async def list_accounts(
    current_tenant_id: UUID = Depends(get_current_tenant_id),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Liste tous les ad accounts du tenant actuel

    🔒 Protected endpoint - requires valid JWT
    🏢 Tenant-isolated - only returns accounts for authenticated tenant

    Returns:
        {
            "accounts": [...]
        }
    """
    accounts = db.execute(
        select(models.AdAccount).where(
            models.AdAccount.tenant_id == current_tenant_id
        )
    ).scalars().all()

    return {
        "accounts": [
            {
                "id": str(acc.id),
                "fb_account_id": acc.fb_account_id,
                "name": acc.name,
                "profile": acc.profile,
                "currency": acc.currency,  # USD, MXN, EUR, etc.
                "last_refresh_at": acc.last_refresh_at.isoformat() if acc.last_refresh_at else None,
            }
            for acc in accounts
        ]
    }


@router.post("/refresh/{fb_account_id}")
async def trigger_refresh(
    fb_account_id: str,
    background_tasks: BackgroundTasks,
    current_tenant_id: UUID = Depends(get_current_tenant_id),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Déclenche un refresh asynchrone des données pour un compte

    🔒 Protected endpoint - requires valid JWT
    🏢 Tenant-isolated - can only refresh accounts belonging to your tenant
    ⚡ Asynchronous - returns immediately with job_id, polling required

    Flow:
    1. Vérifie ownership du compte (tenant isolation)
    2. Crée un RefreshJob (status=QUEUED)
    3. Lance le refresh en background
    4. Retourne immédiatement avec job_id

    Returns:
        {
            "status": "processing",
            "job_id": "uuid",
            "already_processing": bool
        }
    """
    # 1. Vérifier que l'ad account appartient au tenant
    ad_account = db.execute(
        select(models.AdAccount).where(
            models.AdAccount.fb_account_id == fb_account_id,
            models.AdAccount.tenant_id == current_tenant_id
        )
    ).scalar_one_or_none()

    if not ad_account:
        raise HTTPException(
            status_code=404,
            detail=f"Ad account {fb_account_id} not found for your workspace"
        )

    # 2. Vérifier si un job est déjà en cours (idempotence)
    existing_job = db.execute(
        select(RefreshJob).where(
            RefreshJob.tenant_id == current_tenant_id,
            RefreshJob.ad_account_id == ad_account.id,
            RefreshJob.status.in_([JobStatus.QUEUED, JobStatus.RUNNING])
        )
    ).scalar_one_or_none()

    if existing_job:
        return {
            "status": "processing",
            "job_id": str(existing_job.id),
            "already_processing": True
        }

    # 3. Créer un nouveau job
    job = RefreshJob(
        tenant_id=current_tenant_id,
        ad_account_id=ad_account.id,
        status=JobStatus.QUEUED
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    # 4. Lancer le refresh en background
    background_tasks.add_task(
        _run_refresh_job,
        job.id,
        fb_account_id,
        current_tenant_id
    )

    return {
        "status": "processing",
        "job_id": str(job.id),
        "already_processing": False
    }


@router.get("/refresh/status/{job_id}")
async def get_refresh_status(
    job_id: UUID,
    current_tenant_id: UUID = Depends(get_current_tenant_id),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Récupère le statut d'un job de refresh

    🔒 Protected endpoint - requires valid JWT
    🏢 Tenant-isolated - can only check jobs belonging to your tenant

    Returns:
        {
            "status": "queued" | "running" | "ok" | "error",
            "started_at": "ISO datetime" | null,
            "finished_at": "ISO datetime" | null,
            "error": "error message" | null
        }
    """
    job = db.get(RefreshJob, job_id)

    if not job or job.tenant_id != current_tenant_id:
        raise HTTPException(
            status_code=404,
            detail="Job not found"
        )

    return {
        "status": job.status.value,  # Convertir enum en string
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "finished_at": job.finished_at.isoformat() if job.finished_at else None,
        "error": job.error
    }


@router.post("/refresh-tenant-accounts")
async def refresh_tenant_accounts(
    background_tasks: BackgroundTasks,
    current_tenant_id: UUID = Depends(get_current_tenant_id),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Déclenche le refresh de TOUS les ad accounts du tenant authentifié.

    🔒 Protected - requires valid JWT
    🏢 Tenant-isolated - only refreshes YOUR accounts
    ⚡ Async - returns immediately, jobs run in background
    🔴 PRIORITÉ HAUTE - L'API a la priorité sur le CRON

    Use case: Nouvel utilisateur qui vient de se connecter via OAuth
    et ne veut pas attendre le cron (2h max).

    Returns:
        {
            "status": "processing",
            "accounts_total": 60,
            "jobs_launched": 10,
            "jobs_queued_for_later": 50,
            "estimated_time_minutes": 15
        }
    """
    # 1. Vérifier les slots disponibles (nettoie aussi les zombies)
    can_proceed, available_slots, message = can_api_proceed(db)

    if not can_proceed:
        # Système saturé - rare, mais possible
        return {
            "status": "system_busy",
            "message": message,
            "retry_in_minutes": 5
        }

    # 2. Récupérer tous les ad accounts du tenant
    accounts = db.execute(
        select(models.AdAccount).where(
            models.AdAccount.tenant_id == current_tenant_id
        )
    ).scalars().all()

    if not accounts:
        return {
            "status": "no_accounts",
            "accounts_total": 0,
            "jobs_launched": 0,
            "estimated_time_minutes": 0
        }

    # 3. Créer et lancer les jobs (limité par available_slots)
    jobs_launched = []
    jobs_already_running = []
    slots_used = 0

    for account in accounts:
        # Check idempotence - skip if already running
        existing = db.execute(
            select(RefreshJob).where(
                RefreshJob.tenant_id == current_tenant_id,
                RefreshJob.ad_account_id == account.id,
                RefreshJob.status.in_([JobStatus.QUEUED, JobStatus.RUNNING])
            )
        ).scalar_one_or_none()

        if existing:
            jobs_already_running.append({
                "account_id": account.fb_account_id,
                "job_id": str(existing.id)
            })
            continue

        # Vérifier si on a encore des slots
        if slots_used >= available_slots:
            # Plus de slots - les comptes restants seront traités par le cron
            break

        # Create new job
        job = RefreshJob(
            tenant_id=current_tenant_id,
            ad_account_id=account.id,
            status=JobStatus.QUEUED
        )
        db.add(job)
        db.commit()
        db.refresh(job)

        # Launch in background
        background_tasks.add_task(
            _run_refresh_job,
            job.id,
            account.fb_account_id,
            current_tenant_id
        )
        jobs_launched.append({
            "account_id": account.fb_account_id,
            "job_id": str(job.id)
        })
        slots_used += 1

    # Comptes non traités (seront pris par le cron)
    accounts_remaining = len(accounts) - len(jobs_launched) - len(jobs_already_running)

    # Estimation: ~30s par compte avec 10 workers parallèles
    estimated_minutes = max(5, (len(jobs_launched) * 30) // 60)

    return {
        "status": "processing",
        "accounts_total": len(accounts),
        "jobs_launched": len(jobs_launched),
        "jobs_already_running": len(jobs_already_running),
        "accounts_queued_for_cron": accounts_remaining,
        "available_slots": available_slots,
        "estimated_time_minutes": estimated_minutes,
        "message": f"{len(jobs_launched)} comptes en cours de traitement" + (
            f", {accounts_remaining} seront traités par le cron" if accounts_remaining > 0 else ""
        )
    }


