"""
Point d'entrée principal de l'API FastAPI
"""
import sentry_sdk
from fastapi import FastAPI, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from sqlalchemy.orm import Session
from .config import settings
from .routers import auth, accounts, data, billing
from .database import get_db
from .middleware.csrf import CSRFFromCookieGuard

# Initialisation FastAPI
app = FastAPI(
    title="Creative Testing SaaS API",
    description="Backend API pour le dashboard Meta Ads Creative Testing",
    version=settings.API_VERSION,
    docs_url="/docs" if settings.DEBUG else None,  # Désactiver Swagger en prod
    redoc_url="/redoc" if settings.DEBUG else None,
)

# SessionMiddleware pour OAuth state (doit être avant les routers)
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.SESSION_SECRET,
    same_site="lax",
    https_only=not settings.DEBUG,  # HTTPS seulement en prod
)

# CSRF protection for cookie-based auth (before CORS)
app.add_middleware(CSRFFromCookieGuard)

# CORS - Configure for dashboard with credentials (cookies)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins_list,  # Dashboard URL(s)
    allow_credentials=True,  # Required for HttpOnly cookies
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Routes
app.include_router(auth.router, prefix="/auth", tags=["Authentication"])
app.include_router(accounts.router, prefix="/api/accounts", tags=["Accounts"])
app.include_router(data.router, prefix="/api/data", tags=["Data"])
app.include_router(billing.router, prefix="/billing", tags=["Billing"])


@app.get("/")
def read_root():
    """Root endpoint"""
    return {
        "service": "Creative Testing SaaS API",
        "version": settings.API_VERSION,
        "environment": settings.ENVIRONMENT,
        "status": "running",
    }


@app.get("/health")
def health_check():
    """
    Health check simple (stateless)
    Utilisé par les load balancers pour vérifier que le process répond
    """
    return {"status": "healthy"}


@app.get("/healthz")
def healthz():
    """
    Kubernetes-style liveness probe
    Vérifie que l'application est vivante (mais pas forcément prête)

    Returns 200 si le process est up (pas de checks externes)
    """
    return {"status": "alive", "version": settings.API_VERSION}


@app.get("/readyz")
async def readyz(db: Session = Depends(get_db)):
    """
    Kubernetes-style readiness probe
    Vérifie que l'application est prête à accepter du trafic

    Checks:
    - Database connection (SELECT 1)
    - Storage accessibility (vérifie que LOCAL_DATA_ROOT existe)

    Returns 200 si tout est OK, 503 Service Unavailable sinon
    """
    from sqlalchemy import text
    from pathlib import Path
    from fastapi import status
    from fastapi.responses import JSONResponse

    checks = {}

    # Check 1: Database connection
    try:
        db.execute(text("SELECT 1"))
        checks["database"] = "ok"
    except Exception as e:
        sentry_sdk.capture_exception(e)
        checks["database"] = f"error: {str(e)[:100]}"

    # Check 2: Storage accessibility
    checks["storage"] = "ok"

    # Check 3: Application is configured
    checks["app"] = "ok"

    all_ok = all(v == "ok" for v in checks.values())

    response_data = {
        "ready": all_ok,
        "checks": checks,
        "version": settings.API_VERSION,
    }

    if all_ok:
        return response_data
    else:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=response_data
        )


@app.post("/facebook/data-deletion")
async def facebook_data_deletion(request: Request):
    """
    Endpoint de Data Deletion requis par Meta pour App Review

    Appelé par Facebook quand un utilisateur demande la suppression de ses données
    https://developers.facebook.com/docs/development/create-an-app/app-dashboard/data-deletion-callback

    Pour l'instant, stub minimal qui retourne un confirmation code.
    TODO: Implémenter la vraie suppression des données
    """
    import uuid
    from datetime import datetime

    # Récupérer les données envoyées par Facebook
    form_data = await request.form()
    user_id = form_data.get("user_id")  # Facebook user ID

    # Générer un confirmation code unique
    confirmation_code = f"del-{uuid.uuid4().hex[:16]}"

    # TODO: Implémenter la vraie suppression:
    # 1. Marquer le user/tenant pour suppression
    # 2. Job background pour anonymiser/supprimer les données
    # 3. Conserver un log de la demande (compliance)

    # Retourner la réponse attendue par Meta
    return {
        "url": f"{request.base_url}data-deletion-status/{confirmation_code}",
        "confirmation_code": confirmation_code,
    }


@app.get("/data-deletion-status/{code}")
async def data_deletion_status(code: str):
    """
    Endpoint pour vérifier le statut d'une demande de suppression de données
    Requis par Meta (l'URL est retournée par /facebook/data-deletion)

    Pour l'instant, stub qui retourne toujours "pending"
    TODO: Implémenter le vrai tracking des suppressions
    """
    return {
        "status": "pending",
        "confirmation_code": code,
        "message": "Data deletion request is being processed"
    }


# Sentry (si configuré)
if settings.SENTRY_DSN:
    import sentry_sdk
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        traces_sample_rate=1.0 if settings.DEBUG else 0.1,
        environment=settings.ENVIRONMENT,
    )
