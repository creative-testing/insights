"""
Configuration centralisée pour l'API FastAPI
Utilise pydantic-settings pour validation et typage fort
"""
from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    """Configuration de l'application avec validation Pydantic"""

    # Application
    ENVIRONMENT: str = "development"
    DEBUG: bool = True
    API_VERSION: str = "v1"
    SECRET_KEY: str = ""  # Optional for cron jobs
    SESSION_SECRET: str = ""  # Optional for cron jobs

    # Database
    DATABASE_URL: str

    # Meta/Facebook OAuth
    META_APP_ID: str = ""  # Optional for cron jobs (no OAuth callback)
    META_APP_SECRET: str = ""  # Optional for cron jobs
    META_API_VERSION: str = "v23.0"
    META_REDIRECT_URI: str = ""  # Optional for cron jobs

    # Production token (for seeding Ads Alchimie tenant)
    FACEBOOK_ACCESS_TOKEN: str = ""  # Optional, only for /dev/seed-production

    # Stripe (optional for cron jobs - no billing logic)
    STRIPE_SECRET_KEY: str = ""
    STRIPE_PUBLISHABLE_KEY: str = ""
    STRIPE_WEBHOOK_SECRET: str = ""
    STRIPE_PRICE_FREE: str = ""
    STRIPE_PRICE_PRO: str = ""
    STRIPE_PRICE_ENTERPRISE: str = ""

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"

    # Storage (R2/S3)
    STORAGE_MODE: str = "local"  # "local" or "r2"
    LOCAL_DATA_ROOT: str = "./data"  # For local storage mode
    STORAGE_ENDPOINT: str = ""
    STORAGE_ACCESS_KEY: str = ""
    STORAGE_SECRET_KEY: str = ""
    STORAGE_BUCKET: str = ""
    STORAGE_REGION: str = "auto"

    # Security
    TOKEN_ENCRYPTION_KEY: str
    JWT_ISSUER: str = "insights-api"  # JWT issuer claim

    # Supabase Auth Integration (pour unification auth)
    SUPABASE_JWT_SECRET: str = ""  # JWT secret pour valider les tokens Supabase
    SUPABASE_URL: str = ""  # URL du projet Supabase (ex: https://xxx.supabase.co)

    # Cookie settings (cross-site compatibility)
    COOKIE_SAMESITE: str = "lax"  # "none" if dashboard and API on different eTLD+1
    COOKIE_DOMAIN: str = ""  # ".yourdomain.com" for subdomain sharing

    # CORS
    ALLOWED_ORIGINS: str = "http://localhost:3000,https://insights.theaipipe.com"

    # Dashboard URL (for OAuth redirect after success)
    DASHBOARD_URL: str = "https://insights.theaipipe.com/oauth-callback.html"

    @property
    def allowed_origins_list(self) -> List[str]:
        """Parse CORS origins"""
        return [origin.strip() for origin in self.ALLOWED_ORIGINS.split(",")]

    # Rate Limiting
    RATE_LIMIT_PER_MINUTE: int = 60

    # Sentry
    SENTRY_DSN: str = ""

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "allow"  # Allow extra fields from .env without failing


# Instance globale
settings = Settings()
