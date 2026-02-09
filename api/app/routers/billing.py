"""
Router pour Stripe (Checkout + Webhooks)
"""
import sentry_sdk
from datetime import datetime
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.orm import Session
from pydantic import BaseModel
from ..database import get_db
from ..config import settings
from ..models import Tenant, Subscription
from ..dependencies.auth import get_current_tenant_id
import stripe

router = APIRouter()

# Configurer Stripe
stripe.api_key = settings.STRIPE_SECRET_KEY


class CheckoutSessionRequest(BaseModel):
    """Request body for creating checkout session"""
    plan: str  # "pro" or "enterprise" (free is default, no checkout needed)


@router.post("/create-checkout-session")
async def create_checkout_session(
    body: CheckoutSessionRequest,
    db: Session = Depends(get_db),
    current_tenant_id: UUID = Depends(get_current_tenant_id)
):
    """
    Crée une session Stripe Checkout pour upgrade de plan

    Args:
        body.plan: Plan choisi ("pro" or "enterprise")

    Returns:
        checkout_url: URL Stripe Checkout
    """
    # Validate plan
    if body.plan not in ["pro", "enterprise"]:
        raise HTTPException(
            status_code=400,
            detail="Invalid plan. Use 'pro' or 'enterprise'"
        )

    # Get tenant + subscription
    tenant = db.execute(
        select(Tenant).where(Tenant.id == current_tenant_id)
    ).scalar_one_or_none()

    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    subscription = db.execute(
        select(Subscription).where(Subscription.tenant_id == current_tenant_id)
    ).scalar_one_or_none()

    if not subscription:
        raise HTTPException(status_code=404, detail="Subscription not found")

    # Map plan -> price_id
    price_ids = {
        "pro": settings.STRIPE_PRICE_PRO,
        "enterprise": settings.STRIPE_PRICE_ENTERPRISE,
    }

    try:
        # Get or create Stripe customer
        customer_id = subscription.stripe_customer_id
        if not customer_id:
            # Create Stripe customer
            user_email = tenant.users[0].email if tenant.users else None
            customer = stripe.Customer.create(
                email=user_email,
                metadata={"tenant_id": str(current_tenant_id)}
            )
            customer_id = customer.id

            # Save customer_id to subscription
            subscription.stripe_customer_id = customer_id
            db.commit()

        # Create Stripe Checkout Session
        checkout_session = stripe.checkout.Session.create(
            customer=customer_id,
            payment_method_types=["card"],
            line_items=[
                {
                    "price": price_ids[body.plan],
                    "quantity": 1,
                }
            ],
            mode="subscription",
            success_url=f"{settings.DASHBOARD_URL}/billing/success?session_id={{CHECKOUT_SESSION_ID}}",
            cancel_url=f"{settings.DASHBOARD_URL}/billing/cancel",
            metadata={
                "tenant_id": str(current_tenant_id),
                "plan": body.plan,
            }
        )

        return {"checkout_url": checkout_session.url}

    except stripe.error.StripeError as e:
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=502, detail=f"Stripe API error: {str(e)}")
    except Exception as e:
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


@router.post("/webhook")
async def stripe_webhook(request: Request, db: Session = Depends(get_db)):
    """
    Webhook Stripe pour gérer les événements d'abonnement

    Events handled:
    - checkout.session.completed: New subscription created
    - customer.subscription.updated: Plan change, status change
    - customer.subscription.deleted: Cancellation
    - invoice.payment_failed: Payment issue
    """
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, settings.STRIPE_WEBHOOK_SECRET
        )
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid payload")
    except stripe.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid signature")

    event_type = event["type"]
    data = event["data"]["object"]

    if event_type == "checkout.session.completed":
        # New subscription created via Checkout
        tenant_id_str = data.get("metadata", {}).get("tenant_id")
        plan = data.get("metadata", {}).get("plan")

        if tenant_id_str and plan:
            subscription = db.execute(
                select(Subscription).where(Subscription.tenant_id == UUID(tenant_id_str))
            ).scalar_one_or_none()

            if subscription:
                # Extract Stripe subscription ID from checkout session
                stripe_subscription_id = data.get("subscription")

                # Update subscription
                subscription.plan = plan
                subscription.status = "active"
                subscription.stripe_subscription_id = stripe_subscription_id

                # Update quotas based on plan
                if plan == "pro":
                    subscription.quota_accounts = 10
                    subscription.quota_refresh_per_day = 5
                elif plan == "enterprise":
                    subscription.quota_accounts = 999  # Unlimited
                    subscription.quota_refresh_per_day = 999

                db.commit()

    elif event_type == "customer.subscription.updated":
        # Subscription status or plan changed
        stripe_subscription_id = data.get("id")
        stripe_status = data.get("status")  # active, past_due, canceled, etc.
        current_period_end_ts = data.get("current_period_end")

        subscription = db.execute(
            select(Subscription).where(Subscription.stripe_subscription_id == stripe_subscription_id)
        ).scalar_one_or_none()

        if subscription:
            # Map Stripe status to our enum
            status_map = {
                "active": "active",
                "trialing": "trialing",
                "past_due": "past_due",
                "canceled": "canceled",
                "incomplete": "incomplete",
            }
            subscription.status = status_map.get(stripe_status, "active")

            # Update current_period_end
            if current_period_end_ts:
                subscription.current_period_end = datetime.fromtimestamp(current_period_end_ts)

            db.commit()

    elif event_type == "customer.subscription.deleted":
        # Subscription canceled - downgrade to FREE
        stripe_subscription_id = data.get("id")

        subscription = db.execute(
            select(Subscription).where(Subscription.stripe_subscription_id == stripe_subscription_id)
        ).scalar_one_or_none()

        if subscription:
            subscription.plan = "free"
            subscription.status = "canceled"
            subscription.quota_accounts = 3
            subscription.quota_refresh_per_day = 1
            subscription.stripe_subscription_id = None
            db.commit()

    elif event_type == "invoice.payment_failed":
        # Payment failed - mark as past_due
        customer_id = data.get("customer")

        subscription = db.execute(
            select(Subscription).where(Subscription.stripe_customer_id == customer_id)
        ).scalar_one_or_none()

        if subscription:
            subscription.status = "past_due"
            db.commit()

            # TODO: Send email/Slack alert to user
            # TODO: Log for monitoring

    return {"status": "success"}
