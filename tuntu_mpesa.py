import os
import base64
import logging
import threading
import time as _time
from datetime import datetime
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ── M-Pesa Daraja Config ─────────────────────────────────
MPESA_ENV             = os.getenv("MPESA_ENV", "sandbox")   # "sandbox" | "production"
CONSUMER_KEY          = os.getenv("MPESA_CONSUMER_KEY", "")
CONSUMER_SECRET       = os.getenv("MPESA_CONSUMER_SECRET", "")
SHORTCODE             = os.getenv("MPESA_SHORTCODE", "6761606")
PASSKEY               = os.getenv("MPESA_PASSKEY", "")
CALLBACK_URL          = os.getenv("MPESA_CALLBACK_URL", "")
B2C_SHORTCODE         = os.getenv("MPESA_B2C_SHORTCODE", SHORTCODE)
B2C_INITIATOR_NAME    = os.getenv("MPESA_B2C_INITIATOR_NAME", "")
B2C_SECURITY_CREDENTIAL = os.getenv("MPESA_B2C_SECURITY_CREDENTIAL", "")
B2C_RESULT_URL        = os.getenv("MPESA_B2C_RESULT_URL", "")
B2C_QUEUE_TIMEOUT_URL = os.getenv("MPESA_B2C_QUEUE_TIMEOUT_URL", "")

BASE_URL = (
    "https://sandbox.safaricom.co.ke"
    if MPESA_ENV == "sandbox"
    else "https://api.safaricom.co.ke"
)

# Daraja field length limits
_MAX_ACCOUNT_REF  = 12
_MAX_TXN_DESC     = 13
_MAX_REMARKS      = 100
_MAX_OCCASION     = 100

# ── Startup config warnings ──────────────────────────────
def _warn_missing_config() -> None:
    required = {
        "MPESA_CONSUMER_KEY":    CONSUMER_KEY,
        "MPESA_CONSUMER_SECRET": CONSUMER_SECRET,
        "MPESA_SHORTCODE":       SHORTCODE,
        "MPESA_PASSKEY":         PASSKEY,
        "MPESA_CALLBACK_URL":    CALLBACK_URL,
    }
    for name, val in required.items():
        if not val:
            logger.warning(f"⚠️  M-Pesa config missing: {name} is not set")

_warn_missing_config()


# ── Token cache (thread-safe) ────────────────────────────
class _TokenCache:
    """
    Caches the Safaricom OAuth token for its lifetime (~1 hour).
    Avoids a redundant round-trip on every STK push / B2C call.
    """
    def __init__(self) -> None:
        self._token: Optional[str] = None
        self._expires_at: float = 0.0
        self._lock = threading.Lock()

    def get(self) -> str:
        with self._lock:
            # Refresh 60 s before expiry to avoid edge-case races
            if self._token and _time.monotonic() < self._expires_at - 60:
                return self._token
            self._token, ttl = self._fetch()
            self._expires_at = _time.monotonic() + ttl
            return self._token

    @staticmethod
    def _fetch() -> tuple[str, int]:
        url = f"{BASE_URL}/oauth/v1/generate?grant_type=client_credentials"
        credentials = base64.b64encode(
            f"{CONSUMER_KEY}:{CONSUMER_SECRET}".encode()
        ).decode()
        resp = requests.get(
            url,
            headers={"Authorization": f"Basic {credentials}"},
            timeout=10,
        )
        _log_response_on_error(resp, "OAuth token fetch")
        resp.raise_for_status()
        data = resp.json()
        token = data.get("access_token")
        if not token:
            raise ValueError(f"No access_token in OAuth response: {resp.text}")
        ttl = int(data.get("expires_in", 3600))
        logger.debug("🔑 M-Pesa OAuth token refreshed (ttl=%ds)", ttl)
        return token, ttl


_token_cache = _TokenCache()


# ── Internal helpers ─────────────────────────────────────

def _get_access_token() -> str:
    """Return a valid cached OAuth token, refreshing only when needed."""
    return _token_cache.get()


def _build_password() -> tuple[str, str]:
    """Return (base64_password, timestamp) for STK push."""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    raw = f"{SHORTCODE}{PASSKEY}{timestamp}"
    password = base64.b64encode(raw.encode()).decode()
    return password, timestamp


def _normalize_phone(raw: str) -> str:
    """
    Normalise any Kenyan phone format to 254XXXXXXXXX (12 digits).
    Handles: +254..., 254..., 07..., 01..., whatsapp:+254...
    """
    phone = str(raw or "").strip()
    # Strip WhatsApp prefix if caller forgot to normalise beforehand
    if phone.lower().startswith("whatsapp:"):
        phone = phone[len("whatsapp:"):]
    phone = phone.lstrip("+").strip()
    if phone.startswith("0") and len(phone) == 10:
        phone = "254" + phone[1:]
    if not phone.startswith("254"):
        phone = "254" + phone
    return phone


def _log_response_on_error(resp: requests.Response, label: str) -> None:
    """Log the full response body before raise_for_status so error details aren't lost."""
    if not resp.ok:
        logger.error(
            "❌ %s failed [HTTP %s]: %s",
            label, resp.status_code, resp.text,
        )


def _post_with_retry(
    url: str,
    payload: dict,
    label: str,
    max_attempts: int = 3,
    backoff: float = 1.5,
) -> dict:
    """
    POST with automatic retry on transient errors (429, 5xx).
    Refreshes the OAuth token on 401 and retries once.

    Returns the parsed JSON response dict.
    Raises requests.HTTPError on final failure.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        token = _get_access_token()
        try:
            resp = requests.post(
                url,
                json=payload,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type":  "application/json",
                },
                timeout=20,
            )
            logger.info("📡 %s response [HTTP %s | attempt %d]: %s", label, resp.status_code, attempt, resp.text)

            # 401 → stale token; force a refresh and retry immediately
            if resp.status_code == 401:
                logger.warning("🔄 401 Unauthorized — forcing token refresh (attempt %d)", attempt)
                _token_cache._expires_at = 0.0   # invalidate cache
                last_exc = requests.HTTPError(response=resp)
                continue

            # 429 / 5xx → transient; back off and retry
            if resp.status_code == 429 or resp.status_code >= 500:
                _log_response_on_error(resp, label)
                last_exc = requests.HTTPError(response=resp)
                if attempt < max_attempts:
                    sleep_secs = backoff * attempt
                    logger.warning("⏳ %s transient error — retrying in %.1fs (attempt %d/%d)", label, sleep_secs, attempt, max_attempts)
                    _time.sleep(sleep_secs)
                continue

            # 4xx (not 401) → permanent client error; log and raise immediately
            _log_response_on_error(resp, label)
            resp.raise_for_status()
            return resp.json()

        except requests.Timeout:
            last_exc = requests.Timeout(f"{label} timed out on attempt {attempt}")
            logger.warning("⏱ %s timed out (attempt %d/%d)", label, attempt, max_attempts)
            if attempt < max_attempts:
                _time.sleep(backoff * attempt)

    raise last_exc or RuntimeError(f"{label} failed after {max_attempts} attempts")


# ── Public API ───────────────────────────────────────────

def stk_push(
    phone_number: str,
    amount: int,
    account_reference: str,
    transaction_desc: str,
) -> dict:
    """
    Initiate an M-Pesa STK Push (Lipa Na M-Pesa Online).

    Args:
        phone_number:      Kenyan phone in any format — 07xx, 2547xx, +2547xx
        amount:            Amount in KES (integer, must be >= 1)
        account_reference: Shown on customer's phone (truncated to 12 chars)
        transaction_desc:  Short description (truncated to 13 chars)

    Returns:
        Safaricom JSON response dict, e.g.:
        {
          "MerchantRequestID":  "...",
          "CheckoutRequestID":  "...",
          "ResponseCode":       "0",
          "ResponseDescription":"Success. Request accepted for processing",
          "CustomerMessage":    "..."
        }

    Raises:
        requests.HTTPError — on non-2xx HTTP response after retries
        ValueError         — on bad config or missing token
    """
    phone = _normalize_phone(phone_number)
    password, timestamp = _build_password()

    payload = {
        "BusinessShortCode": SHORTCODE,
        "Password":          password,
        "Timestamp":         timestamp,
        "TransactionType":   "CustomerPayBillOnline",
        "Amount":            int(amount),
        "PartyA":            phone,
        "PartyB":            SHORTCODE,
        "PhoneNumber":       phone,
        "CallBackURL":       CALLBACK_URL,
        "AccountReference":  account_reference[:_MAX_ACCOUNT_REF],
        "TransactionDesc":   transaction_desc[:_MAX_TXN_DESC],
    }

    logger.info("📲 STK push | phone=%s amount=%d ref=%s", phone, int(amount), account_reference[:_MAX_ACCOUNT_REF])
    return _post_with_retry(
        f"{BASE_URL}/mpesa/stkpush/v1/processrequest",
        payload,
        label="STK push",
    )


def b2c_payment(
    phone_number: str,
    amount: int,
    account_reference: str,   # kept for API compatibility; not sent to Daraja (B2C has no such field)
    remarks: str,
    occasion: str = "Lotto payout",
) -> dict:
    """
    Initiate an M-Pesa B2C payment to a winner.

    Args:
        phone_number:      Recipient phone in any format
        amount:            Amount in KES (integer, must be >= 10 on production)
        account_reference: Not used by Daraja B2C — retained for caller compatibility
        remarks:           Internal note (truncated to 100 chars)
        occasion:          Label shown to recipient (truncated to 100 chars)

    Returns:
        Safaricom JSON response dict, e.g.:
        {
          "ConversationID":            "...",
          "OriginatorConversationID":  "...",
          "ResponseCode":              "0",
          "ResponseDescription":       "Accept the service request successfully."
        }

    Raises:
        ValueError         — if required B2C env vars are not set
        requests.HTTPError — on non-2xx HTTP response after retries
    """
    missing = [
        name for name, val in [
            ("MPESA_B2C_INITIATOR_NAME",       B2C_INITIATOR_NAME),
            ("MPESA_B2C_SECURITY_CREDENTIAL",  B2C_SECURITY_CREDENTIAL),
            ("MPESA_B2C_RESULT_URL",           B2C_RESULT_URL),
            ("MPESA_B2C_QUEUE_TIMEOUT_URL",    B2C_QUEUE_TIMEOUT_URL),
        ] if not val
    ]
    if missing:
        raise ValueError(
            f"B2C payment configuration incomplete. Missing env vars: {', '.join(missing)}"
        )

    phone = _normalize_phone(phone_number)

    payload = {
        "InitiatorName":      B2C_INITIATOR_NAME,
        "SecurityCredential": B2C_SECURITY_CREDENTIAL,
        "CommandID":          "BusinessPayment",
        "Amount":             int(amount),
        "PartyA":             B2C_SHORTCODE,
        "PartyB":             phone,
        "Remarks":            remarks[:_MAX_REMARKS],
        "QueueTimeOutURL":    B2C_QUEUE_TIMEOUT_URL,
        "ResultURL":          B2C_RESULT_URL,
        "Occasion":           occasion[:_MAX_OCCASION],
    }

    logger.info("💸 B2C payment | phone=%s amount=%d", phone, int(amount))
    return _post_with_retry(
        f"{BASE_URL}/mpesa/b2c/v3/paymentrequest",   # v3 is current; v1 still works but is deprecated
        payload,
        label="B2C payment",
    )