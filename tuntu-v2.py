import os
import json
import random
import sqlite3
import string
import logging
from datetime import datetime, timedelta, time
from functools import wraps
from flask import Flask, request, jsonify
from twilio.rest import Client
from twilio.twiml.messaging_response import MessagingResponse
from twilio.request_validator import RequestValidator
from dotenv import load_dotenv
from tuntu_mpesa import stk_push, b2c_payment

load_dotenv()

# ── Logging ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s [%(name)s] %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ── Twilio Config ───────────────────────────────────────
account_sid     = os.getenv('TWILIO_ACCOUNT_SID', '')
auth_token      = os.getenv('TWILIO_AUTH_TOKEN', '')
TWILIO_PHONE    = os.getenv('TWILIO_WHATSAPP_PHONE', '')
CONTENT_SID     = os.getenv('TWILIO_CONTENT_SID', '')
TWILIO_VALIDATE = os.getenv('TWILIO_VALIDATE_WEBHOOK', 'false').lower() == 'true'

# ── JACKPOT AMOUNT ──────────────────────────────────────
# Set the jackpot payout amount here (KES). Adjust as needed or load from env.
JACKPOT_AMOUNT = int(os.getenv('JACKPOT_AMOUNT', 100000))

def _check_required_env() -> None:
    missing = [k for k in (
        "TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN",
        "TWILIO_WHATSAPP_PHONE", "TWILIO_CONTENT_SID",
    ) if not os.getenv(k)]
    if missing:
        logger.warning("⚠️  Missing Twilio env vars: %s", ", ".join(missing))

_check_required_env()

client    = Client(account_sid, auth_token)
validator = RequestValidator(auth_token)

# ── Stores ──────────────────────────────────────────────
PROCESSED_SIDS = set()

# In-memory pending payments cache for active payment sessions.
PENDING_PAYMENTS: dict[str, dict] = {}

# SQLite database for tickets
DB_PATH = os.getenv('TUNTU_DB_PATH', os.path.join(os.path.dirname(__file__), 'tickets.db'))

# ── Database Helpers ─────────────────────────────────────
def get_db_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db_connection()
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id TEXT UNIQUE NOT NULL,
                numbers TEXT NOT NULL,
                draw TEXT NOT NULL,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL,
                sender TEXT,
                wa_id TEXT,
                profile TEXT,
                checkout_id TEXT,
                total_stake INTEGER,
                updated_at TEXT,
                payment_confirmed_at TEXT,
                expires_at TEXT,
                matched_count INTEGER DEFAULT 0,
                prize_name TEXT,
                prize_amount INTEGER,
                winning_numbers TEXT
            )
            """
        )
        conn.commit()
        ensure_ticket_columns(conn)
        ensure_draws_table(conn)
        ensure_payouts_table(conn)
    finally:
        conn.close()


def ensure_ticket_columns(conn):
    existing = {row['name'] for row in conn.execute("PRAGMA table_info(tickets);").fetchall()}
    migration_columns = {
        'expires_at': 'TEXT',
        'matched_count': 'INTEGER DEFAULT 0',
        'prize_name': 'TEXT',
        'prize_amount': 'INTEGER',
        'winning_numbers': 'TEXT',
    }
    for name, ddl in migration_columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE tickets ADD COLUMN {name} {ddl}")


def ensure_draws_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS draws (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            draw_id TEXT NOT NULL,
            draw_date TEXT NOT NULL,
            winning_numbers TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(draw_id, draw_date)
        )
        """
    )


def ensure_payouts_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS payouts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticket_id TEXT,
            phone TEXT NOT NULL,
            amount INTEGER NOT NULL,
            status TEXT NOT NULL,
            remarks TEXT,
            occasion TEXT,
            request_response TEXT,
            callback_response TEXT,
            transaction_receipt TEXT,
            conversation_id TEXT,
            originator_conversation_id TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            completed_at TEXT
        )
        """
    )


def row_to_ticket(row):
    if not row:
        return None
    ticket = dict(row)
    ticket['numbers'] = json.loads(ticket['numbers']) if ticket.get('numbers') else []
    raw_winning = ticket.get('winning_numbers')
    ticket['winning_numbers'] = json.loads(raw_winning) if raw_winning else []
    return ticket


def row_to_draw(row):
    if not row:
        return None
    draw = dict(row)
    draw['winning_numbers'] = json.loads(draw['winning_numbers']) if draw.get('winning_numbers') else []
    return draw


def row_to_payout(row):
    if not row:
        return None
    payout = dict(row)
    return payout


def save_payout_request(ticket_id, phone, amount, remarks, occasion, status='queued', request_response=None, conversation_id=None, originator_conversation_id=None):
    now = datetime.now().isoformat()
    conn = get_db_connection()
    try:
        cur = conn.execute(
            """
            INSERT INTO payouts (
                ticket_id, phone, amount, status, remarks, occasion,
                request_response, callback_response, transaction_receipt,
                conversation_id, originator_conversation_id,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticket_id,
                phone,
                amount,
                status,
                remarks,
                occasion,
                json.dumps(request_response) if request_response is not None else None,
                None,
                None,
                conversation_id,
                originator_conversation_id,
                now,
                now,
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def update_payout_by_conversation(conversation_id=None, originator_conversation_id=None, status=None, transaction_receipt=None, callback_response=None):
    if not conversation_id and not originator_conversation_id:
        return None
    now = datetime.now().isoformat()
    conn = get_db_connection()
    try:
        update_fields = []
        params = []
        if status is not None:
            update_fields.append("status = ?")
            params.append(status)
        if transaction_receipt is not None:
            update_fields.append("transaction_receipt = ?")
            params.append(transaction_receipt)
        if callback_response is not None:
            update_fields.append("callback_response = ?")
            params.append(json.dumps(callback_response))
        if status == 'success':
            update_fields.append("completed_at = ?")
            params.append(now)
        update_fields.append("updated_at = ?")
        params.append(now)
        where_clause = []
        if conversation_id is not None:
            where_clause.append("conversation_id = ?")
            params.append(conversation_id)
        if originator_conversation_id is not None:
            where_clause.append("originator_conversation_id = ?")
            params.append(originator_conversation_id)

        sql = f"UPDATE payouts SET {', '.join(update_fields)} WHERE {' OR '.join(where_clause)}"
        conn.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def update_payout_request(payout_id, **fields):
    if not payout_id or not fields:
        return None
    now = datetime.now().isoformat()
    columns = []
    params = []
    for key, value in fields.items():
        if key in ('request_response', 'callback_response'):
            columns.append(f"{key} = ?")
            params.append(json.dumps(value) if value is not None else None)
        else:
            columns.append(f"{key} = ?")
            params.append(value)
    columns.append("updated_at = ?")
    params.append(now)
    params.append(payout_id)
    conn = get_db_connection()
    try:
        conn.execute(
            f"UPDATE payouts SET {', '.join(columns)} WHERE id = ?",
            params,
        )
        conn.commit()
    finally:
        conn.close()


def get_payouts_by_ticket(ticket_id):
    conn = get_db_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM payouts WHERE ticket_id = ? ORDER BY created_at DESC",
            (ticket_id,),
        ).fetchall()
        return [row_to_payout(row) for row in rows]
    finally:
        conn.close()


def get_payouts_by_phone(phone):
    normalized_phone = _normalize_phone_for_b2c(phone)
    conn = get_db_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM payouts WHERE phone = ? ORDER BY created_at DESC",
            (normalized_phone,),
        ).fetchall()
        return [row_to_payout(row) for row in rows]
    finally:
        conn.close()


def save_draw_result(draw_id, winning_numbers, draw_date=None):
    draw_date = draw_date or datetime.now().date().isoformat()
    now = datetime.now().isoformat()
    conn = get_db_connection()
    try:
        conn.execute(
            """
            INSERT INTO draws (draw_id, draw_date, winning_numbers, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(draw_id, draw_date) DO UPDATE SET
                winning_numbers = excluded.winning_numbers,
                created_at = excluded.created_at
            """,
            (
                draw_id,
                draw_date,
                json.dumps(winning_numbers),
                now,
            ),
        )
        conn.commit()
        return draw_date
    finally:
        conn.close()


def get_draw_result(draw_id, draw_date=None):
    conn = get_db_connection()
    try:
        if draw_date:
            row = conn.execute(
                "SELECT * FROM draws WHERE draw_id = ? AND draw_date = ?",
                (draw_id, draw_date),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM draws WHERE draw_id = ? ORDER BY draw_date DESC LIMIT 1",
                (draw_id,),
            ).fetchone()
        return row_to_draw(row)
    finally:
        conn.close()


def get_latest_draw_result(draw_id):
    return get_draw_result(draw_id)


def get_draw_expiry(draw_id, created_at):
    if draw_id == 'weekly':
        days_until_wednesday = (2 - created_at.weekday()) % 7
        expiry_date = (created_at + timedelta(days=days_until_wednesday)).date()
        expiry_time = time(13, 0)
        expiry_datetime = datetime.combine(expiry_date, expiry_time)
        if expiry_datetime <= created_at:
            expiry_datetime += timedelta(days=7)
        return expiry_datetime

    next_midnight = datetime.combine((created_at + timedelta(days=1)).date(), time(0, 0))
    return next_midnight


def is_ticket_expired(ticket):
    expires_at = ticket.get('expires_at')
    if not expires_at:
        return False
    try:
        return datetime.fromisoformat(expires_at) <= datetime.now()
    except ValueError:
        return False


def expire_ticket(ticket_id):
    conn = get_db_connection()
    try:
        now = datetime.now().isoformat()
        conn.execute(
            "UPDATE tickets SET status = ?, updated_at = ? WHERE ticket_id = ? AND status != ?",
            ('expired', now, ticket_id, 'expired'),
        )
        conn.commit()
    finally:
        conn.close()


def expire_old_tickets():
    conn = get_db_connection()
    try:
        now = datetime.now().isoformat()
        conn.execute(
            "UPDATE tickets SET status = ?, updated_at = ? WHERE expires_at IS NOT NULL AND expires_at <= ? AND status IN ('active', 'pending')",
            ('expired', now, now),
        )
        conn.commit()
    finally:
        conn.close()


def save_tickets_db(tickets, sender, wa_id, profile, checkout_id, total_stake):
    conn = get_db_connection()
    try:
        now = datetime.now().isoformat()
        for ticket in tickets:
            conn.execute(
                """
                INSERT INTO tickets (
                    ticket_id, numbers, draw, created_at, status,
                    sender, wa_id, profile, checkout_id, total_stake,
                    updated_at, expires_at, matched_count, prize_name, prize_amount, winning_numbers
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ticket['ticket_id'],
                    json.dumps(ticket['numbers']),
                    ticket['draw'],
                    ticket['created_at'],
                    ticket['status'],
                    sender,
                    wa_id,
                    profile,
                    checkout_id,
                    total_stake,
                    now,
                    ticket.get('expires_at'),
                    0,
                    None,
                    None,
                    None,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def update_tickets_status_by_checkout(checkout_id, status):
    conn = get_db_connection()
    try:
        now = datetime.now().isoformat()
        confirmed_at = now if status == 'active' else None
        conn.execute(
            """
            UPDATE tickets
            SET status = ?, updated_at = ?, payment_confirmed_at = ?
            WHERE checkout_id = ?
            """,
            (status, now, confirmed_at, checkout_id),
        )
        conn.commit()
    finally:
        conn.close()


def update_ticket_prize(ticket_id, matched_count, prize_name, prize_amount, winning_numbers):
    conn = get_db_connection()
    try:
        now = datetime.now().isoformat()
        conn.execute(
            """
            UPDATE tickets
            SET matched_count = ?, prize_name = ?, prize_amount = ?, winning_numbers = ?, updated_at = ?
            WHERE ticket_id = ?
            """,
            (
                matched_count,
                prize_name,
                prize_amount,
                json.dumps(winning_numbers) if winning_numbers else None,
                now,
                ticket_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_ticket_db(ticket_id):
    expire_old_tickets()
    conn = get_db_connection()
    try:
        row = conn.execute(
            "SELECT * FROM tickets WHERE ticket_id = ?",
            (ticket_id,),
        ).fetchone()
        ticket = row_to_ticket(row)
        if ticket and is_ticket_expired(ticket) and ticket.get('status') != 'expired':
            expire_ticket(ticket_id)
            ticket['status'] = 'expired'
        return ticket
    finally:
        conn.close()


def get_active_ticket_count_db():
    expire_old_tickets()
    conn = get_db_connection()
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS count FROM tickets WHERE status = 'active'"
        ).fetchone()
        return row['count'] if row else 0
    finally:
        conn.close()


def load_pending_payments():
    expire_old_tickets()
    conn = get_db_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM tickets WHERE status = 'pending' ORDER BY id"
        ).fetchall()
        by_checkout = {}
        for row in rows:
            ticket = row_to_ticket(row)
            if is_ticket_expired(ticket):
                expire_ticket(ticket['ticket_id'])
                continue
            checkout_id = row['checkout_id']
            if not checkout_id:
                continue
            entry = by_checkout.setdefault(checkout_id, {
                'sender': row['sender'],
                'wa_id': row['wa_id'],
                'profile': row['profile'],
                'tickets': [],
                'total_stake': row['total_stake'],
                'num_tickets': 0,
                'draw_id': ticket['draw'],
            })
            entry['tickets'].append(ticket)
            entry['num_tickets'] += 1
        return by_checkout
    finally:
        conn.close()


def load_pending_payment_by_checkout(checkout_id):
    expire_old_tickets()
    conn = get_db_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM tickets WHERE checkout_id = ? AND status = 'pending' ORDER BY id",
            (checkout_id,),
        ).fetchall()
        if not rows:
            return None
        tickets = []
        for row in rows:
            ticket = row_to_ticket(row)
            if is_ticket_expired(ticket):
                expire_ticket(ticket['ticket_id'])
                continue
            tickets.append(ticket)
        if not tickets:
            return None
        row = rows[0]
        draw_id = tickets[0]['draw']
        return {
            'sender': row['sender'],
            'wa_id': row['wa_id'],
            'profile': row['profile'],
            'tickets': tickets,
            'total_stake': row['total_stake'],
            'num_tickets': len(tickets),
            'cfg': DRAW_CONFIG.get(draw_id, {'label': draw_id}),
        }
    finally:
        conn.close()


# ── FIX 1: prize_for_matches now returns a concrete amount for jackpot ──────
def prize_for_matches(matched_count):
    """
    Returns (prize_name, prize_amount).
    Jackpot (6 matches) uses JACKPOT_AMOUNT so B2C payout is never zero.
    """
    if matched_count == 6:
        return 'Jackpot', JACKPOT_AMOUNT
    if matched_count == 5:
        return 'Ksh 500', 500
    if matched_count == 4:
        return 'Ksh 400', 400
    return 'none', 0


def evaluate_ticket_against_winning_numbers(ticket, winning_numbers):
    if not isinstance(ticket.get('numbers'), list):
        return 0, 'none', 0
    matches = len(set(ticket['numbers']) & set(winning_numbers))
    prize_name, prize_amount = prize_for_matches(matches)
    return matches, prize_name, prize_amount


def get_active_tickets_by_draw(draw_id):
    expire_old_tickets()
    conn = get_db_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM tickets WHERE draw = ? AND status = 'active'",
            (draw_id,),
        ).fetchall()
        return [row_to_ticket(row) for row in rows]
    finally:
        conn.close()


def validate_winning_numbers(raw_numbers, draw_id):
    cfg = DRAW_CONFIG.get(draw_id)
    if not cfg or not isinstance(raw_numbers, list):
        return None
    try:
        normalized = [int(n) for n in raw_numbers]
    except (TypeError, ValueError):
        return None
    if len(normalized) != cfg['count'] or len(set(normalized)) != cfg['count']:
        return None
    if not all(cfg['min'] <= n <= cfg['max'] for n in normalized):
        return None
    return sorted(normalized)


def generate_winning_numbers(draw_id):
    cfg = DRAW_CONFIG.get(draw_id)
    if not cfg:
        return []
    return sorted(random.sample(range(cfg['min'], cfg['max'] + 1), cfg['count']))


def evaluate_draw_results(draw_id, winning_numbers):
    winners = []
    tickets = get_active_tickets_by_draw(draw_id)
    for ticket in tickets:
        matched_count, prize_name, prize_amount = evaluate_ticket_against_winning_numbers(ticket, winning_numbers)
        update_ticket_prize(ticket['ticket_id'], matched_count, prize_name, prize_amount, winning_numbers)
        if prize_name != 'none':
            winners.append({
                'ticket_id': ticket['ticket_id'],
                'draw': ticket['draw'],
                'matched_count': matched_count,
                'prize_name': prize_name,
                'prize_amount': prize_amount,
                'wa_id': ticket.get('wa_id'),
                'sender': ticket.get('sender'),
            })
    return winners


# ── FIX 2: normalize phone robustly; validate before use ────────────────────
def _normalize_phone_for_b2c(raw_phone: str) -> str:
    """
    Always returns a 12-digit Safaricom number string (254XXXXXXXXX).
    Handles: whatsapp:+254..., +254..., 254..., 07...
    """
    phone = str(raw_phone or '').strip()
    # Strip whatsapp: prefix
    if phone.lower().startswith('whatsapp:'):
        phone = phone[len('whatsapp:'):]
    # Strip leading +
    phone = phone.lstrip('+').strip()
    # Convert local 07/01 format
    if phone.startswith('0') and len(phone) == 10:
        phone = '254' + phone[1:]
    # Ensure country code prefix
    if not phone.startswith('254'):
        phone = '254' + phone
    return phone


def _is_valid_safaricom_phone(phone: str) -> bool:
    """254XXXXXXXXX — exactly 12 digits."""
    return phone.isdigit() and len(phone) == 12 and phone.startswith('254')


# ── FIX 3: account_reference capped at 12 chars (Daraja limit) ──────────────
_MAX_ACCT_REF_LEN = 12


def _safe_account_reference(ticket_id: str) -> str:
    """Daraja rejects account_reference longer than 12 characters."""
    return ticket_id[:_MAX_ACCT_REF_LEN]


def _send_b2c_payout(entries, draw_label):
    """
    Initiates a single B2C payment for all winning entries belonging to the
    same player (grouped by notify_winners before this is called).

    Fixes applied:
      - Jackpot prize_amount is now a concrete integer (JACKPOT_AMOUNT), not None.
      - Phone validation uses _is_valid_safaricom_phone after normalisation.
      - account_reference is capped at 12 chars.
      - Logs include normalized phone and total_amount for easier debugging.
    """
    total_amount = sum((entry.get('prize_amount') or 0) for entry in entries)
    if total_amount <= 0:
        logger.warning(f"⚠️  B2C skipped: total_amount={total_amount} for entries={[e.get('ticket_id') for e in entries]}")
        return None

    # Prefer sender (whatsapp:+254...) then fall back to wa_id
    raw_phone = entries[0].get('sender') or entries[0].get('wa_id') or ''
    phone = _normalize_phone_for_b2c(raw_phone)

    if not _is_valid_safaricom_phone(phone):
        logger.warning(f"⚠️  B2C skipped: invalid phone '{raw_phone}' → '{phone}'")
        return None

    ticket_id = entries[0].get('ticket_id', 'UNKNOWN')
    account_reference = _safe_account_reference(ticket_id)
    remarks = f"Tuntu {draw_label} win"[:100]   # Daraja remarks max 100 chars
    occasion = "Lottery Win"

    payout_id = save_payout_request(
        ticket_id=ticket_id,
        phone=phone,
        amount=total_amount,
        remarks=remarks,
        occasion=occasion,
        status='queued',
    )

    logger.info(
        f"💸 Initiating B2C | phone={phone} amount={total_amount} "
        f"ticket={ticket_id} acct_ref={account_reference}"
    )

    try:
        response = b2c_payment(
            phone_number=phone,
            amount=total_amount,
            account_reference=account_reference,
            remarks=remarks,
            occasion=occasion,
        )
        conversation_id = (
            response.get('ConversationID')
            or response.get('conversationID')
            or response.get('ConversationId')
        )
        originator = (
            response.get('OriginatorConversationID')
            or response.get('originatorConversationID')
            or response.get('OriginatorConversationId')
        )
        update_payout_request(
            payout_id,
            status='submitted',
            request_response=response,
            conversation_id=conversation_id,
            originator_conversation_id=originator,
        )
        logger.info(f"✅ B2C payout submitted | phone={phone} conversation_id={conversation_id} response={response}")
        return response
    except Exception as exc:
        update_payout_request(
            payout_id,
            status='failed',
            request_response={'error': str(exc)},
        )
        logger.error(f"❌ B2C payout failed | phone={phone} amount={total_amount} error={exc}")
        return {"error": str(exc)}


def notify_winners(winners, draw_label, winning_numbers):
    if not winners:
        return

    recipients: dict[str, list[dict]] = {}
    for win in winners:
        to = win.get('sender') or win.get('wa_id')
        if not to:
            continue
        if not to.startswith('whatsapp:'):
            to = f'whatsapp:{to}'
        recipients.setdefault(to, []).append(win)

    for to, entries in recipients.items():
        payment_response = _send_b2c_payout(entries, draw_label)
        if payment_response is None:
            payment_line = "\n\n⚠️ Prize payment could not be queued automatically. We will follow up shortly."
        elif payment_response.get('error'):
            payment_line = "\n\n⚠️ There was a problem queuing your prize payment. Our team will follow up shortly."
        else:
            payment_line = "\n\n💸 Your prize payment has been queued via M-Pesa. You will receive it shortly."

        lines = []
        for entry in entries:
            amount_text = f"KES {entry['prize_amount']}" if entry.get('prize_amount') else 'pending'
            lines.append(
                f"🎫 {entry['ticket_id']}: {entry['matched_count']} matched → {entry['prize_name']} ({amount_text})"
            )

        message = (
            f"🎉 Congratulations! Your ticket{'s' if len(entries) > 1 else ''} won in {draw_label}.\n\n"
            f"Winning numbers: {', '.join(map(str, winning_numbers))}\n\n"
            f"{chr(10).join(lines)}"
            f"{payment_line}"
        )
        send_whatsapp(to, message)


def build_winning_numbers_for_ticket(ticket, prize_type):
    cfg = DRAW_CONFIG.get(ticket['draw'])
    if not cfg:
        return ticket['numbers']

    base = sorted(ticket['numbers'] or [])
    if prize_type in ('jackpot', 'JACKPOT', 'Jackpot'):
        return base

    if prize_type in ('Ksh 500', '500', '500 bob', '500sh'):  # 5 matches
        target_count = cfg['count'] - 1
    elif prize_type in ('Ksh 400', '400', '400 bob', '400sh'):  # 4 matches
        target_count = cfg['count'] - 2
    else:
        target_count = len(base)

    target_count = max(0, min(target_count, len(base)))

    selected = base[:target_count]
    non_ticket_numbers = [n for n in range(cfg['min'], cfg['max'] + 1) if n not in base]
    extras = non_ticket_numbers[: cfg['count'] - target_count]
    return sorted(selected + extras)


def force_ticket_win(ticket_id, prize_type=None, winning_numbers=None):
    ticket = get_ticket_db(ticket_id)
    if not ticket:
        return None, 'not_found'

    if not winning_numbers and prize_type:
        winning_numbers = build_winning_numbers_for_ticket(ticket, prize_type)

    if winning_numbers is None:
        return None, 'missing_numbers'

    validated = validate_winning_numbers(winning_numbers, ticket['draw'])
    if not validated:
        return None, 'invalid_numbers'

    matched_count, prize_name, prize_amount = evaluate_ticket_against_winning_numbers(ticket, validated)
    if matched_count < 4:
        return None, 'not_a_winner'

    update_ticket_prize(ticket_id, matched_count, prize_name, prize_amount, validated)
    updated_ticket = get_ticket_db(ticket_id)
    return updated_ticket, None

init_db()
PENDING_PAYMENTS.update(load_pending_payments())

# ── Business Rules ──────────────────────────────────────
DRAW_CONFIG = {
    'weekly':          {'label': 'Weekly Jackpot',       'price': 50, 'min': 1, 'max': 49, 'count': 6},
    'mega':            {'label': 'Mega Jackpot',         'price': 50, 'min': 1, 'max': 60, 'count': 6},
    'shinda-nduthi':   {'label': 'Shinda Nduthi Weekly', 'price': 50, 'min': 1, 'max': 49, 'count': 6},
    'shinda-probox':   {'label': 'Shinda Pro-Box',       'price': 50, 'min': 1, 'max': 49, 'count': 6},
}

# ── Helpers ─────────────────────────────────────────────
def parse_safe_json(raw):
    if not raw:              return None
    if isinstance(raw, dict): return raw
    try:   return json.loads(raw)
    except: return None


def extract_payload_data(flow_raw, interactive_raw):
    data = {}
    flow        = parse_safe_json(flow_raw)
    interactive = parse_safe_json(interactive_raw)

    if flow and isinstance(flow, dict):
        data.update(flow)
    if interactive and isinstance(interactive, dict):
        nested = interactive.get('flowResponse', {})
        data.update(nested if isinstance(nested, dict) else interactive)
    return data


def generate_ticket_id() -> str:
    suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return f"TUNTU-{suffix}"


def generate_lottery_numbers(min_n: int, max_n: int, count: int) -> list[int]:
    return sorted(random.sample(range(min_n, max_n + 1), count))


def generate_tickets(num_tickets: int, mode: str, manual_nums, draw_id: str) -> list[dict]:
    cfg     = DRAW_CONFIG.get(draw_id, DRAW_CONFIG['weekly'])
    tickets = []
    for _ in range(num_tickets):
        tid  = generate_ticket_id()
        nums = None

        if mode == 'manual' and manual_nums:
            try:
                parsed = [int(x.strip()) for x in str(manual_nums).split(',') if x.strip()]
                if len(parsed) == cfg['count'] and all(cfg['min'] <= n <= cfg['max'] for n in parsed):
                    nums = sorted(parsed)
            except Exception:
                pass

        if not nums:
            nums = generate_lottery_numbers(cfg['min'], cfg['max'], cfg['count'])

        created_at_dt = datetime.now()
        created_at = created_at_dt.isoformat()
        expires_at = get_draw_expiry(draw_id, created_at_dt).isoformat()
        ticket = {
            "ticket_id":  tid,
            "numbers":    nums,
            "draw":       draw_id,
            "created_at": created_at,
            "expires_at": expires_at,
            "status":     "pending",
        }
        tickets.append(ticket)
    return tickets


def validate_twilio(f):
    @wraps(f)
    def wrap(*a, **kw):
        if not TWILIO_VALIDATE:
            return f(*a, **kw)
        sig = request.headers.get('X-Twilio-Signature', '')
        url = request.url.replace('http://', 'https://')
        if not validator.validate(url, request.form, sig):
            logger.warning("❌ Signature validation failed")
            return jsonify({"error": "Unauthorized"}), 403
        return f(*a, **kw)
    return wrap


def send_whatsapp(to: str, body: str):
    client.messages.create(from_=TWILIO_PHONE, to=to, body=body)
    logger.info(f"📤 WhatsApp → {to}")


# ── Flow Handler ─────────────────────────────────────────
def handle_flow(payload_raw, interactive_raw, sender, profile, wa_id, sid):
    data = extract_payload_data(payload_raw, interactive_raw)
    logger.info(f"📦 Extracted keys: {list(data.keys())} | Values: {data}")

    draw_id = str(data.get('draw', 'weekly')).lower().strip()
    stake   = data.get('stake_amount', '50')
    mode    = str(data.get('selection_mode', 'quick_pick')).lower().replace(' ', '_')
    manual  = data.get('manual_numbers')

    if draw_id not in DRAW_CONFIG:
        logger.warning(f"⚠️  Unknown draw '{draw_id}'. Defaulting to weekly.")
        draw_id = 'weekly'

    cfg          = DRAW_CONFIG[draw_id]
    num_tickets  = 1
    total_stake  = cfg['price'] * num_tickets

    try:
        tickets = generate_tickets(num_tickets, mode, manual, draw_id)
    except Exception as e:
        logger.error(f"❌ Ticket gen failed: {e}")
        send_whatsapp(sender, "❌ Could not generate tickets. Type *start* to retry.")
        return

    try:
        result = stk_push(
            phone_number      = wa_id,
            amount            = total_stake,
            account_reference = tickets[0]['ticket_id'],
            transaction_desc  = "Lotto ticket",
        )
        checkout_id = result.get("CheckoutRequestID")
        if not checkout_id:
            raise ValueError(f"Missing CheckoutRequestID: {result}")
        logger.info(f"✅ STK push initiated | CheckoutRequestID: {checkout_id}")
    except Exception as e:
        logger.error(f"❌ M-Pesa STK push failed: {e}")
        send_whatsapp(sender, "❌ Could not initiate M-Pesa payment. Please type *start* to try again.")
        return

    PENDING_PAYMENTS[checkout_id] = {
        "sender":      sender,
        "wa_id":       wa_id,
        "profile":     profile,
        "tickets":     tickets,
        "cfg":         cfg,
        "total_stake": total_stake,
        "num_tickets": num_tickets,
    }

    save_tickets_db(tickets, sender, wa_id, profile, checkout_id, total_stake)

    send_whatsapp(
        sender,
        f"📲 *M-Pesa payment request sent!*\n\n"
        f"Please check your phone and enter your M-Pesa PIN to complete the payment of "
        f"*KES {total_stake}* for your {cfg['label']} ticket.\n\n"
        f"Your ticket will be confirmed automatically once payment is received. 🎟️"
    )


# ── M-Pesa Callback ──────────────────────────────────────
@app.route('/webhook/mpesa', methods=['POST'])
def mpesa_callback():
    body = request.get_json(silent=True)
    if not body:
        raw = request.get_data(as_text=True)
        logger.info(f"📩 M-Pesa callback raw payload: {raw}")
        try:
            body = json.loads(raw)
        except Exception as e:
            logger.error(f"❌ Could not parse M-Pesa payload: {e}")
            return jsonify({"ResultCode": 0, "ResultDesc": "Accepted"}), 200

    logger.info(f"📩 M-Pesa callback parsed body keys: {list(body.keys())}")

    stk_callback = None
    checkout_id = ''
    result_code = None
    result_desc = ''

    if isinstance(body, dict):
        stk_callback = body.get('Body', {}).get('stkCallback') or body.get('body', {}).get('stkCallback')
        if not stk_callback:
            stk_callback = body.get('stkCallback') or body.get('Body')
        if isinstance(stk_callback, dict):
            checkout_id = stk_callback.get('CheckoutRequestID', '')
            result_code = stk_callback.get('ResultCode')
            result_desc = stk_callback.get('ResultDesc', '')

    if not stk_callback or not checkout_id:
        logger.error(f"❌ Malformed M-Pesa callback: missing stkCallback or CheckoutRequestID | body={json.dumps(body)}")
        return jsonify({"ResultCode": 0, "ResultDesc": "Accepted"}), 200

    pending = PENDING_PAYMENTS.pop(checkout_id, None)
    if not pending:
        pending = load_pending_payment_by_checkout(checkout_id)
        if pending:
            logger.info(f"🔁 Rehydrated pending payment from DB for CheckoutRequestID: {checkout_id}")

    if not pending:
        logger.warning(f"⚠️  No pending payment for CheckoutRequestID: {checkout_id}")
        return jsonify({"ResultCode": 0, "ResultDesc": "Accepted"}), 200

    sender      = pending['sender']
    profile     = pending['profile']
    wa_id       = pending['wa_id']
    tickets     = pending['tickets']
    cfg         = pending['cfg']
    total_stake = pending['total_stake']
    num_tickets = pending['num_tickets']

    if result_code != 0:
        logger.warning(f"⚠️  Payment failed for {wa_id}: {result_desc}")
        update_tickets_status_by_checkout(checkout_id, 'cancelled')
        send_whatsapp(
            sender,
            f"❌ Payment was not completed ({result_desc}).\n\n"
            f"Type *start* to try again. Your ticket has not been activated."
        )
        return jsonify({"ResultCode": 0, "ResultDesc": "Accepted"}), 200

    update_tickets_status_by_checkout(checkout_id, 'active')
    lines = "\n".join([
        f"🎫 *{t['ticket_id']}*: {', '.join(map(str, t['numbers']))}"
        for t in tickets
    ])
    msg = (
        f"✅ *Tuntu Lotto — Ticket Confirmed!*\n\n"
        f"👤 {profile or 'Player'} (ID: {wa_id})\n"
        f"🎯 Draw: *{cfg['label']}*\n"
        f"💰 Paid: KES {total_stake}\n"
        f"🎟 Tickets: {num_tickets}\n\n"
        f"*Your Numbers:*\n{lines}\n\n"
        f"🔍 Save your ticket IDs for result verification.\n"
        f"🍀 Good luck!"
    )
    send_whatsapp(sender, msg)
    logger.info(f"✅ Ticket(s) activated and confirmation sent to {wa_id}")

    return jsonify({"ResultCode": 0, "ResultDesc": "Accepted"}), 200


def _extract_mpesa_b2c_callback_data(body):
    result = body.get('Result') or body.get('result') or body
    conversation_id = result.get('ConversationID') or result.get('conversationID') or result.get('ConversationId')
    originator_id = result.get('OriginatorConversationID') or result.get('originatorConversationID') or result.get('OriginatorConversationId')
    transaction_receipt = None
    result_code = None

    parameters = result.get('ResultParameters', {}).get('ResultParameter') if isinstance(result.get('ResultParameters'), dict) else result.get('ResultParameters')
    if isinstance(parameters, list):
        for param in parameters:
            if isinstance(param, dict) and param.get('Key') == 'TransactionReceipt':
                transaction_receipt = param.get('Value')
            if isinstance(param, dict) and param.get('Key') == 'ResultCode':
                result_code = param.get('Value')

    if result_code is None:
        result_code = result.get('ResultCode')

    return {
        'conversation_id': conversation_id,
        'originator_conversation_id': originator_id,
        'transaction_receipt': transaction_receipt,
        'result_code': result_code,
    }


@app.route('/webhook/mpesa/b2c/result', methods=['POST'])
def mpesa_b2c_result():
    body = request.get_json(silent=True)
    if not body:
        raw = request.get_data(as_text=True)
        logger.info(f"📩 M-Pesa B2C result callback raw payload: {raw}")
        return jsonify({"ResultCode": 0, "ResultDesc": "Accepted"}), 200

    callback_data = _extract_mpesa_b2c_callback_data(body)
    status = 'success' if callback_data['result_code'] == 0 else 'failed'
    logger.info(f"📩 M-Pesa B2C result: conversation_id={callback_data['conversation_id']} status={status}")
    update_payout_by_conversation(
        conversation_id=callback_data['conversation_id'],
        originator_conversation_id=callback_data['originator_conversation_id'],
        status=status,
        transaction_receipt=callback_data['transaction_receipt'],
        callback_response=body,
    )
    return jsonify({"ResultCode": 0, "ResultDesc": "Accepted"}), 200


@app.route('/webhook/mpesa/b2c/timeout', methods=['POST'])
def mpesa_b2c_timeout():
    body = request.get_json(silent=True)
    if not body:
        raw = request.get_data(as_text=True)
        logger.info(f"📩 M-Pesa B2C timeout callback raw payload: {raw}")
        return jsonify({"ResultCode": 0, "ResultDesc": "Accepted"}), 200

    callback_data = _extract_mpesa_b2c_callback_data(body)
    logger.warning(f"⚠️ M-Pesa B2C timeout: conversation_id={callback_data['conversation_id']}")
    update_payout_by_conversation(
        conversation_id=callback_data['conversation_id'],
        originator_conversation_id=callback_data['originator_conversation_id'],
        status='timeout',
        callback_response=body,
    )
    return jsonify({"ResultCode": 0, "ResultDesc": "Accepted"}), 200


# ── WhatsApp Webhook ─────────────────────────────────────
@app.route('/webhook/reply', methods=['POST'])
@validate_twilio
def webhook():
    if app.debug:
        logger.info(f"🔍 RAW FORM: {dict(request.form)}")

    sender   = request.form.get('From', '')
    wa_id    = request.form.get('WaId', sender.replace('whatsapp:', ''))
    profile  = request.form.get('ProfileName')
    sid      = request.form.get('MessageSid')
    body     = request.form.get('Body', '').strip()

    flow_raw        = request.form.get('FlowData')
    interactive_raw = request.form.get('InteractiveData')

    resp = MessagingResponse()

    if sid in PROCESSED_SIDS:
        logger.info(f"⏭️  Duplicate ignored: {sid}")
        return str(resp), 200
    PROCESSED_SIDS.add(sid)

    if body.lower() == 'start':
        client.messages.create(from_=TWILIO_PHONE, content_sid=CONTENT_SID, to=sender)
        return str(resp), 200

    if flow_raw or interactive_raw:
        logger.info(f"📥 Flow submission from {wa_id}")
        handle_flow(flow_raw, interactive_raw, sender, profile, wa_id, sid)
        return str(resp), 200

    send_whatsapp(sender, "👋 Welcome to *Tuntu Lotto!*\nType *start* to play 🎟️")
    return str(resp), 200


# ── Utility Routes ───────────────────────────────────────
@app.route('/api/draw/results', methods=['POST'])
def draw_results():
    payload = request.get_json(silent=True) or {}
    draw_id = str(payload.get('draw', 'weekly')).lower().strip()
    if draw_id not in DRAW_CONFIG:
        return jsonify({"error": "Invalid draw ID"}), 400

    winning_numbers = payload.get('winning_numbers')
    auto_generated = False
    if winning_numbers is None:
        winning_numbers = generate_winning_numbers(draw_id)
        auto_generated = True
    else:
        winning_numbers = validate_winning_numbers(winning_numbers, draw_id)
        if not winning_numbers:
            return jsonify({
                "error": "Invalid winning_numbers. Provide a list of unique integers matching the draw count"
            }), 400

    draw_date = payload.get('draw_date')
    stored_draw_date = save_draw_result(draw_id, winning_numbers, draw_date)
    winners = evaluate_draw_results(draw_id, winning_numbers)
    notify_winners(winners, DRAW_CONFIG[draw_id]['label'], winning_numbers)
    return jsonify({
        "status":          "ok",
        "draw":            draw_id,
        "draw_date":       stored_draw_date,
        "winning_numbers": winning_numbers,
        "auto_generated":  auto_generated,
        "processed":       len(get_active_tickets_by_draw(draw_id)),
        "winners":         winners,
    })


@app.route('/api/admin/force-win', methods=['POST'])
def admin_force_win():
    payload = request.get_json(silent=True) or {}
    ticket_id = payload.get('ticket_id')
    prize_type = payload.get('prize_type')
    winning_numbers = payload.get('winning_numbers')

    if not ticket_id:
        return jsonify({"error": "ticket_id is required"}), 400

    updated_ticket, error = force_ticket_win(ticket_id, prize_type, winning_numbers)
    if error == 'not_found':
        return jsonify({"error": "Ticket not found"}), 404
    if error == 'missing_numbers':
        return jsonify({"error": "winning_numbers or prize_type is required"}), 400
    if error == 'invalid_numbers':
        return jsonify({"error": "Invalid winning_numbers for this draw"}), 400
    if error == 'not_a_winner':
        return jsonify({"error": "Provided numbers do not result in a winning ticket"}), 400

    winners = [{
        'ticket_id': updated_ticket['ticket_id'],
        'draw': updated_ticket['draw'],
        'matched_count': updated_ticket['matched_count'],
        'prize_name': updated_ticket['prize_name'],
        'prize_amount': updated_ticket['prize_amount'],
        'wa_id': updated_ticket.get('wa_id'),
        'sender': updated_ticket.get('sender'),
    }]
    notify_winners(winners, DRAW_CONFIG.get(updated_ticket['draw'], {}).get('label', updated_ticket['draw']), updated_ticket.get('winning_numbers', []))

    return jsonify({
        "status": "ok",
        "ticket": updated_ticket,
        "winners": winners,
    })


@app.route('/api/admin/b2c-test', methods=['POST'])
def admin_b2c_test():
    payload = request.get_json(silent=True) or {}
    phone_number = payload.get('phone_number')
    amount = payload.get('amount')
    ticket_id = payload.get('ticket_id', 'MANUALB2C')
    remarks = payload.get('remarks', 'Tuntu Lotto payout')

    if not phone_number:
        return jsonify({"error": "phone_number is required"}), 400
    if amount is None:
        return jsonify({"error": "amount is required"}), 400
    try:
        amount = int(amount)
    except (TypeError, ValueError):
        return jsonify({"error": "amount must be an integer"}), 400
    if amount <= 0:
        return jsonify({"error": "amount must be greater than zero"}), 400

    # Normalise and validate the phone before hitting Daraja
    normalized = _normalize_phone_for_b2c(phone_number)
    if not _is_valid_safaricom_phone(normalized):
        return jsonify({"error": f"Invalid phone number '{phone_number}' → '{normalized}'"}), 400

    try:
        response = b2c_payment(
            phone_number=normalized,
            amount=amount,
            account_reference=_safe_account_reference(str(ticket_id)),
            remarks=remarks[:100],
            occasion="Lottery Payout",
        )
    except Exception as exc:
        logger.error(f"❌ B2C test payment failed: {exc}")
        return jsonify({"status": "error", "message": str(exc)}), 500

    return jsonify({"status": "ok", "normalized_phone": normalized, "b2c_response": response})


@app.route('/api/admin/payouts', methods=['GET'])
def admin_payouts():
    ticket_id = request.args.get('ticket_id')
    phone = request.args.get('phone')

    if not ticket_id and not phone:
        return jsonify({
            "error": "Provide either ticket_id or phone query parameter",
        }), 400

    if ticket_id:
        payouts = get_payouts_by_ticket(ticket_id)
    else:
        payouts = get_payouts_by_phone(phone)

    return jsonify({
        "status": "ok",
        "query": {
            "ticket_id": ticket_id,
            "phone": phone,
        },
        "payouts": payouts,
    })


@app.route('/api/verify/<ticket_id>', methods=['GET'])
def verify(ticket_id):
    t = get_ticket_db(ticket_id)
    if not t:
        return jsonify({"error": "Not found"}), 404
    return jsonify({
        "status":          t.get('status', 'unknown'),
        "ticket":          t,
        "draw":            DRAW_CONFIG.get(t['draw'], {}).get('label', t['draw']),
        "matched_count":   t.get('matched_count', 0),
        "prize_name":      t.get('prize_name'),
        "prize_amount":    t.get('prize_amount'),
        "winning_numbers": t.get('winning_numbers', []),
    })


@app.route('/health')
def health():
    return jsonify({
        "status":          "ok",
        "tickets_active":  get_active_ticket_count_db(),
        "tickets_pending": len(PENDING_PAYMENTS),
        "jackpot_amount":  JACKPOT_AMOUNT,
    })


if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    logger.info(f"🚀 Tuntu Lotto on :{port}")
    app.run(host='0.0.0.0', port=port, debug=os.getenv('FLASK_DEBUG', 'false').lower() == 'true')