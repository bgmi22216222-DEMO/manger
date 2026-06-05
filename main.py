"""
XVIP Hybrid Telegram Bot + Userbot
====================================
Architecture:
  - Bot Client  : python-telegram-bot style Admin UI (via Telethon bot-mode)
  - Userbot Client : Telethon StringSession — handles source monitoring & bot-to-bot delivery
  - DB          : Supabase REST API (httpx) — stores ONLY the string session
  - Deploy      : Railway (all secrets via env vars)

Required env vars:
  API_ID, API_HASH, BOT_TOKEN, ADMIN_IDS,
  SUPABASE_URL, SUPABASE_KEY,
  TERA_SOURCE_CHANNELS, DISK_SOURCE_CHANNELS,
  TERA_CONVERTER_BOT, DISK_CONVERTER_BOT,
  TERA_DESTINATION, DISK_DESTINATION
"""

import asyncio
import logging
import os
import sys
from typing import Optional

import httpx
from telethon import TelegramClient, events, errors
from telethon.sessions import StringSession

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("xvip")


# ─────────────────────────────────────────────
# ENV CONFIG
# ─────────────────────────────────────────────
def _require(key: str) -> str:
    val = os.environ.get(key, "").strip()
    if not val:
        raise RuntimeError(f"Missing required env var: {key}")
    return val


def _parse_ids(raw: str) -> list[int]:
    """Parse comma-separated numeric IDs, silently skip blanks."""
    result = []
    for part in raw.split(","):
        part = part.strip()
        if part.lstrip("-").isdigit():
            result.append(int(part))
    return result


def _clean_username(username: str) -> str:
    """Strip '@' and lowercase — safe for send_message() calls."""
    return username.strip().lstrip("@").lower()


API_ID              = int(_require("API_ID"))
API_HASH            = _require("API_HASH")
BOT_TOKEN           = _require("BOT_TOKEN")
ADMIN_IDS           = set(_parse_ids(_require("ADMIN_IDS")))
SUPABASE_URL        = _require("SUPABASE_URL").rstrip("/")
SUPABASE_KEY        = _require("SUPABASE_KEY")

TERA_SOURCE_IDS     = _parse_ids(_require("TERA_SOURCE_CHANNELS"))
DISK_SOURCE_IDS     = _parse_ids(_require("DISK_SOURCE_CHANNELS"))

TERA_CONVERTER_BOT  = _clean_username(_require("TERA_CONVERTER_BOT"))
DISK_CONVERTER_BOT  = _clean_username(_require("DISK_CONVERTER_BOT"))
TERA_DESTINATION    = _clean_username(_require("TERA_DESTINATION"))
DISK_DESTINATION    = _clean_username(_require("DISK_DESTINATION"))

ALL_SOURCE_IDS      = set(TERA_SOURCE_IDS + DISK_SOURCE_IDS)

log.info("Tera sources  : %s", TERA_SOURCE_IDS)
log.info("Disk sources  : %s", DISK_SOURCE_IDS)
log.info("Tera converter: @%s → dest @%s", TERA_CONVERTER_BOT, TERA_DESTINATION)
log.info("Disk converter: @%s → dest @%s", DISK_CONVERTER_BOT, DISK_DESTINATION)


# ─────────────────────────────────────────────
# SUPABASE HELPERS
# ─────────────────────────────────────────────
_SB_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}
# Strip any trailing /rest/v1 from SUPABASE_URL to avoid duplication
_SB_BASE = SUPABASE_URL.rstrip("/")
if _SB_BASE.endswith("/rest/v1"):
    _SB_BASE = _SB_BASE[: -len("/rest/v1")]
_SB_TABLE = f"{_SB_BASE}/rest/v1/bot_config"
_SESSION_KEY = "telegram_string_session"


async def sb_get_session() -> Optional[str]:
    """Fetch the stored Telegram String Session from Supabase."""
    async with httpx.AsyncClient() as client:
        r = await client.get(
            _SB_TABLE,
            headers=_SB_HEADERS,
            params={"key_name": f"eq.{_SESSION_KEY}", "select": "key_value"},
        )
    if r.status_code == 200:
        rows = r.json()
        if rows:
            return rows[0]["key_value"]
    return None


async def sb_upsert_session(session_string: str) -> bool:
    """Upsert the Telegram String Session into Supabase."""
    payload = {"key_name": _SESSION_KEY, "key_value": session_string}
    headers = {**_SB_HEADERS, "Prefer": "resolution=merge-duplicates,return=minimal"}
    async with httpx.AsyncClient() as client:
        r = await client.post(_SB_TABLE, headers=headers, json=payload)
    if r.status_code in (200, 201, 204):
        log.info("Session upserted to Supabase successfully.")
        return True
    log.error("Supabase upsert failed: %s %s", r.status_code, r.text)
    return False


# ─────────────────────────────────────────────
# GLOBAL STATE
# ─────────────────────────────────────────────

# The admin bot client (always running)
bot_client: TelegramClient = TelegramClient("bot_session", API_ID, API_HASH)

# The userbot client (started only when session exists)
userbot: Optional[TelegramClient] = None
userbot_active: bool = False

# Login state machine per admin user
# States: "await_phone" | "await_otp" | "await_2fa"
login_state: dict[int, dict] = {}

# Temporary userbot during login (not yet authenticated)
login_userbot: Optional[TelegramClient] = None


# ─────────────────────────────────────────────
# USERBOT: ATTACH HANDLERS + START
# ─────────────────────────────────────────────

def attach_userbot_handlers(ub: TelegramClient) -> None:
    """
    Register all Telethon event handlers on the userbot.
    Called once after the userbot is authenticated (either from saved session or fresh login).
    """

    # ── Handler 1: Monitor source channels/groups for new posts ──────────────
    @ub.on(events.NewMessage(chats=list(ALL_SOURCE_IDS)))
    async def on_source_message(event):
        msg = event.message
        chat_id = event.chat_id

        # Phase 2.1 — Media enforcement: must have photo or video
        has_photo = bool(msg.photo)
        has_video = (
            msg.document is not None
            and msg.document.mime_type is not None
            and (
                msg.document.mime_type.startswith("video/")
                or msg.document.mime_type.startswith("image/")
            )
        )
        if not has_photo and not has_video:
            return  # Drop text-only messages

        caption = (msg.message or "").lower()

        # Phase 2.2 — Keyword-source alignment
        if chat_id in TERA_SOURCE_IDS and "tera" in caption:
            log.info("Tera match from %s — forwarding to @%s", chat_id, TERA_CONVERTER_BOT)
            await safe_send(ub, TERA_CONVERTER_BOT, file=msg.media, message=msg.message or "")

        elif chat_id in DISK_SOURCE_IDS and "disk" in caption:
            log.info("Disk match from %s — forwarding to @%s", chat_id, DISK_CONVERTER_BOT)
            await safe_send(ub, DISK_CONVERTER_BOT, file=msg.media, message=msg.message or "")

    # ── Handler 2: Intercept converter bot replies & relay to destination ─────
    @ub.on(events.NewMessage(from_users=[TERA_CONVERTER_BOT, DISK_CONVERTER_BOT]))
    async def on_converter_reply(event):
        msg = event.message
        sender = event.sender

        # Resolve sender username safely
        sender_username = ""
        if sender and getattr(sender, "username", None):
            sender_username = _clean_username(sender.username)

        if sender_username == TERA_CONVERTER_BOT:
            dest = TERA_DESTINATION
            label = "Tera"
            keyword = "tera"
        elif sender_username == DISK_CONVERTER_BOT:
            dest = DISK_DESTINATION
            label = "Disk"
            keyword = "disk"
        else:
            return

        # ── Media check: photo ya video hona chahiye ──────────────────────────
        has_photo = bool(msg.photo)
        has_video = (
            msg.document is not None
            and msg.document.mime_type is not None
            and (
                msg.document.mime_type.startswith("video/")
                or msg.document.mime_type.startswith("image/")
            )
        )
        if not has_photo and not has_video:
            log.info("%s converter reply skipped — no media.", label)
            return

        # ── Link check: keyword must appear inside a URL in the message ───────
        all_urls = []

        # 1. Text entities (MessageEntityUrl → raw text, MessageEntityTextUrl → .url attr)
        if msg.entities:
            for ent in msg.entities:
                url_attr = getattr(ent, "url", None)
                if url_attr:
                    all_urls.append(url_attr.lower())
                else:
                    raw_text = msg.message or ""
                    chunk = raw_text[ent.offset: ent.offset + ent.length]
                    if chunk.lower().startswith("http"):
                        all_urls.append(chunk.lower())

        # 2. Inline keyboard button URLs
        if msg.reply_markup:
            for row in getattr(msg.reply_markup, "rows", []):
                for btn in getattr(row, "buttons", []):
                    btn_url = getattr(btn, "url", None)
                    if btn_url:
                        all_urls.append(btn_url.lower())

        keyword_found = any(keyword in url for url in all_urls)
        if not keyword_found:
            log.info(
                "%s converter reply skipped — keyword '%s' not found in any link. URLs found: %s",
                label, keyword, all_urls,
            )
            return

        log.info("%s converter replied — keyword '%s' found in link, sending to @%s", label, keyword, dest)

        # Phase 3 — Fresh send (no forward tag) to destination
        await safe_send(
            ub,
            dest,
            file=msg.media,
            message=msg.message or "",
        )


async def start_userbot(session_string: str) -> bool:
    """
    Initialize and connect the Userbot using a saved StringSession.
    Returns True on success.
    """
    global userbot, userbot_active

    log.info("Starting Userbot with saved StringSession...")
    ub = TelegramClient(StringSession(session_string), API_ID, API_HASH)

    try:
        await ub.start()
        if not await ub.is_user_authorized():
            log.error("Userbot session is invalid/expired.")
            return False

        me = await ub.get_me()
        log.info("Userbot connected as: %s (id=%s)", me.username or me.first_name, me.id)

        attach_userbot_handlers(ub)
        userbot = ub
        userbot_active = True
        return True

    except Exception as exc:
        log.exception("Failed to start userbot: %s", exc)
        return False


# ─────────────────────────────────────────────
# SAFE SEND HELPER (FloodWait aware)
# ─────────────────────────────────────────────

async def safe_send(
    client: TelegramClient,
    target: str,
    message: str = "",
    file=None,
) -> None:
    """
    Send a message (with optional media) to `target`.
    Handles FloodWaitError gracefully.
    """
    for attempt in range(3):
        try:
            if file:
                await client.send_message(target, message, file=file)
            else:
                if message.strip():
                    await client.send_message(target, message)
            return
        except errors.FloodWaitError as e:
            log.warning("FloodWait: sleeping %ds (attempt %d/3)", e.seconds, attempt + 1)
            await asyncio.sleep(e.seconds + 2)
        except errors.UserIsBlockedError:
            log.error("Bot is blocked by %s — skipping.", target)
            return
        except Exception as exc:
            log.exception("safe_send failed for %s: %s", target, exc)
            return


# ─────────────────────────────────────────────
# ADMIN BOT HANDLERS
# ─────────────────────────────────────────────

def admin_only(handler):
    """Decorator: silently ignore messages from non-admins."""
    import functools
    @functools.wraps(handler)
    async def wrapper(event):
        if event.sender_id not in ADMIN_IDS:
            return
        await handler(event)
    return wrapper


def register_bot_handlers() -> None:
    """Attach all command/message handlers to the admin bot."""

    # ── /start ────────────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(pattern=r"^/start$"))
    @admin_only
    async def cmd_start(event):
        status = "🟢 Connected" if userbot_active else "🔴 Offline"
        await event.respond(
            f"**XVIP Hybrid Bot**\n\n"
            f"Userbot Status: {status}\n\n"
            f"Commands:\n"
            f"`/status` — check userbot status\n"
            f"`/login`  — authenticate userbot (if offline)\n"
        )

    # ── /status ───────────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(pattern=r"^/status$"))
    @admin_only
    async def cmd_status(event):
        if userbot_active and userbot:
            try:
                me = await userbot.get_me()
                name = me.username or me.first_name or str(me.id)
                await event.respond(f"🟢 **Userbot Connected**\nLogged in as: @{name}")
            except Exception:
                await event.respond("🟡 Userbot started but couldn't fetch profile.")
        else:
            await event.respond("🔴 **Userbot Offline**\nType `/login` to authenticate.")

    # ── /login — Step 1: ask for phone ───────────────────────────────────────
    @bot_client.on(events.NewMessage(pattern=r"^/login$"))
    @admin_only
    async def cmd_login(event):
        if userbot_active:
            await event.respond("✅ Userbot is already connected. Use `/status` to verify.")
            return

        login_state[event.sender_id] = {"step": "await_phone"}
        await event.respond(
            "📱 **Login Step 1/3**\n\nSend your Telegram phone number in international format:\n`+91XXXXXXXXXX`"
        )

    # ── Generic message handler — drives the login state machine ─────────────
    @bot_client.on(events.NewMessage)
    async def on_message(event):
        # Admin check inline (decorator causes Telethon registration issues)
        if event.sender_id not in ADMIN_IDS:
            return
        # Ignore commands — handled above
        if event.message.message.startswith("/"):
            return

        uid = event.sender_id
        state = login_state.get(uid)
        if not state:
            return  # Not in a login flow

        step = state.get("step")

        # ── STEP A: Receive phone number ──────────────────────────────────
        if step == "await_phone":
            phone = event.message.message.strip()
            if not phone.startswith("+"):
                await event.respond("❌ Invalid format. Use `+CountryCodeNumber` (e.g. `+919876543210`)")
                return

            state["phone"] = phone

            # Create a fresh temp client for the login flow
            global login_userbot
            login_userbot = TelegramClient(StringSession(), API_ID, API_HASH)
            await login_userbot.connect()

            try:
                result = await login_userbot.send_code_request(phone)
                state["phone_code_hash"] = result.phone_code_hash
                state["step"] = "await_otp"
                state["otp_requested_at"] = asyncio.get_event_loop().time()

                await event.respond(
                    "📟 **Login Step 2/3**\n\n"
                    "OTP aapke Telegram par bheja gaya hai.\n\n"
                    "⚠️ **OTP sirf ~60 seconds valid hai — turant bhejo!**\n\n"
                    "Code bhejo (spaces hata ke): e.g. `12345`\n"
                    "Ya space ke saath bhi chalega: `1 2 3 4 5`"
                )
            except errors.PhoneNumberInvalidError:
                await event.respond("❌ Phone number is invalid. Restart with `/login`.")
                login_state.pop(uid, None)
            except Exception as exc:
                await event.respond(f"❌ Error: `{exc}`\nRestart with `/login`.")
                login_state.pop(uid, None)

        # ── STEP B: Receive OTP ───────────────────────────────────────────
        elif step == "await_otp":
            # Strip spaces so "1 2 3 4 5" → "12345"
            otp = event.message.message.strip().replace(" ", "")
            phone = state["phone"]
            code_hash = state["phone_code_hash"]

            log.info("OTP attempt: phone=%s otp_len=%d otp_val=%s hash=%s", phone, len(otp), otp, code_hash[:8])

            try:
                await login_userbot.sign_in(phone, otp, phone_code_hash=code_hash)
                log.info("sign_in succeeded for %s", phone)
                # OTP success — no 2FA needed
                await _finalize_login(event, uid)

            except errors.SessionPasswordNeededError:
                log.info("2FA required for %s", phone)
                state["step"] = "await_2fa"
                await event.respond(
                    "🔐 **Login Step 3/3 — 2FA Required**\n\nEnter your Two-Factor Authentication cloud password:"
                )
            except errors.PhoneCodeInvalidError as exc:
                log.warning("PhoneCodeInvalid: %s", exc)
                await event.respond("❌ Wrong OTP. Send the correct 5-digit code:")
            except errors.PhoneCodeExpiredError as exc:
                log.warning("PhoneCodeExpired: %s", exc)
                try:
                    result = await login_userbot.send_code_request(phone)
                    state["phone_code_hash"] = result.phone_code_hash
                    await event.respond(
                        "⏰ **OTP expire hua — naya OTP bheja gaya!**\n\n"
                        "Apne Telegram par naya code dekho aur **turant** bhejo!"
                    )
                except Exception as exc2:
                    await event.respond(f"❌ OTP expired aur re-request bhi fail: `{exc2}`\nRestart with `/login`.")
                    login_state.pop(uid, None)
            except Exception as exc:
                log.exception("sign_in unexpected error: %s", exc)
                await event.respond(f"❌ sign_in error: `{exc}`\nRestart with `/login`.")
                login_state.pop(uid, None)

        # ── STEP C: Receive 2FA password ──────────────────────────────────
        elif step == "await_2fa":
            password = event.message.message.strip()
            try:
                await login_userbot.sign_in(password=password)
                await _finalize_login(event, uid)

            except errors.PasswordHashInvalidError:
                await event.respond("❌ Wrong 2FA password. Try again:")
            except Exception as exc:
                await event.respond(f"❌ 2FA error: `{exc}`\nRestart with `/login`.")
                login_state.pop(uid, None)


async def _finalize_login(event, uid: int) -> None:
    """
    Called after successful Telethon authentication (OTP or 2FA).
    Saves session to Supabase and activates the userbot live.
    """
    global userbot, userbot_active, login_userbot

    try:
        session_string = login_userbot.session.save()

        # Persist to Supabase
        ok = await sb_upsert_session(session_string)
        if not ok:
            await event.respond("⚠️ Authenticated but failed to save session to Supabase. Try `/login` again.")
            login_state.pop(uid, None)
            return

        # Attach handlers and go live — no reboot needed
        me = await login_userbot.get_me()
        name = me.username or me.first_name or str(me.id)

        attach_userbot_handlers(login_userbot)
        userbot = login_userbot
        userbot_active = True
        login_userbot = None  # Hand off ownership
        login_state.pop(uid, None)

        log.info("Userbot activated live for: %s", name)
        await event.respond(
            f"✅ **Userbot Activated!**\n\nLogged in as: @{name}\n\n"
            f"Now monitoring {len(ALL_SOURCE_IDS)} source channel(s)/group(s).\n"
            f"No reboot required. 🚀"
        )

    except Exception as exc:
        log.exception("_finalize_login failed: %s", exc)
        await event.respond(f"❌ Post-login error: `{exc}`")
        login_state.pop(uid, None)


# ─────────────────────────────────────────────
# MAIN ENTRYPOINT
# ─────────────────────────────────────────────

async def main():
    log.info("Booting XVIP Hybrid Bot...")

    # ── Step 1: Start the admin bot client ──────────────────────────────────
    await bot_client.start(bot_token=BOT_TOKEN)
    log.info("Admin bot started.")

    # ── Step 2: Register admin bot handlers ─────────────────────────────────
    register_bot_handlers()

    # ── Step 3: Check Supabase for an existing session ───────────────────────
    saved_session = os.environ.get("STRING_SESSION", "").strip() or await sb_get_session()

    if saved_session:
        log.info("Found saved session in Supabase — starting userbot...")
        success = await start_userbot(saved_session)
        if not success:
            log.warning("Saved session is invalid. Admin must run /login to re-authenticate.")
    else:
        log.warning("No session found in Supabase. Admin must send /login to the bot.")

    # ── Step 4: Run both clients concurrently ────────────────────────────────
    log.info("Running. Press Ctrl+C to stop.")

    clients_to_run = [bot_client]
    if userbot and userbot_active:
        clients_to_run.append(userbot)

    # Use run_until_disconnected on the bot (primary).
    # Userbot runs in background — its handlers fire independently.
    await bot_client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
