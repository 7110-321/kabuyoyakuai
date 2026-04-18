import os
import hmac
import base64
import hashlib
import time
import re
import threading
from collections import deque, defaultdict
from functools import wraps
from flask import Flask, request, abort, jsonify
from dotenv import load_dotenv
from flask import redirect
from src.disclosure_source import fetch_disclosures_by_codes
from src.pdf_extract import extract_summary_text_from_pdf
from src.llm import summarize_kessan_text, enrich_key_points_from_text
from src.summarizer import build_line_message
from datetime import datetime, timezone, timedelta
import json
from google.cloud import firestore

# Initialize Firestore client
db = firestore.Client()
from google.cloud import tasks_v2
from src.firestore_db import (
    mark_user_inactive,
    upsert_user,
    add_watch,
    remove_watch,
    get_watchlist,
    get_users_watching,
    is_watched_by_anyone,
    mark_disclosure_if_new,
    get_disclosure,
    upsert_disclosure,
    try_lock_disclosure,
    unlock_disclosure,
    try_lock_delivery,
    finalize_delivery_sent,
    finalize_delivery_failed,
    try_lock_cron,
    unlock_cron,
    get_all_watched_codes,
    has_summary,
    get_watch_added_at,
)

from src.line_api import (
    line_push_text_detail,
    line_reply_text,
)
import google.auth
import requests

load_dotenv()

app = Flask(__name__)

# ---- mode switch (deploy web/worker separately) ----
APP_MODE = (os.getenv("APP_MODE", "all") or "all").strip().lower()
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
CRON_SHARED_SECRET = os.getenv("CRON_SHARED_SECRET", "")
WATCHLIST_FREE_LIMIT = int(os.getenv("WATCHLIST_FREE_LIMIT", "3") or 3)
TASKS_LOCATION = os.getenv("TASKS_LOCATION", "asia-northeast1")
TASKS_QUEUE = os.getenv("TASKS_QUEUE", "disclosures-queue")
TASKS_SERVICE_ACCOUNT = os.getenv("TASKS_SERVICE_ACCOUNT", "")
RUN_BASE_URL = (os.getenv("RUN_BASE_URL", "") or "").rstrip("/")
ADMIN_BASIC_USER = (os.getenv("ADMIN_BASIC_USER", "") or "").strip()
ADMIN_BASIC_PASS = (os.getenv("ADMIN_BASIC_PASS", "") or "").strip()
ADMIN_ALLOWED_IPS = {
    ip.strip() for ip in (os.getenv("ADMIN_ALLOWED_IPS", "") or "").split(",") if ip.strip()
}
APP_ENV = (os.getenv("APP_ENV", "prod") or "prod").strip().lower()

SUBSCRIBE_RATE_LIMIT_WINDOW_SEC = int(os.getenv("SUBSCRIBE_RATE_LIMIT_WINDOW_SEC", "600") or 600)   # 10分
SUBSCRIBE_RATE_LIMIT_PER_IP = int(os.getenv("SUBSCRIBE_RATE_LIMIT_PER_IP", "5") or 5)
SUBSCRIBE_RATE_LIMIT_PER_UID = int(os.getenv("SUBSCRIBE_RATE_LIMIT_PER_UID", "3") or 3)
SUBSCRIBE_FAIL_BLOCK_SEC = int(os.getenv("SUBSCRIBE_FAIL_BLOCK_SEC", "1800") or 1800)  # 30分
SUBSCRIBE_MAX_FAILS = int(os.getenv("SUBSCRIBE_MAX_FAILS", "5") or 5)


def get_project_id() -> str:
    # 1) 明示 env（あなたが入れるなら）
    pid = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT") or os.getenv("PROJECT_ID")
    if pid:
        return pid
    # 2) Cloud Run のデフォルト認証から取得
    creds, proj = google.auth.default()
    if proj:
        return proj
    raise RuntimeError("project_id is missing: set GOOGLE_CLOUD_PROJECT (or PROJECT_ID)")

def verify_line_signature(body: bytes, signature: str) -> bool:
    mac = hmac.new(LINE_CHANNEL_SECRET.encode("utf-8"), body, hashlib.sha256).digest()
    expected = base64.b64encode(mac).decode("utf-8")
    return hmac.compare_digest(expected, signature)


def verify_cron_request(req) -> bool:
    env = (CRON_SHARED_SECRET or "").strip()
    if not env:
        app.logger.error("cron: CRON_SHARED_SECRET is empty (env var not set)")
        return False

    # 1) body（推奨：Schedulerはこちら）
    try:
        data = req.get_json(silent=True) or {}
        body_secret = (data.get("cronSecret") or "").strip()
        if body_secret:
            ok = hmac.compare_digest(body_secret.encode("utf-8"), env.encode("utf-8"))
            if not ok:
                app.logger.warning("cron: invalid cronSecret in body")
            return ok
    except Exception:
        pass

    # 2) header（互換：手動curl用）
    header = (req.headers.get("X-CRON-SECRET", "") or "").strip()
    if header:
        ok = hmac.compare_digest(header.encode("utf-8"), env.encode("utf-8"))
        if not ok:
            app.logger.warning("cron: invalid X-CRON-SECRET")
        return ok

    app.logger.warning("cron: missing cron secret (body/header)")
    return False


_CODE_IN_TITLE_PATTERNS = [
    r"[（(]\s*([0-9]{4}[A-Z]?)\s*[）)]",
    r"\b([0-9]{4}[A-Z]?)\b",
]

def _extract_code_from_recent_item(d: dict) -> str:
    code = (d.get("code") or "").strip().upper()
    if code:
        return code

    title = (d.get("title") or "").strip()
    for p in _CODE_IN_TITLE_PATTERNS:
        m = re.search(p, title)
        if m:
            return m.group(1).upper()

    return ""

def _is_recent_iso(iso_text: str, lookback_minutes: int = 180) -> bool:
    s = (iso_text or "").strip()
    if not s:
        return False
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return dt >= now - timedelta(minutes=lookback_minutes)
    except Exception:
        return False

# ---------------------------
# admin protect
# ---------------------------
def _client_ip() -> str:
    xff = (request.headers.get("X-Forwarded-For") or "").strip()
    if xff:
        return xff.split(",")[0].strip()
    return (request.remote_addr or "").strip()

def _basic_auth_ok(req) -> bool:
    auth = req.authorization
    if not auth:
        return False
    if not (ADMIN_BASIC_USER and ADMIN_BASIC_PASS):
        return False
    return (
        hmac.compare_digest((auth.username or "").encode("utf-8"), ADMIN_BASIC_USER.encode("utf-8"))
        and
        hmac.compare_digest((auth.password or "").encode("utf-8"), ADMIN_BASIC_PASS.encode("utf-8"))
    )

def require_admin_access():
    # 本番は env_check 自体を閉じたいので、呼び出し側で 404 にしてOK
    ip = _client_ip()

    # IP制限が設定されているなら、まず IP を確認
    if ADMIN_ALLOWED_IPS and ip in ADMIN_ALLOWED_IPS:
        return None

    # IP制限できない場合は Basic認証
    if _basic_auth_ok(request):
        return None

    return (
        jsonify({"error": "unauthorized"}),
        401,
        {"WWW-Authenticate": 'Basic realm="Admin Area"'}
    )

# ---------------------------
# anti carding / anti bot
# ---------------------------
_rate_lock = threading.Lock()
_ip_events = defaultdict(deque)      # ip -> timestamps
_uid_events = defaultdict(deque)     # uid -> timestamps
_ip_failures = defaultdict(deque)    # ip -> failed timestamps
_uid_failures = defaultdict(deque)   # uid -> failed timestamps
_ip_block_until = {}
_uid_block_until = {}

def _now_ts() -> float:
    return time.time()

def _prune_deque(dq: deque, window_sec: int, now_ts: float):
    while dq and (now_ts - dq[0]) > window_sec:
        dq.popleft()

def is_temporarily_blocked(ip: str, uid: str) -> bool:
    now_ts = _now_ts()
    with _rate_lock:
        if _ip_block_until.get(ip, 0) > now_ts:
            return True
        if uid and _uid_block_until.get(uid, 0) > now_ts:
            return True
    return False

def check_and_mark_subscribe_attempt(ip: str, uid: str) -> tuple[bool, str]:
    now_ts = _now_ts()
    with _rate_lock:
        ip_dq = _ip_events[ip]
        uid_dq = _uid_events[uid] if uid else deque()

        _prune_deque(ip_dq, SUBSCRIBE_RATE_LIMIT_WINDOW_SEC, now_ts)
        _prune_deque(uid_dq, SUBSCRIBE_RATE_LIMIT_WINDOW_SEC, now_ts)

        if len(ip_dq) >= SUBSCRIBE_RATE_LIMIT_PER_IP:
            return False, "rate_limited_ip"
        if uid and len(uid_dq) >= SUBSCRIBE_RATE_LIMIT_PER_UID:
            return False, "rate_limited_uid"

        ip_dq.append(now_ts)
        if uid:
            uid_dq.append(now_ts)
            _uid_events[uid] = uid_dq

    return True, "ok"

def record_subscribe_failure(ip: str, uid: str):
    now_ts = _now_ts()
    with _rate_lock:
        ip_dq = _ip_failures[ip]
        _prune_deque(ip_dq, SUBSCRIBE_RATE_LIMIT_WINDOW_SEC, now_ts)
        ip_dq.append(now_ts)

        if len(ip_dq) >= SUBSCRIBE_MAX_FAILS:
            _ip_block_until[ip] = now_ts + SUBSCRIBE_FAIL_BLOCK_SEC

        if uid:
            uid_dq = _uid_failures[uid]
            _prune_deque(uid_dq, SUBSCRIBE_RATE_LIMIT_WINDOW_SEC, now_ts)
            uid_dq.append(now_ts)

            if len(uid_dq) >= SUBSCRIBE_MAX_FAILS:
                _uid_block_until[uid] = now_ts + SUBSCRIBE_FAIL_BLOCK_SEC

def clear_subscribe_failures(ip: str, uid: str):
    with _rate_lock:
        _ip_failures.pop(ip, None)
        _ip_block_until.pop(ip, None)
        if uid:
            _uid_failures.pop(uid, None)
            _uid_block_until.pop(uid, None)


def _normalize_text(s: str) -> str:
    if not s:
        return ""

    # 全角英数字→半角（０-９Ａ-Ｚａ-ｚ）
    trans = {}
    fw_digits = "０１２３４５６７８９"
    hw_digits = "0123456789"
    fw_upper = "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ"
    hw_upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    fw_lower = "ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ"
    hw_lower = "abcdefghijklmnopqrstuvwxyz"

    trans.update(str.maketrans(fw_digits, hw_digits))
    trans.update(str.maketrans(fw_upper, hw_upper))
    trans.update(str.maketrans(fw_lower, hw_lower))
    s = s.translate(trans)

    # よくある矢印
    s = s.replace("→", " ").replace("➡︎", " ").replace("➡", " ").replace("⇒", " ")
    # 全角スペース→半角
    s = s.replace("\u3000", " ")
    # 余分な空白を詰める
    s = re.sub(r"\s+", " ", s).strip()

    # 英字は大文字に統一（285a → 285A）
    return s.upper()

def normalize_stock_code(v: str) -> str:
    s = (str(v).strip().upper() if v else "")

    # 全角→半角
    trans = {}
    fw_digits = "０１２３４５６７８９"
    hw_digits = "0123456789"
    fw_upper = "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ"
    hw_upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    trans.update(str.maketrans(fw_digits, hw_digits))
    trans.update(str.maketrans(fw_upper, hw_upper))
    s = s.translate(trans).replace("\u3000", " ").strip().upper()

    # TDnet 5桁末尾0 → 4桁
    m = re.fullmatch(r"([0-9]{4})0", s)
    if m:
        return m.group(1)

    # 通常4桁
    m = re.fullmatch(r"([0-9]{4})", s)
    if m:
        return m.group(1)

    # 4桁+英字
    m = re.fullmatch(r"([0-9]{4}[A-Z])", s)
    if m:
        return m.group(1)

    # 文中に埋まっているケースも救う
    m = re.search(r"\b([0-9]{4})0\b", s)
    if m:
        return m.group(1)

    m = re.search(r"\b([0-9]{4}[A-Z]?)\b", s)
    if m:
        return m.group(1)

    return ""

def _parse_command(text: str):
    t = _normalize_text(text)
    if not t:
        return None, None

    if t in ("LIST", "L", "リスト"):
        return "list", None

    CODE = r"([0-9A-Z]{4,5})"

    m = re.match(rf"^銘柄(追加|削除)\s*{CODE}$", t)
    if m:
        action = m.group(1)
        code = normalize_stock_code(m.group(2))
        if not code:
            return None, None
        return ("add" if action == "追加" else "remove"), code

    m = re.match(rf"^{CODE}\s*(追加|削除)$", t)
    if m:
        code = normalize_stock_code(m.group(1))
        action = m.group(2)
        if not code:
            return None, None
        return ("add" if action == "追加" else "remove"), code

    return None, None

if APP_MODE in ("web", "all"):
    @app.post("/webhook")
    def webhook():
        try:
            body = request.get_data()
            sig = request.headers.get("X-Line-Signature", "")

            if not LINE_CHANNEL_SECRET:
                app.logger.error("LINE_CHANNEL_SECRET is empty")
                return "LINE_CHANNEL_SECRET is empty", 500

            if not sig:
                app.logger.error("Missing X-Line-Signature header")
                return "missing signature", 403

            if not verify_line_signature(body, sig):
                app.logger.error("Invalid signature")
                return "invalid signature", 403

            data = request.get_json(force=True) or {}
            events = data.get("events") or []

            for ev in events:
                try:
                    ev_type = ev.get("type")
                    reply_token = ev.get("replyToken")
                    user_id = (ev.get("source") or {}).get("userId")

                    if not user_id:
                        continue

                    # follow
                    if ev_type == "follow":
                        upsert_user(user_id)
                        if reply_token:
                            line_reply_text(
                                reply_token,
                                "【決算要約AI】へようこそ！\n\n"
                                "使い方\n"
                                "・銘柄を追加： 1234追加\n"
                                "・削除： 1234削除\n"
                                "・ウォッチリスト参照： リスト\n"
                                "ウォッチに入れた銘柄の決算短信だけ要約して通知します。",
                            )
                        continue

                    # text message
                    if ev_type == "message" and (ev.get("message") or {}).get("type") == "text":
                        upsert_user(user_id)
                        text = (ev.get("message") or {}).get("text", "")
                        cmd, code = _parse_command(text)

                        if not reply_token:
                            continue

                        if cmd == "add" and code:
                            add_watch(user_id, code)
                            line_reply_text(reply_token, f"{code} をウォッチに追加しました。")
                            continue

                        if cmd == "remove" and code:
                            remove_watch(user_id, code)
                            line_reply_text(reply_token, f"{code} をウォッチから削除しました。")
                            continue

                        if cmd == "list":
                            wl = get_watchlist(user_id)
                            if not wl:
                                line_reply_text(reply_token, "ウォッチリストは空です。\n追加：1234")
                            else:
                                line_reply_text(reply_token, "ウォッチリスト\n" + "\n".join(wl))
                            continue

                        # default
                        line_reply_text(
                            reply_token,
                            "操作はこちら👇\n"
                            "・銘柄追加→ 1234追加\n"
                            "・銘柄削除→ 1234削除\n"
                            "・ウォッチリスト参照→ リスト\n"
                        )
                        continue

                except Exception as e:
                    # 1イベント失敗しても webhook 全体は落とさない
                    app.logger.exception(f"webhook: event failed err={e}")
                    continue

            app.logger.info("webhook: reached end, returning 200")
            return "ok", 200

        except Exception as e:
            app.logger.exception(f"webhook: fatal err={e}")
            return "ok", 200


if APP_MODE in ("web","all"):
    @app.get("/admin/env_check")
    def env_check():
        # 本番では存在自体を見せない
        if APP_ENV == "prod":
            abort(404)

        protected = require_admin_access()
        if protected:
            return protected

        return jsonify({
            "LINE_CHANNEL_SECRET_set": bool((os.getenv("LINE_CHANNEL_SECRET") or "").strip()),
            "LINE_CHANNEL_ACCESS_TOKEN_set": bool((os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()),
            "CRON_SHARED_SECRET_set": bool((os.getenv("CRON_SHARED_SECRET") or "").strip()),
        })

if APP_MODE in ("worker", "all"):
    @app.post("/cron/check")
    def cron_check():
        t0 = time.time()
        app.logger.info("cron: start /cron/check")

        if not verify_cron_request(request):
            abort(403)

        if not try_lock_cron("check", ttl_seconds=90):
            app.logger.info("cron: skipped (cron locked)")
            return jsonify({
                "status": "ok",
                "processed": 0,
                "stats": {"skipped_cron_locked": 1, "fetched": 0, "enqueued": 0},
            })

        try:
            watched_codes = sorted(set(get_all_watched_codes(limit=5000)))
            watched_set = set(str(x).strip().upper() for x in watched_codes if str(x).strip())

            if not watched_set:
                return jsonify({
                    "status": "ok",
                    "processed": 0,
                    "stats": {"fetched": 0, "skipped_no_watch": 1, "enqueued": 0},
                })

            disclosures = fetch_disclosures_by_codes(
                watched_codes,
                per_feed=1,
                limit_per_feed=10,
                lookback_hours=48,
            )

            enqueued = 0
            skipped_no_id = 0
            skipped_no_code = 0
            skipped_not_watched = 0
            skipped_already_done = 0
            skipped_mark_failed = 0
            skipped_enqueue_failed = 0

            for d in disclosures:
                disclosure_id = d.get("disclosureId")
                if not disclosure_id:
                    skipped_no_id += 1
                    continue

                code = (d.get("code") or "").strip().upper()
                if not code:
                    skipped_no_code += 1
                    app.logger.warning(
                        f"cron: skipped_no_code id={disclosure_id} title={d.get('title','')}"
                    )
                    continue

                if code not in watched_set:
                    skipped_not_watched += 1
                    continue

                d["code"] = code
                d["codeSource"] = d.get("codeSource") or "rss_by_codes"

                try:
                    is_new = mark_disclosure_if_new(disclosure_id, d)
                    app.logger.info(f"cron: disclosure id={disclosure_id} is_new={is_new}")
                except Exception as e:
                    skipped_mark_failed += 1
                    app.logger.exception(f"cron: mark_disclosure failed id={disclosure_id}: {e}")
                    continue
                
                if not is_new:
                    skipped_already_done += 1
                    app.logger.info(f"cron: skipped_already_done id={disclosure_id}")
                    continue

                try:
                    basic = {
                        "companyName": d.get("companyName"),
                        "code": d.get("code"),
                        "codeSource": d.get("codeSource"),
                        "url": d.get("url"),
                        "title": d.get("title"),
                        "category": d.get("category"),
                        "publishedAt": d.get("publishedAt"),
                        "source": d.get("source"),
                    }
                    basic = {k: v for k, v in basic.items() if v not in (None, "")}
                    if basic:
                        upsert_disclosure(disclosure_id, basic)
                except Exception as e:
                    app.logger.warning(f"cron: upsert basic failed id={disclosure_id}: {e}")

                try:
                    enqueue_disclosure_task(disclosure_id)
                    enqueued += 1
                    app.logger.info(f"cron: enqueued id={disclosure_id}")
                except Exception as e:
                    skipped_enqueue_failed += 1

                    # enqueue に失敗したら、mark_disclosure_if_new() で作った
                    # disclosures/{disclosure_id} を削除して、次回 cron で再挑戦できるようにする
                    try:
                        db.collection("disclosures").document(disclosure_id).delete()
                        app.logger.warning(f"cron: rolled back disclosure id={disclosure_id}")
                    except Exception as delete_e:
                        app.logger.exception(
                            f"cron: rollback failed id={disclosure_id}: {delete_e}"
                        )
                    app.logger.exception(f"cron: enqueue failed id={disclosure_id}: {e}")

            app.logger.info(f"cron: fetched watched disclosures={len(disclosures)}")
            app.logger.info(f"cron: end total={(time.time()-t0):.2f}s enqueued={enqueued}")

            return jsonify({
                "status": "ok",
                "processed": enqueued,
                "stats": {
                    "fetched": len(disclosures),
                    "skipped_no_id": skipped_no_id,
                    "skipped_no_code": skipped_no_code,
                    "skipped_not_watched": skipped_not_watched,
                    "skipped_mark_failed": skipped_mark_failed,
                    "skipped_already_done": skipped_already_done,
                    "skipped_enqueue_failed": skipped_enqueue_failed,
                    "enqueued": enqueued,
                },
            })

        finally:
            unlock_cron("check")
        
def enqueue_disclosure_task(disclosure_id: str):
    client = tasks_v2.CloudTasksClient()

    project_id = get_project_id()
    parent = client.queue_path(project_id, TASKS_LOCATION, TASKS_QUEUE)

    base = (RUN_BASE_URL or "").strip().rstrip("/")
    if not base.startswith("http://") and not base.startswith("https://"):
        raise RuntimeError(f"RUN_BASE_URL is invalid: {base!r}")

    url = f"{base}/tasks/process_disclosure"
    payload = json.dumps({"disclosureId": disclosure_id}).encode()

    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": url,
            "headers": {"Content-Type": "application/json"},
            "body": payload,
        }
    }

    if TASKS_SERVICE_ACCOUNT:
        task["http_request"]["oidc_token"] = {
            "service_account_email": TASKS_SERVICE_ACCOUNT
        }

    client.create_task(request={"parent": parent, "task": task})


if APP_MODE in ("worker","all"):
    @app.post("/cron/recover")
    def cron_recover():
        """
        取りこぼし救済ジョブ：
        - 直近の開示をもう一度広めに取りに行き、
        - 「未要約（has_summary=False）」のものだけ Tasks に再投入する。
        - 既に送れている/要約済みはスキップされるので、何度回しても安全。
        """
        if not verify_cron_request(request):
            abort(403)

        if not try_lock_cron("recover", ttl_seconds=300):
            return jsonify({
                "status": "ok",
                "processed": 0,
                "stats": {"skipped_cron_locked": 1, "fetched": 0, "enqueued": 0}
            })

        try:
            codes = get_all_watched_codes(limit=5000)
            app.logger.info(f"recover: watched codes={len(codes)}")

            if not codes:
                return jsonify({
                    "status": "ok",
                    "processed": 0,
                    "stats": {"fetched": 0, "skipped_no_id": 0, "enqueued": 0}
                })

            disclosures = fetch_disclosures_by_codes(
                codes,
                per_feed=80,
                limit_per_feed=100,  # 800 -> 100 に縮小
            )
            app.logger.info(f"recover: fetched disclosures={len(disclosures)}")

            enqueued = 0
            skipped_no_id = 0
            skipped_already_done = 0
            skipped_mark_failed = 0
            skipped_enqueue_failed = 0

            for d in disclosures:
                disclosure_id = d.get("disclosureId")
                if not disclosure_id:
                    skipped_no_id += 1
                    continue

                try:
                    _ = mark_disclosure_if_new(disclosure_id, d)
                except Exception as e:
                    skipped_mark_failed += 1
                    app.logger.exception(f"recover: mark_disclosure failed id={disclosure_id}: {e}")
                    continue

                try:
                    if has_summary(disclosure_id):
                        skipped_already_done += 1
                        continue
                except Exception as e:
                    app.logger.exception(f"recover: has_summary failed id={disclosure_id}: {e}")

                try:
                    basic = {
                        "companyName": d.get("companyName"),
                        "code": d.get("code"),
                        "codeSource": d.get("codeSource"),
                        "url": d.get("url"),
                        "title": d.get("title"),
                        "category": d.get("category"),
                        "publishedAt": d.get("publishedAt"),
                    }
                    basic = {k: v for k, v in basic.items() if v not in (None, "")}
                    if basic:
                        upsert_disclosure(disclosure_id, basic)
                except Exception as e:
                    app.logger.warning(f"recover: upsert basic failed id={disclosure_id}: {e}")

                try:
                    enqueue_disclosure_task(disclosure_id)
                    enqueued += 1
                except Exception as e:
                    skipped_enqueue_failed += 1
                    app.logger.exception(f"recover: enqueue failed id={disclosure_id}: {e}")

            return jsonify({
                "status": "ok",
                "processed": enqueued,
                "stats": {
                    "fetched": len(disclosures),
                    "skipped_no_id": skipped_no_id,
                    "skipped_mark_failed": skipped_mark_failed,
                    "skipped_already_done": skipped_already_done,
                    "skipped_enqueue_failed": skipped_enqueue_failed,
                    "enqueued": enqueued,
                }
            })

        except Exception as e:
            app.logger.exception(f"recover: unexpected failed: {e}")
            return jsonify({
                "status": "ok",
                "processed": 0,
                "stats": {
                    "fetched": 0,
                    "enqueued": 0,
                    "unexpected_error": str(e)[:200],
                }
            }), 200

        finally:
            unlock_cron("recover")


if APP_MODE in ("worker","all"):
    @app.post("/tasks/process_disclosure")
    def tasks_process_disclosure():
        data = request.get_json(silent=True) or {}
        disclosure_id = data.get("disclosureId")
        if not disclosure_id:
            return jsonify({"status": "error", "reason": "no_disclosureId"}), 400

        d = get_disclosure(disclosure_id) or {}
        url = d.get("url")
        if not url:
            return jsonify({"status": "ok", "skipped": "no_url"}), 200

        # --------
        # 0) 開示日時（publishedAt）を datetime にする（無ければ None）
        #    ※ disclosure_source.py 側で入ってくる想定。無い場合は addedAtフィルタをスキップする。
        # --------
        def _parse_dt(v):
            if not v:
                return None
            # Firestore Timestamp / datetime
            if hasattr(v, "year") and hasattr(v, "month") and hasattr(v, "day"):
                try:
                    return v
                except Exception:
                    return None
            # ISO文字列
            if isinstance(v, str):
                try:
                    return datetime.fromisoformat(v)
                except Exception:
                    return None
            return None

        published_at_dt = _parse_dt(d.get("publishedAt")) or _parse_dt(d.get("disclosedAt")) or _parse_dt(d.get("createdAt"))

        # ✅ pre_pdf 判定（取りこぼし防止版）
        code0 = (d.get("code") or "").strip().upper()
        code_source = (d.get("codeSource") or "").strip()

        if code0 and code_source in ("pdf", "watchmatch"):
            try:
                if not is_watched_by_anyone(code0):
                    return jsonify({"status": "ok", "skipped": "not_watched_pre_pdf"}), 200
            except Exception as e:
                app.logger.exception(
                    f"task: watch_check_pre_pdf failed code={code0} id={disclosure_id}: {e}"
                )
                # 失敗時は安全側（落とさず続行）
                pass
        # --------
        # 1) PDF抽出（ここで code/company を拾う）
        # --------
        try:
            pdf_text, pdf_code, pdf_company = extract_summary_text_from_pdf(url)
        except Exception as e:
            msg = str(e)

            temporary_pdf_errors = (
                "HTTP 404",
                "HTTP 403",
                "proxy HTTP 404",
                "proxy HTTP 403",
                "non-pdf",
                "pdf too small",
                "Read timed out",
                "ConnectTimeout",
                "ConnectionError",
            )

            if any(k in msg for k in temporary_pdf_errors):
                app.logger.warning(
                    f"task: pdf_extract temporary_fail id={disclosure_id} url={url}: {msg}"
                )
                try:
                    upsert_disclosure(disclosure_id, {
                        "pdfError": msg[:500],
                        "pdfRetryNeeded": True,
                    })
                except Exception:
                    pass

                # Cloud Tasks に再試行させる
                return jsonify({
                    "status": "error",
                    "reason": "pdf_temporarily_unavailable",
                    "detail": msg[:200],
                }), 500

            app.logger.exception(f"task: pdf_extract failed id={disclosure_id} url={url}: {e}")
            try:
                upsert_disclosure(disclosure_id, {
                    "pdfError": msg[:500],
                })
            except Exception:
                pass

            return jsonify({
                "status": "error",
                "reason": "pdf_extract_failed",
                "detail": msg[:200],
            }), 500

        def _norm_code(v):
            return normalize_stock_code(v)

        if pdf_company and not d.get("companyName"):
            d["companyName"] = pdf_company

        # code は「既存値 → PDF → title」の順で補完
        current_code = _norm_code(d.get("code"))
        pdf_code_n = _norm_code(pdf_code)

        if current_code:
            d["code"] = current_code
        elif pdf_code_n:
            d["code"] = pdf_code_n
            d["codeSource"] = "pdf"
        else:
            title = (d.get("title") or "")
            title_code = normalize_stock_code(title)
            if title_code:
                d["code"] = title_code
                d["codeSource"] = d.get("codeSource") or "title"

        # basic upsert（既存を潰さない）
        try:
            basic = {
                "companyName": d.get("companyName"),
                "code": d.get("code"),
                "codeSource": d.get("codeSource"),
                "url": d.get("url"),
                "title": d.get("title"),
                "category": d.get("category"),
                "publishedAt": d.get("publishedAt"),
            }
            basic = {k: v for k, v in basic.items() if v not in (None, "")}
            if basic:
                upsert_disclosure(disclosure_id, basic)
        except Exception as e:
            app.logger.warning(f"task: upsert basic failed id={disclosure_id}: {e}")

        code = _norm_code(d.get("code"))
        if code:
            d["code"] = code

        if not code:
            app.logger.warning(
                f"task: skipped_no_code id={disclosure_id} url={url} "
                f"title={d.get('title','')} pdf_code={pdf_code!r}"
            )
            return jsonify({"status": "ok", "skipped": "no_code"}), 200

        # watchedじゃなければ終了（LLMも回さない）
        try:
            if not is_watched_by_anyone(code):
                return jsonify({"status": "ok", "skipped": "not_watched"}), 200
        except Exception as e:
            app.logger.exception(f"task: is_watched_by_anyone failed code={code} id={disclosure_id}: {e}")
            return jsonify({"status": "ok", "skipped": "watch_check_failed"}), 200

        # --------
        # 2) ✅ addedAt カットオフ判定（ここがポイント：LLM前にやる）
        #    「追加時刻より前の開示」はそのユーザーには送らない
        # --------
        try:
            users = get_users_watching(code)
        except Exception as e:
            app.logger.exception(f"task: get_users_watching failed code={code} id={disclosure_id}: {e}")
            return jsonify({"status": "ok", "skipped": "get_users_failed"}), 200

        eligible_users = []
        if published_at_dt:
            # timezone を揃える
            pa = published_at_dt.replace(tzinfo=timezone.utc) if getattr(published_at_dt, "tzinfo", None) is None else published_at_dt

            for uid in users:
                try:
                    added_at = get_watch_added_at(uid, code)  # users/{uid}/watchlist/{code}.addedAt
                except Exception:
                    added_at = None

                if added_at:
                    aa = added_at.replace(tzinfo=timezone.utc) if getattr(added_at, "tzinfo", None) is None else added_at
                    # 開示が「追加より前」なら送らない
                    if pa < aa:
                        continue
                eligible_users.append(uid)
        else:
            # publishedAt が無い場合は「全員対象」にする（落とさない方針）
            eligible_users = list(users)

        # ✅ 送る人が0なら、ここで終了（過去決算の暴発もLLMコストも止まる）
        if not eligible_users:
            return jsonify({"status": "ok", "skipped": "no_eligible_users"}), 200

        # --------
        # 3) 要約（キャッシュ優先 + disclosureロック）
        # --------
        try:
            existing = get_disclosure(disclosure_id) or {}
            if existing.get("summaryDone") and existing.get("keyPoints") and existing.get("outlook") is not None:
                d.update({
                    "companyName": existing.get("companyName") or d.get("companyName"),
                    "code": existing.get("code") or d.get("code"),
                    "profitLabel": existing.get("profitLabel") or d.get("profitLabel"),
                    "yoyPct": existing.get("yoyPct"),
                    "salesYoyPct": existing.get("salesYoyPct"),
                    "opProfitYoyPct": existing.get("opProfitYoyPct") or existing.get("yoyPct"),
                    "ordinaryYoyPct": existing.get("ordinaryYoyPct"),
                    "outlook": existing.get("outlook") or d.get("outlook", "不明"),
                    "keyPoints": existing.get("keyPoints") or d.get("keyPoints", []),
                    "summaryModel": existing.get("summaryModel"),
                    "summaryVersion": existing.get("summaryVersion"),
                })
            else:
                if try_lock_disclosure(disclosure_id):
                    try:
                        if pdf_text and pdf_text.strip():
                            s = summarize_kessan_text(
                                text=pdf_text,
                                company=d.get("companyName", "不明"),
                                code=d.get("code", "----"),
                                title=d.get("title", ""),
                            )

                            enriched_points = enrich_key_points_from_text(
                                text=pdf_text,
                                key_points=s.get("keyPoints") or [],
                                limit=5,
                            )

                            d.update({
                                "profitLabel": s.get("profitLabel"),
                                "yoyPct": s.get("yoyPct"),
                                "salesYoyPct": s.get("salesYoyPct"),
                                "opProfitYoyPct": s.get("opProfitYoyPct") or s.get("yoyPct"),
                                "ordinaryYoyPct": s.get("ordinaryYoyPct"),
                                "outlook": s.get("outlook"),
                                "keyPoints": enriched_points,
                                "summaryModel": s.get("summaryModel"),
                                "summaryVersion": s.get("summaryVersion"),
                            })
                            upsert_disclosure(disclosure_id, {
                                "companyName": d.get("companyName"),
                                "code": d.get("code"),
                                "profitLabel": d.get("profitLabel"),
                                "yoyPct": d.get("yoyPct"),
                                "salesYoyPct": d.get("salesYoyPct"),
                                "opProfitYoyPct": d.get("opProfitYoyPct"),
                                "ordinaryYoyPct": d.get("ordinaryYoyPct"),
                                "outlook": d.get("outlook"),
                                "keyPoints": d.get("keyPoints"),
                                "summaryModel": d.get("summaryModel"),
                                "summaryVersion": d.get("summaryVersion"),
                                "summaryDone": True,
                            })
                    finally:
                        unlock_disclosure(disclosure_id)
        except Exception as e:
            app.logger.exception(f"task: summary failed id={disclosure_id}: {e}")
            try:
                upsert_disclosure(disclosure_id, {
                    "summaryError": str(e)[:500],
                    "summaryRetryNeeded": True,
                })
            except Exception:
                pass

            return jsonify({
                "status": "error",
                "reason": "summary_failed",
                "detail": str(e)[:200],
            }), 500

        if d.get("opProfitYoyPct") is None and d.get("yoyPct") is not None:
            d["opProfitYoyPct"] = d["yoyPct"]

        msg = build_line_message(d)

        # --------
        # 4) 配信（eligible_users だけ）
        # --------
        sent = 0
        for uid in eligible_users:
            try:
                if not try_lock_delivery(disclosure_id, uid):
                    continue
            except Exception as e:
                app.logger.exception(f"task: try_lock_delivery failed uid={uid} id={disclosure_id}: {e}")
                continue

            try:
                ok, status_code, body = line_push_text_detail(uid, msg)
                app.logger.info(f"push result uid={uid} ok={ok} status={status_code} body={body}")
            except Exception as e:
                finalize_delivery_failed(disclosure_id, uid, repr(e))
                continue

            if ok:
                finalize_delivery_sent(disclosure_id, uid)
                sent += 1
            else:
                if status_code in (400, 403):
                    try:
                        mark_user_inactive(uid, f"push_failed status={status_code} body={body}")
                    except Exception:
                        pass
                finalize_delivery_failed(disclosure_id, uid, f"push_failed status={status_code} body={body}")

        return jsonify({"status": "ok", "sent": sent, "code": code}), 200

if APP_MODE in ("worker","all"):
    @app.post("/admin/test_one")
    def test_one():
        protected = require_admin_access()
        if protected:
            return protected

        if not verify_cron_request(request):
            abort(403)

        data = request.get_json(force=True) or {}
        url = (data.get("url") or "").strip()
        title = (data.get("title") or "(test)").strip()

        if not url:
            return jsonify({"status": "error", "reason": "missing url"}), 400

        # 1) PDF抽出
        try:
            pdf_text, pdf_code, pdf_company = extract_summary_text_from_pdf(url)
        except Exception as e:
            app.logger.exception(f"test_one: pdf_extract failed: {e}")
            return jsonify({"status": "error", "reason": "pdf_extract_failed", "error": repr(e)}), 500

        # 2) LLM要約
        try:
            s = summarize_kessan_text(
                text=pdf_text,
                company=pdf_company or "不明",
                code=pdf_code or "----",
                title=title,
            )
        except Exception as e:
            app.logger.exception(f"test_one: llm failed: {e}")
            return jsonify({"status": "error", "reason": "llm_failed", "error": repr(e)}), 500

        d = {
            "category": "決算短信",
            "companyName": s.get("companyName") or (pdf_company or "不明"),
            "code": s.get("code") or (pdf_code or "----"),
            "title": title,
            "url": url,
            **s,
        }

        if d.get("opProfitYoyPct") is None and d.get("yoyPct") is not None:
            d["opProfitYoyPct"] = d["yoyPct"]

        msg = build_line_message(d)

        # 3) ウォッチしてるユーザーに送る
        code = str(d.get("code") or "")
        users = get_users_watching(code) if code and code != "----" else []

        sent = failed = skipped = 0
        test_disclosure_id = f"test_{hash(url)}"

        for uid in users:
            try:
                if not try_lock_delivery(test_disclosure_id, uid):
                    skipped += 1
                    continue
            except Exception as e:
                app.logger.exception(f"test_one: try_lock_delivery failed uid={uid}: {e}")
                failed += 1
                continue

            try:
                ok, status_code, body = line_push_text_detail(uid, msg)
            except Exception as e:
                app.logger.exception(f"test_one: push failed uid={uid}: {e}")
                finalize_delivery_failed(test_disclosure_id, uid, repr(e))
                failed += 1
                continue

            if ok:
                finalize_delivery_sent(test_disclosure_id, uid)
                sent += 1
            else:
                if status_code in (400, 403):
                    mark_user_inactive(uid, f"push_failed status={status_code} body={body}")

                finalize_delivery_failed(test_disclosure_id, uid, f"push_failed status={status_code} body={body}")
                failed += 1

        return jsonify({
            "status": "ok",
            "code": d.get("code"),
            "company": d.get("companyName"),
            "watchers": len(users),
            "sent": sent,
            "failed": failed,
            "skipped": skipped,
            "messagePreview": msg[:300],
        })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
