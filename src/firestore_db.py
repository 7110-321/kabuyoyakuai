import os
from datetime import datetime, timezone, timedelta
from google.cloud import firestore
from google.api_core.exceptions import AlreadyExists

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
try:
    DELIVERY_MAX_ATTEMPTS = int(os.getenv("DELIVERY_MAX_ATTEMPTS", "7"))
except ValueError:
    DELIVERY_MAX_ATTEMPTS = 7
db = firestore.Client(project=PROJECT_ID) if PROJECT_ID else firestore.Client()


# =========================
# Users
# =========================
def upsert_user(line_user_id: str):
    ref = db.collection("users").document(line_user_id)
    ref.set(
        {
            "lineUserId": line_user_id,  # docIdと同じだけど入れておくとデバッグ楽
            "updatedAt": firestore.SERVER_TIMESTAMP,
            "createdAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )


def get_all_active_users(limit: int = 20000) -> list[str]:
    q = db.collection("users").where("isActive", "==", True).limit(limit)
    return [doc.id for doc in q.stream()]


def mark_user_inactive(line_user_id: str, reason: str = ""):
    db.collection("users").document(line_user_id).set(
        {
            "isActive": False,
            "inactiveReason": (reason or "")[:200],
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

# =========================
# Watchlist (B案：ユーザー配下 + 逆引き)
# =========================
def _watch_doc(line_user_id: str, code: str):
    return (
        db.collection("users")
        .document(line_user_id)
        .collection("watchlist")
        .document(str(code))
    )


def _reverse_doc(line_user_id: str, code: str):
    return (
        db.collection("watchlist_codes")
        .document(str(code))
        .collection("users")
        .document(line_user_id)
    )


def add_watch(line_user_id: str, code: str) -> None:
    code = str(code)

    user_ref = db.collection("users").document(line_user_id)
    watch_ref = _watch_doc(line_user_id, code)
    rev_ref = _reverse_doc(line_user_id, code)
    agg_ref = db.collection("watchlist_codes").document(code)

    @firestore.transactional
    def _tx(tx: firestore.Transaction):
        # ✅ 先に読む（read after write を避ける）
        watch_snap = watch_ref.get(transaction=tx)
        agg_snap = agg_ref.get(transaction=tx)

        # すでに登録済みなら何もしない
        if watch_snap.exists:
            tx.set(user_ref, {"updatedAt": firestore.SERVER_TIMESTAMP}, merge=True)
            return

        # ✅ ここから書く（write）
        tx.set(
            user_ref,
            {
                "lineUserId": line_user_id,
                "updatedAt": firestore.SERVER_TIMESTAMP,
                "createdAt": firestore.SERVER_TIMESTAMP,
                "isActive": True,
            },
            merge=True,
        )
        tx.set(
            watch_ref,
            {
                "code": code,
                "createdAt": firestore.SERVER_TIMESTAMP,
                "addedAt": firestore.SERVER_TIMESTAMP,  # ✅追加：この時刻より前の開示は送らない
            },
            merge=True,
        )
        tx.set(rev_ref, {"lineUserId": line_user_id, "createdAt": firestore.SERVER_TIMESTAMP}, merge=True)

        prev = 0
        if agg_snap.exists:
            prev = (agg_snap.to_dict() or {}).get("userCount", 0) or 0
        tx.set(agg_ref, {"userCount": prev + 1, "updatedAt": firestore.SERVER_TIMESTAMP}, merge=True)

    _tx(db.transaction())

def get_watch_added_at(line_user_id: str, code: str):
    doc = _watch_doc(line_user_id, code).get()
    if not doc.exists:
        return None
    return (doc.to_dict() or {}).get("addedAt") or (doc.to_dict() or {}).get("createdAt")


def remove_watch(line_user_id: str, code: str) -> None:
    code = str(code)

    user_ref = db.collection("users").document(line_user_id)
    watch_ref = _watch_doc(line_user_id, code)
    rev_ref = _reverse_doc(line_user_id, code)
    agg_ref = db.collection("watchlist_codes").document(code)

    @firestore.transactional
    def _tx(tx: firestore.Transaction):
        # ✅ 先に読む
        watch_snap = watch_ref.get(transaction=tx)
        agg_snap = agg_ref.get(transaction=tx)

        # 未登録なら何もしない
        if not watch_snap.exists:
            tx.set(user_ref, {"updatedAt": firestore.SERVER_TIMESTAMP}, merge=True)
            return

        # ✅ ここから書く
        tx.delete(watch_ref)
        tx.delete(rev_ref)

        if agg_snap.exists:
            prev = (agg_snap.to_dict() or {}).get("userCount", 0) or 0
            new = max(prev - 1, 0)
            if new == 0:
                tx.delete(agg_ref)  # ★親ドキュメントも消す
            else:
                tx.set(agg_ref, {"userCount": new, "updatedAt": firestore.SERVER_TIMESTAMP}, merge=True)

        tx.set(user_ref, {"updatedAt": firestore.SERVER_TIMESTAMP}, merge=True)

    _tx(db.transaction())


def get_watchlist(line_user_id: str) -> list[str]:
    col = db.collection("users").document(line_user_id).collection("watchlist")
    return [doc.id for doc in col.stream()]


def get_users_watching(code: str) -> list[str]:
    code = str(code)
    col = db.collection("watchlist_codes").document(code).collection("users")
    # isActive を見たい場合：ここで users/{id} を参照する必要があるが重くなるので、
    # まずはwatchlistから削除されたらここも消える運用前提でOK
    return [doc.id for doc in col.stream()]


def is_watched_by_anyone(code: str) -> bool:
    code = str(code)
    doc = db.collection("watchlist_codes").document(code).get()
    if not doc.exists:
        return False
    n = (doc.to_dict() or {}).get("userCount", 0) or 0
    return n > 0

def get_all_watched_codes(limit: int = 5000) -> list[str]:
    q = db.collection("watchlist_codes").where("userCount", ">", 0).limit(limit)
    return [doc.id for doc in q.stream()]


# =========================
# Disclosures / Summary (要約は1回だけ)
# =========================

def mark_disclosure_if_new(disclosure_id: str, data: dict) -> bool:
    ref = db.collection("disclosures").document(disclosure_id)

    payload = dict(data or {})
    payload["disclosureId"] = disclosure_id
    payload["status"] = "discovered"
    payload["createdAt"] = firestore.SERVER_TIMESTAMP
    payload["updatedAt"] = firestore.SERVER_TIMESTAMP

    try:
        ref.create(payload)
        return True
    except AlreadyExists:
        try:
            ref.set({"updatedAt": firestore.SERVER_TIMESTAMP}, merge=True)
        except Exception:
            pass
        return False

def get_disclosure(disclosure_id: str) -> dict | None:
    doc = db.collection("disclosures").document(disclosure_id).get()
    if not doc.exists:
        return None
    return doc.to_dict() or {}


def upsert_disclosure(disclosure_id: str, data: dict):
    db.collection("disclosures").document(disclosure_id).set(
        {**data, "updatedAt": firestore.SERVER_TIMESTAMP},
        merge=True,
    )


def has_summary(disclosure_id: str) -> bool:
    doc = db.collection("disclosures").document(disclosure_id).get()
    if not doc.exists:
        return False
    cur = doc.to_dict() or {}
    # ✅ 新方式
    if cur.get("summaryDone") is True:
        return True
    # ✅ 互換：過去データ救済（keyPoints/outlook があれば要約済み扱い）
    if cur.get("keyPoints") and cur.get("outlook") is not None:
        return True
    # （古い方式が残ってる場合）
    if cur.get("summary"):
        return True
    return False


def try_lock_disclosure(disclosure_id: str, ttl_seconds: int = 180) -> bool:
    """
    同時実行で二重要約しないためのロック（TTL付き）
    processing=true の間は要約処理させない
    """
    ref = db.collection("disclosures").document(disclosure_id)

    @firestore.transactional
    def _tx(tx: firestore.Transaction):
        snap = ref.get(transaction=tx)
        cur = snap.to_dict() if snap.exists else {}

        # ✅ 既に要約済みならロック不要
        if cur.get("summaryDone") is True:
            return False
        if cur.get("keyPoints") and cur.get("outlook") is not None:
            return False
        if cur.get("summary"):
            return False
        
        if cur.get("processing") is True:
            ts = cur.get("processingAt")
            if ts:
                now = datetime.now(timezone.utc)
                locked_at = ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts
                if now - locked_at < timedelta(seconds=ttl_seconds):
                    return False
            else:
                return False

        tx.set(
            ref,
            {"processing": True, "processingAt": firestore.SERVER_TIMESTAMP},
            merge=True,
        )
        return True

    return _tx(db.transaction())


def unlock_disclosure(disclosure_id: str):
    db.collection("disclosures").document(disclosure_id).set(
        {"processing": False, "processingAt": firestore.DELETE_FIELD},
        merge=True,
    )


# =========================
# Delivery (ユーザーごとに1回だけ送る)
# =========================

def try_lock_delivery(disclosure_id: str, line_user_id: str, ttl_seconds: int = 180) -> bool:
    doc_id = f"{disclosure_id}_{line_user_id}"
    ref = db.collection("deliveries").document(doc_id)

    @firestore.transactional
    def _tx(tx: firestore.Transaction):
        snap = ref.get(transaction=tx)
        cur = snap.to_dict() if snap.exists else {}

        if cur.get("status") == "sent":
            return False

        # ✅ 諦めフラグが立ってたら送らない
        if cur.get("giveUp") is True:
            return False

        if cur.get("status") == "sending":
            ts = cur.get("lockedAt")
            if ts:
                now = datetime.now(timezone.utc)
                locked_at = ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts
                if now - locked_at < timedelta(seconds=ttl_seconds):
                    return False
            else:
                return False

        attempts = int(cur.get("attempts") or 0)

        # ✅ attempts上限：これ以上は諦める
        if attempts >= DELIVERY_MAX_ATTEMPTS:
            tx.set(ref, {
                "disclosureId": disclosure_id,
                "lineUserId": line_user_id,
                "status": "failed",
                "giveUp": True,
                "lastError": f"attempts_exceeded(max={DELIVERY_MAX_ATTEMPTS})",
                "updatedAt": firestore.SERVER_TIMESTAMP,
            }, merge=True)
            return False

        tx.set(ref, {
            "disclosureId": disclosure_id,
            "lineUserId": line_user_id,
            "status": "sending",
            "attempts": attempts + 1,
            "lockedAt": firestore.SERVER_TIMESTAMP,
            "updatedAt": firestore.SERVER_TIMESTAMP,
            "giveUp": firestore.DELETE_FIELD,  # もし過去に諦めてたら解除
        }, merge=True)
        return True

    return _tx(db.transaction())

def finalize_delivery_sent(disclosure_id: str, line_user_id: str):
    doc_id = f"{disclosure_id}_{line_user_id}"
    ref = db.collection("deliveries").document(doc_id)
    ref.set({
        "status": "sent",
        "sentAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
        "lastError": firestore.DELETE_FIELD,
        "giveUp": firestore.DELETE_FIELD,
    }, merge=True)


def finalize_delivery_failed(disclosure_id: str, line_user_id: str, error: str):
    doc_id = f"{disclosure_id}_{line_user_id}"
    ref = db.collection("deliveries").document(doc_id)
    ref.set({
        "status": "failed",
        "lastError": (error or "")[:500],
        "updatedAt": firestore.SERVER_TIMESTAMP,
    }, merge=True)

# =========================
# Cron lock
# =========================
def try_lock_cron(name: str = "check", ttl_seconds: int = 90) -> bool:
    """
    cron の多重起動を防ぐためのロック
    - check は毎分運用向けに短め
    - recover は呼び出し側で長めTTLを指定する
    """
    ref = db.collection("cronLocks").document(name)

    @firestore.transactional
    def _tx(tx: firestore.Transaction):
        snap = ref.get(transaction=tx)
        cur = snap.to_dict() if snap.exists else {}

        if cur.get("locked") is True:
            ts = cur.get("lockedAt")
            if ts:
                now = datetime.now(timezone.utc)
                locked_at = ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts
                if now - locked_at < timedelta(seconds=ttl_seconds):
                    return False
            else:
                return False

        tx.set(ref, {"locked": True, "lockedAt": firestore.SERVER_TIMESTAMP}, merge=True)
        return True

    return _tx(db.transaction())


def unlock_cron(name: str = "check"):
    db.collection("cronLocks").document(name).set(
        {"locked": False, "lockedAt": firestore.DELETE_FIELD},
        merge=True,
    )

