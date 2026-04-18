import os
import httpx
import requests
import time

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")

def _headers():
    return {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

def line_reply_text(reply_token: str, text: str) -> bool:
    if not LINE_CHANNEL_ACCESS_TOKEN:
        return False
    url = "https://api.line.me/v2/bot/message/reply"
    payload = {"replyToken": reply_token, "messages": [{"type": "text", "text": text}]}
    r = requests.post(url, headers=_headers(), json=payload, timeout=10)
    return r.status_code == 200

def line_push_text(user_id: str, text: str) -> bool:
    if not LINE_CHANNEL_ACCESS_TOKEN:
        return False
    url = "https://api.line.me/v2/bot/message/push"
    payload = {"to": user_id, "messages": [{"type": "text", "text": text}]}
    r = requests.post(url, headers=_headers(), json=payload, timeout=10)
    return r.status_code == 200


def line_broadcast_text(text: str):
    if not LINE_CHANNEL_ACCESS_TOKEN:
        return False, {"reason": "LINE_CHANNEL_ACCESS_TOKEN is empty"}

    url = "https://api.line.me/v2/bot/message/broadcast"
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {"messages": [{"type": "text", "text": text}]}

    try:
        with httpx.Client(timeout=20.0) as client:
            r = client.post(url, headers=headers, json=payload)
        ok = (200 <= r.status_code < 300)
        # LINEの返答ボディを返す（失敗理由がここに出る）
        return ok, {
            "status_code": r.status_code,
            "body": r.text[:1000],
        }
    except Exception as e:
        # 例外は投げてもいいし、ここでFalse返してもOK（とにかくdetailが欲しい）
        return False, {"reason": "exception", "error": repr(e)}
    
def line_multicast_text(user_ids: list[str], text: str) -> bool:
    """指定ユーザーにまとめて配信（最大500人/回）"""
    if not LINE_CHANNEL_ACCESS_TOKEN:
        return False
    if not user_ids:
        return True

    url = "https://api.line.me/v2/bot/message/multicast"
    payload = {
        "to": user_ids,
        "messages": [{"type": "text", "text": text}],
    }
    r = requests.post(url, headers=_headers(), json=payload, timeout=10)
    return r.status_code == 200


def _chunks(xs: list[str], n: int):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]


def line_multicast_all_text(user_ids: list[str], text: str, chunk_size: int = 500, sleep_sec: float = 0.2) -> tuple[int, int]:
    """全員に multicast で送る（500人ずつ）。戻り値=(成功チャンク数,失敗チャンク数)"""
    ok = ng = 0
    for batch in _chunks(user_ids, chunk_size):
        if line_multicast_text(batch, text):
            ok += 1
        else:
            ng += 1
        # レート制限対策で少し待つ（必要なら調整）
        time.sleep(sleep_sec)
    return ok, ng

def line_push_text_detail(user_id: str, text: str) -> tuple[bool, int, str]:
    """
    戻り値: (ok, status_code, body_snippet)
    """
    if not LINE_CHANNEL_ACCESS_TOKEN:
        return False, 0, "LINE_CHANNEL_ACCESS_TOKEN is empty"

    url = "https://api.line.me/v2/bot/message/push"
    payload = {"to": user_id, "messages": [{"type": "text", "text": text}]}

    try:
        r = requests.post(url, headers=_headers(), json=payload, timeout=10)
        body = (r.text or "")[:500]
        ok = (200 <= r.status_code < 300)
        return ok, r.status_code, body
    except Exception as e:
        return False, 0, repr(e)