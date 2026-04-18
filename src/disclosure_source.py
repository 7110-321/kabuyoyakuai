# src/disclosure_source.py
import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
import requests

TDNET_RSS_URL = "https://webapi.yanoshin.jp/webapi/tdnet/list/recent.rss"
TDNET_BASE = "https://webapi.yanoshin.jp/webapi/tdnet/list"

# 通知対象は決算短信だけに絞る
KEYWORDS = ["決算短信", "四半期決算短信"]

def _parse_pub(pub_date_text: str):
    try:
        return datetime.strptime(pub_date_text, "%a, %d %b %Y %H:%M:%S %z")
    except Exception:
        return None

def _to_iso(pub_date_text: str) -> str:
    dt = _parse_pub(pub_date_text)
    return dt.isoformat() if dt else ""

def _chunks_by_size(xs: list[str], n: int):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

def _detect_category(title: str) -> str:
    t = title or ""
    if "決算短信" in t:
        return "決算短信"
    return "その他"

def fetch_disclosures_by_codes(
    codes: list[str],
    per_feed: int = 1,
    limit_per_feed: int = 10,
    lookback_hours: int = 48,
) -> list[dict]:
    codes = [str(c).strip().upper() for c in codes if str(c).strip()]
    if not codes:
        return []

    out: list[dict] = []
    seen: set[str] = set()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)

    for group in _chunks_by_size(codes, per_feed):
        url = f"{TDNET_BASE}/{'-'.join(group)}.rss?limit={limit_per_feed}"

        r = requests.get(url, timeout=20, headers={"User-Agent": "kessan-yoyaku-ai/1.0"})
        r.raise_for_status()

        root = ET.fromstring(r.content)
        items = root.findall(".//item")

        # per_feed=1 前提なら、この feed の item は全部この code
        feed_code = group[0] if len(group) == 1 else None

        for it in items:
            title = (it.findtext("title") or "").strip()
            link = (it.findtext("link") or "").strip()
            guid = (it.findtext("guid") or "").strip()
            pub = (it.findtext("pubDate") or "").strip()
            pub_dt = _parse_pub(pub)

            if not title or title == ":":
                continue
            if not link:
                continue
            if not any(k in title for k in KEYWORDS):
                continue
            if not pub_dt:
                continue
            if pub_dt.astimezone(timezone.utc) < cutoff:
                continue

            category = _detect_category(title)
            if category != "決算短信":
                continue

            base = guid or link or title
            disclosure_id = hashlib.sha256(base.encode("utf-8")).hexdigest()[:32]
            if disclosure_id in seen:
                continue
            seen.add(disclosure_id)

            out.append({
                "disclosureId": disclosure_id,
                "code": feed_code,
                "companyName": "",
                "title": title,
                "url": link,
                "publishedAt": pub_dt.isoformat(),
                "profitLabel": "",
                "yoyPct": None,
                "outlook": "不明",
                "keyPoints": [],
                "source": "tdnet_rss_by_codes",
                "category": category,
                "codeSource": "rss_by_codes" if feed_code else "",
            })

    return out


# 保険として残してもいいが、cron/check では使わない
def fetch_latest_disclosures(limit: int = 200) -> list[dict]:
    url = f"{TDNET_RSS_URL}?limit={limit}"
    r = requests.get(url, timeout=20, headers={"User-Agent": "kessan-yoyaku-ai/1.0"})
    r.raise_for_status()

    root = ET.fromstring(r.content)
    items = root.findall(".//item")

    out: list[dict] = []
    for it in items:
        title = (it.findtext("title") or "").strip()
        link = (it.findtext("link") or "").strip()
        guid = (it.findtext("guid") or "").strip()
        pub = (it.findtext("pubDate") or "").strip()

        if not title or title == ":":
            continue
        if not link:
            continue
        if not any(k in title for k in KEYWORDS):
            continue

        base = guid or link or title
        disclosure_id = hashlib.sha256(base.encode("utf-8")).hexdigest()[:32]

        out.append({
            "disclosureId": disclosure_id,
            "code": None,
            "companyName": "",
            "title": title,
            "url": link,
            "publishedAt": _to_iso(pub),
            "profitLabel": "",
            "yoyPct": None,
            "outlook": "不明",
            "keyPoints": [],
            "source": "tdnet_rss_mirror",
            "category": "決算短信",
        })

        if len(out) >= limit:
            break

    return out