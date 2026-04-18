# src/llm.py
import json
import os
import re
import httpx
from openai import OpenAI

MODEL = os.getenv("SUMMARY_MODEL", "gpt-4o-mini")
SUMMARY_VERSION = os.getenv("SUMMARY_VERSION", "v1")

_client_singleton: OpenAI | None = None


def _client() -> OpenAI:
    global _client_singleton
    if _client_singleton is not None:
        return _client_singleton

    key = os.getenv("OPENAI_API_KEY")
    if not key:
        raise RuntimeError("OPENAI_API_KEY is not set")

    _client_singleton = OpenAI(
        api_key=key,
        http_client=httpx.Client(timeout=60.0),
    )
    return _client_singleton


def _safe_json_loads(s: str) -> dict:
    s = (s or "").strip()
    if not s:
        return {}
    try:
        return json.loads(s)
    except Exception:
        m = re.search(r"\{.*\}", s, flags=re.DOTALL)
        if not m:
            return {}
        return json.loads(m.group(0))


def _norm_code(code: str) -> str:
    s = (code or "").strip().upper()

    m = re.fullmatch(r"([0-9]{4})0", s)
    if m:
        return m.group(1)

    m = re.fullmatch(r"([0-9]{4}[A-Z]?)", s)
    if m:
        return m.group(1)

    m = re.search(r"\b([0-9]{4})0\b", s)
    if m:
        return m.group(1)

    m = re.search(r"\b([0-9]{4}[A-Z]?)\b", s)
    return m.group(1) if m else s


def _to_float_or_none(x):
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def summarize_kessan_text(text: str, company: str, code: str, title: str) -> dict:
    sys = (
        "あなたは日本株の決算短信を要約するアシスタント。"
        "推測は禁止。根拠は入力テキストのみ。"
        "出力は必ずJSONのみ（前後に文章を付けない）。"
    )

    company_in = (company or "不明").strip()
    code_in = _norm_code(code or "----") or "----"

    user = f"""
【会社名】{company_in}
【証券コード】{code_in}
【タイトル】{title}

以下は決算短信PDFから抽出したテキストです（抜粋）。
この範囲内のみを根拠に、次のJSON形式“だけ”で出力してください。

{{
  "companyName": string,          // TEXT内に会社名があればそれ。無ければ入力の会社名
  "code": string,                 // TEXT内に4桁コードがあればそれ。無ければ入力のコード

  // 既存互換（あなたが「代表利益」として選ぶもの）
  "profitLabel": "営業利益|経常利益|純利益",
  "yoyPct": number|null,          // profitLabelに対応する前年同期比%

  // 追加（3指標）
  "salesYoyPct": number|null,     // 売上高の前年同期比%（増収=正、減収=負）
  "opProfitYoyPct": number|null,  // 営業利益の前年同期比%（増益=正、減益=負）
  "ordinaryYoyPct": number|null,  // 経常利益の前年同期比%（増益=正、減益=負）

  "outlook": "上方|下方|不明",
  "keyPoints": ["...", "...", "...", "...", "..."]  // 最大5つ
}}

ルール：
- companyName/code: 推測は禁止。TEXTに明記がある場合のみ抽出。無ければ入力値をそのまま使う。
- yoyPct/profitLabel: 原則「営業利益」。無ければ「経常利益」→「純利益」。yoyPctは選んだ利益の前年同期比%。
- salesYoyPct/opProfitYoyPct/ordinaryYoyPct:
  - それぞれ該当する「前年同期比%」をTEXTから抽出できる場合のみ入れる。無ければnull。
  - 増加は正、減少は負。マイナス表記(▲や△)があれば負として扱う。
  - 「前年差」「前年同期比」「対前年同期比」「YoY」など表現揺れは同義として扱う。
- keyPointsには「売上/利益が増減した」だけの一般論を並べない。必ず“理由（要因）”か“一過性要因”か“見通し修正理由”を含める（TEXTにある場合）。
- outlook: 上方/下方/不明

keyPoints（最重要）：
- 事実のみ（TEXT根拠）。最大5つ。端的に「何が要因でどうなったか」が分かる形にする。
- 次の優先順位で抽出する（TEXTにあるものだけ）：
  1) 増減益/増減収の主因（需要/数量/価格/為替/生産性/コスト等）
  2) 一過性要因（株式売却益など特別損益、減損、補助金等）
  3) 見通し修正の理由（上方/下方の根拠）
  4) 事業/セグメント別の強弱
  5) その他重要事項（配当、自社株買い、投資など）

--- TEXT START ---
{text}
--- TEXT END ---
""".strip()

    resp = _client().responses.create(
        model=MODEL,
        instructions=sys,
        input=user,
        text={
            "format": {
                "type": "json_schema",
                "name": "kessan_summary",
                "strict": True,
                "schema": {
                    "type": "object",
                    "properties": {
                        "companyName": {"type": "string"},
                        "code": {"type": "string"},
                        "profitLabel": {
                            "type": "string",
                            "enum": ["営業利益", "経常利益", "純利益"],
                        },
                        "yoyPct": {"type": ["number", "null"]},

                        # 追加3指標
                        "salesYoyPct": {"type": ["number", "null"]},
                        "opProfitYoyPct": {"type": ["number", "null"]},
                        "ordinaryYoyPct": {"type": ["number", "null"]},

                        "outlook": {"type": "string", "enum": ["上方", "下方", "不明"]},
                        "keyPoints": {
                            "type": "array",
                            "items": {"type": "string"},
                            "maxItems": 5,
                        },
                    },
                    "required": [
                        "companyName",
                        "code",
                        "profitLabel",
                        "yoyPct",
                        "salesYoyPct",
                        "opProfitYoyPct",
                        "ordinaryYoyPct",
                        "outlook",
                        "keyPoints",
                    ],
                    "additionalProperties": False,
                },
            }
        },
    )

    content = (getattr(resp, "output_text", "") or "").strip()
    data = _safe_json_loads(content)

    # ---- 整形 ----
    company_out = (data.get("companyName") or company_in).strip() or "不明"
    code_out = _norm_code(data.get("code") or code_in) or "----"

    profit = data.get("profitLabel") or "営業利益"
    if profit not in ("営業利益", "経常利益", "純利益"):
        profit = "営業利益"

    outlook = data.get("outlook") or "不明"
    if outlook not in ("上方", "下方", "不明"):
        outlook = "不明"

    key_points = data.get("keyPoints") or []
    if not isinstance(key_points, list):
        key_points = []
    key_points = [str(x).strip() for x in key_points if str(x).strip()][:5]

    yoy = _to_float_or_none(data.get("yoyPct"))

    sales_yoy = _to_float_or_none(data.get("salesYoyPct"))
    op_yoy = _to_float_or_none(data.get("opProfitYoyPct"))
    ordinary_yoy = _to_float_or_none(data.get("ordinaryYoyPct"))

    # 後方互換の保険：opProfitが取れてないがyoyPctはある → 営業利益扱いに寄せる
    if op_yoy is None and yoy is not None and profit == "営業利益":
        op_yoy = yoy

    return {
        "companyName": company_out,
        "code": code_out,

        # 既存互換
        "profitLabel": profit,
        "yoyPct": yoy,

        # 追加3指標
        "salesYoyPct": sales_yoy,
        "opProfitYoyPct": op_yoy,
        "ordinaryYoyPct": ordinary_yoy,

        "outlook": outlook,
        "keyPoints": key_points,
        "summaryModel": MODEL,
        "summaryVersion": SUMMARY_VERSION,
    }

_REASON_KEYWORDS = [
    "ため",
    "により",
    "による",
    "影響",
    "背景",
    "反動",
    "寄与",
    "奏功",
    "増加",
    "減少",
    "上昇",
    "下落",
    "高騰",
    "価格改定",
    "値上げ",
    "値下げ",
    "コスト",
    "原材料",
    "仕入",
    "物流費",
    "人件費",
    "燃料費",
    "電力",
    "円安",
    "円高",
    "為替",
    "補助金",
    "助成金",
    "減損",
    "評価益",
    "評価損",
    "売却益",
    "特別利益",
    "特別損失",
    "セグメント",
    "既存店",
    "客数",
    "客単価",
    "出店",
    "退店",
    "PB商品",
    "メディア",
    "集客",
]

_WEAK_PATTERNS = [
    r"売上高は.+?(増収|減収).+?$",
    r"営業利益は.+?(増益|減益).+?$",
    r"経常利益は.+?(増益|減益).+?$",
    r"純利益は.+?(増益|減益).+?$",
]

def _normalize_point(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _is_reason_rich_point(s: str) -> bool:
    t = _normalize_point(s)
    if not t:
        return False
    if any(k in t for k in _REASON_KEYWORDS):
        return True
    return False

def _is_weak_point(s: str) -> bool:
    t = _normalize_point(s)
    if not t:
        return True
    if _is_reason_rich_point(t):
        return False
    return any(re.search(p, t) for p in _WEAK_PATTERNS)

def _extract_reason_sentences(text: str, limit: int = 8) -> list[str]:
    raw = (text or "").replace("\u3000", " ")
    raw = re.sub(r"[ \t]+", " ", raw)
    raw = raw.replace("。", "。\n")
    lines = [x.strip(" ・\t") for x in raw.splitlines() if x.strip()]

    candidates: list[str] = []
    for line in lines:
        if len(line) < 18:
            continue
        if len(line) > 140:
            continue
        if any(k in line for k in _REASON_KEYWORDS):
            candidates.append(line)

    # 重複除去
    out: list[str] = []
    seen: set[str] = set()
    for c in candidates:
        key = re.sub(r"\s+", "", c)
        if key in seen:
            continue
        seen.add(key)
        out.append(c)
        if len(out) >= limit:
            break
    return out

def enrich_key_points_from_text(text: str, key_points: list[str], limit: int = 5) -> list[str]:
    current = [_normalize_point(x) for x in (key_points or []) if _normalize_point(x)]

    strong = [p for p in current if not _is_weak_point(p)]
    if len(strong) >= min(3, limit):
        return strong[:limit]

    fallback = _extract_reason_sentences(text, limit=limit * 2)

    merged: list[str] = []
    seen: set[str] = set()

    for p in strong + fallback + current:
        p2 = _normalize_point(p)
        if not p2:
            continue
        key = re.sub(r"\s+", "", p2)
        if key in seen:
            continue
        seen.add(key)
        merged.append(p2)
        if len(merged) >= limit:
            break

    return merged[:limit]