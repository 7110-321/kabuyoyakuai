# src/pdf_extract.py
import io
import re
import time
from urllib.parse import unquote, urlparse

import requests
from pypdf import PdfReader

CODE_PATTERNS = [
    r"証券コード\s*[:：]?\s*([0-9]{4}(?:0|[A-Z])?)",
    r"[（(]\s*証券コード\s*[:：]?\s*([0-9]{4}(?:0|[A-Z])?)\s*[）)]",
]


def extract_company_code_from_text(full_text: str):
    t = full_text or ""
    t = t.replace("\u3000", " ")
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\s+\n", "\n", t)

    code = None
    patterns_code = [
        r"コ\s*ー\s*ド\s*番\s*号\s*[:：]?\s*([0-9]{4}(?:0|[A-Z])?)",
        r"証\s*券\s*コ\s*ー\s*ド\s*[:：]?\s*([0-9]{4}(?:0|[A-Z])?)",
        r"(?:コード|証券コード)\s*[:：]?\s*([0-9]{4}(?:0|[A-Z])?)",
        r"[（(]\s*([0-9]{4}(?:0|[A-Z])?)\s*[）)]",
    ]
    for p in patterns_code:
        m = re.search(p, t)
        if m:
            code = m.group(1)
            break

    company = None
    patterns_company = [
        r"上\s*場\s*会\s*社\s*名\s*(.+?)\s+上場取引所",
        r"会\s*社\s*名\s*(.+?)\s+上場取引所",
        r"上\s*場\s*会\s*社\s*名\s*(.+)",
    ]
    for p in patterns_company:
        m = re.search(p, t)
        if m:
            company = m.group(1).strip()
            break

    if code:
        m = re.fullmatch(r"([0-9]{4})0", code)
        if m:
            code = m.group(1)

    return code, company


def _normalize_url(url: str) -> str:
    u = (url or "").strip()

    if "webapi.yanoshin.jp/rd.php?" in u:
        base, q = u.split("?", 1)
        if "%2F" in q or "%3A" in q:
            q = unquote(q)
        u = f"{base}?{q}"

    u = u.rstrip("=:")
    return u


def _is_pdf_response(resp: requests.Response, url: str) -> bool:
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "application/pdf" in ctype:
        return True
    if (url or "").lower().endswith(".pdf"):
        return True
    return False


def _warmup_tdnet_session(s: requests.Session, headers: dict, timeout: int):
    warm_url = "https://www.release.tdnet.info/inbs/I_main_00.html"
    try:
        r = s.get(warm_url, timeout=timeout, headers=headers, allow_redirects=True)
        print(
            f"tdnet_warm: status={r.status_code} url={warm_url} "
            f"ct={r.headers.get('content-type', '')}"
        )
    except Exception as e:
        print(f"tdnet_warm: failed err={repr(e)}")


def _fetch_pdf_bytes(url: str, timeout: int, retries: int) -> bytes:
    """
    403対策:
    - Sessionでcookie維持
    - rd.php を先にGETしてcookie/経路を整える
    - その後、リダイレクト先(release.tdnet)へGET
    """
    url = _normalize_url(url)

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; kessan-yoyaku-ai/1.0)",
        "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        "Referer": "https://webapi.yanoshin.jp/webapi/tdnet/list/recent.html",
    }

    last_err = None
    s = requests.Session()

    for attempt in range(1, retries + 1):
        try:
            target = url

            # 1) rd.php 経由なら、まず rd.php を GET
            if "webapi.yanoshin.jp/rd.php?" in url:
                r0 = s.get(url, timeout=timeout, headers=headers, allow_redirects=False)
                print(
                    f"pdf_proxy: status={r0.status_code} url={url} "
                    f"ct={r0.headers.get('content-type', '')}"
                )

                if r0.status_code in (301, 302, 303, 307, 308):
                    loc = (r0.headers.get("Location") or "").strip()
                    if not loc:
                        raise RuntimeError("proxy redirect missing Location header")

                    target = loc
                    parsed = urlparse(target)
                    if parsed.netloc.endswith("release.tdnet.info"):
                        _warmup_tdnet_session(s, headers, timeout)
                        headers = dict(headers)
                        headers["Referer"] = "https://www.release.tdnet.info/inbs/I_main_00.html"

                elif r0.status_code == 200 and _is_pdf_response(r0, url):
                    if not r0.content or len(r0.content) < 1000:
                        ctype = (r0.headers.get("Content-Type") or "").lower()
                        raise RuntimeError(
                            f"pdf too small bytes={len(r0.content)} ctype={ctype}"
                        )
                    return r0.content

                else:
                    head = (r0.text or "")[:200]
                    raise RuntimeError(f"proxy HTTP {r0.status_code} body_head={head}")

            # 2) 本体PDFを取得
            parsed = urlparse(target)
            if parsed.netloc.endswith("release.tdnet.info"):
                headers = dict(headers)
                headers["Referer"] = "https://www.release.tdnet.info/inbs/I_main_00.html"

            r = s.get(target, timeout=timeout, headers=headers, allow_redirects=True)
            print(
                f"pdf_fetch: status={r.status_code} target={target} final={r.url} "
                f"ct={r.headers.get('content-type', '')}"
            )

            if r.status_code != 200:
                head = (r.text or "")[:200]
                raise RuntimeError(f"HTTP {r.status_code} body_head={head}")

            if not _is_pdf_response(r, target):
                ctype = (r.headers.get("Content-Type") or "").lower()
                raise RuntimeError(f"non-pdf content-type={ctype}")

            if not r.content or len(r.content) < 1000:
                ctype = (r.headers.get("Content-Type") or "").lower()
                raise RuntimeError(f"pdf too small bytes={len(r.content)} ctype={ctype}")

            return r.content

        except Exception as e:
            last_err = e
            print(f"[pdf_extract] attempt={attempt}/{retries} url={url} err={repr(e)}")
            time.sleep(0.8 * attempt)

    raise last_err or RuntimeError("pdf fetch failed")


def _summary_text_from_pdf(
    url: str,
    timeout: int = 30,
    max_pages: int = 10,
    max_chars: int = 24000,
    retries: int = 3,
) -> tuple[str, str | None, str | None]:
    pdf_bytes = _fetch_pdf_bytes(url, timeout=timeout, retries=retries)

    reader = PdfReader(io.BytesIO(pdf_bytes))
    text_parts = []
    total_chars = 0

    for i in range(min(max_pages, len(reader.pages))):
        t = reader.pages[i].extract_text() or ""
        if t:
            text_parts.append(t)
            total_chars += len(t)
        if total_chars >= max_chars:
            break

    text = "\n".join(text_parts)[:max_chars]

    code, company = extract_company_code_from_text(text)

    if not code:
        for pat in CODE_PATTERNS:
            m = re.search(pat, text)
            if m:
                code = m.group(1)
                break

    if not company and code:
        m = re.search(
            rf"^(.+?)\s*[（(]\s*証券コード\s*[:：]?\s*{re.escape(code)}",
            text,
            re.MULTILINE,
        )
        if m:
            company = m.group(1).strip()

    return text, code, company


def extract_summary_text_from_pdf(url: str):
    return _summary_text_from_pdf(url)