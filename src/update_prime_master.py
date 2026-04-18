# tools/update_prime_master.py
import io
import re
import requests
import pandas as pd
from google.cloud import firestore

JPX_LIST_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
UA = {"User-Agent": "kessan-yoyaku-ai/1.0"}

def _normalize_code(x) -> str | None:
    if x is None:
        return None
    s = str(x).strip()
    m = re.search(r"(\d{4})", s)
    return m.group(1) if m else None

def _load_jpx_issue_list_df() -> pd.DataFrame:
    r = requests.get(JPX_LIST_URL, timeout=30, headers=UA)
    r.raise_for_status()
    content = r.content

    # 1) まず Excel として読む（うまくいけば一番楽）
    try:
        df = pd.read_excel(io.BytesIO(content))
        return df
    except Exception:
        pass

    # 2) ダメなら HTML として読む（.xlsに見せたHTMLテーブル対策）
    tables = pd.read_html(content)
    if not tables:
        raise RuntimeError("No tables found in JPX issue list file.")
    return tables[0]

def main():
    df = _load_jpx_issue_list_df()

    # 列名はJPX側の変更で揺れるので “それっぽい列” を探す
    cols = {c: str(c) for c in df.columns}
    # 例：コード/銘柄コード/Code など
    code_col = next((c for c in df.columns if "コード" in str(c) or "Code" in str(c)), None)
    market_col = next((c for c in df.columns if "市場" in str(c) or "Market" in str(c)), None)
    name_col = next((c for c in df.columns if "銘柄" in str(c) or "名称" in str(c) or "Name" in str(c)), None)

    if not code_col or not market_col:
        raise RuntimeError(f"Required columns not found. columns={list(df.columns)}")

    db = firestore.Client()
    batch = db.batch()
    n = 0

    for _, row in df.iterrows():
        code = _normalize_code(row.get(code_col))
        if not code:
            continue

        market = str(row.get(market_col, "")).strip()
        name = str(row.get(name_col, "")).strip() if name_col else ""

        # 「プライム（内国株式）」などを含むので、Prime判定は contains でOK
        market_segment = "Prime" if "プライム" in market or "Prime" in market else (
            "Standard" if "スタンダード" in market or "Standard" in market else (
                "Growth" if "グロース" in market or "Growth" in market else "Other"
            )
        )

        ref = db.collection("stocks").document(code)
        batch.set(ref, {
            "code": code,
            "name": name,
            "marketRaw": market,
            "marketSegment": market_segment,
            "updatedAt": firestore.SERVER_TIMESTAMP,
            "source": "jpx_issue_list",
        }, merge=True)
        n += 1

        # Firestore batch は上限があるので分割
        if n % 400 == 0:
            batch.commit()
            batch = db.batch()

    batch.commit()
    print(f"OK: upserted ~{n} rows into stocks/*")

if __name__ == "__main__":
    main()