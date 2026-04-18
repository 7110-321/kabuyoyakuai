def fmt_yoy_line(label: str, yoy: float | None, *, pos_word: str, neg_word: str) -> str:
    """
    yoy: 前年同期比（%）
    表記例:
      増益 +10.7%
      減益 ▲10.7%
      不明
    """
    if yoy is None:
        return f"{label}：不明"
    is_pos = yoy >= 0
    word = pos_word if is_pos else neg_word
    pct = abs(yoy)
    sign = "+" if is_pos else "▲"
    return f"{label}：{word} {sign}{pct:.1f}%"


def fmt_profit_line(profit_label: str, yoy: float | None) -> str:
    return fmt_yoy_line(profit_label, yoy, pos_word="増益", neg_word="減益")


def fmt_sales_line(sales_label: str, yoy: float | None) -> str:
    return fmt_yoy_line(sales_label, yoy, pos_word="増収", neg_word="減収")


def valuation_tag(per: float | None, pbr: float | None, outlook: str) -> tuple[str, str]:
    # MVP用の超簡易ロジック（後で改善）
    if per is not None or pbr is not None:
        if per is not None and per <= 12 and outlook == "上方":
            return "割安寄り", f"PER {per:.1f}倍"
        if per is not None and per >= 25 and outlook == "下方":
            return "割高寄り", f"PER {per:.1f}倍"
        if per is not None and pbr is not None:
            return "妥当", f"PER {per:.1f}倍 / PBR {pbr:.1f}倍"
        if per is not None:
            return "妥当", f"PER {per:.1f}倍"
        return "妥当", f"PBR {pbr:.1f}倍"

    if outlook == "上方":
        return "割安寄り", "上方修正"
    if outlook == "下方":
        return "割高寄り", "下方修正"
    return "妥当", "見通し据え置き/不明"


def build_line_message(d: dict) -> str:
    # 決算短信だけ通知（それ以外は弾く）
    if d.get("category") not in (None, "", "決算短信", "決算"):
        return ""

    company = d.get("companyName", "不明")
    code = d.get("code", "----")
    url = d.get("url", "")

    outlook = d.get("outlook", "不明")

    # --- YoY 指標（%） ---
    # 後方互換：従来は yoyPct(=営業利益YoY想定) だけ
    sales_yoy = d.get("salesYoyPct")
    op_yoy = d.get("opProfitYoyPct") if d.get("opProfitYoyPct") is not None else d.get("yoyPct")
    ordinary_yoy = d.get("ordinaryYoyPct")

    # --- ラベル ---
    sales_label = d.get("salesLabel") or "売上高"
    op_label = d.get("opProfitLabel") or d.get("profitLabel") or "営業利益"
    ordinary_label = d.get("ordinaryLabel") or "経常利益"

    sales_line = fmt_sales_line(sales_label, sales_yoy)
    op_line = fmt_profit_line(op_label, op_yoy)
    ordinary_line = fmt_profit_line(ordinary_label, ordinary_yoy)

    points = (d.get("keyPoints") or [])[:3]
    if not points:
        points = ["要点は取得できませんでした"]

    per = d.get("per")
    pbr = d.get("pbr")

    msg: list[str] = []
    msg.append(f"【決算】{company}（{code}）")
    msg.append(sales_line)
    msg.append(op_line)
    msg.append(ordinary_line)
    msg.append(f"見通し：{outlook}")
    msg.append("")
    msg.append("要点")
    for p in points:
        msg.append(f"・{p}")
    msg.append("")
    if url:
        msg.append(f"資料：{url}")

    return "\n".join(msg)