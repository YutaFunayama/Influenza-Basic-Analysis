import os
import sys
import json
import re
import time
import argparse
from pathlib import Path
from typing import List, Dict
import datetime as dt

import pandas as pd
import requests

"""fetch_trends_cookie.py
Google Trends を手動 Cookie / User-Agent で取得するスクリプト。

環境変数に以下を設定してから実行してください。

    export TRENDS_UA="<User-Agent>"
    export TRENDS_COOKIE="<cookie ヘッダー全文>"

例:
    python scripts/fetch_trends_cookie.py --keyword インフルエンザ --pref all \
           --time_range "2013-01-01 2025-12-31" --out data/raw/trends/

備考:
- Google のレートリミットを避けるため、リクエスト間に数秒スリープを入れています。
- 失敗した県は retry を 3 回まで行います。
"""

# ------------------------------------------------------------
PREFS = {
    "01": "北海道", "02": "青森県", "03": "岩手県", "04": "宮城県", "05": "秋田県",
    "06": "山形県", "07": "福島県", "08": "茨城県", "09": "栃木県", "10": "群馬県",
    "11": "埼玉県", "12": "千葉県", "13": "東京都", "14": "神奈川県", "15": "新潟県",
    "16": "富山県", "17": "石川県", "18": "福井県", "19": "山梨県", "20": "長野県",
    "21": "岐阜県", "22": "静岡県", "23": "愛知県", "24": "三重県", "25": "滋賀県",
    "26": "京都府", "27": "大阪府", "28": "兵庫県", "29": "奈良県", "30": "和歌山県",
    "31": "鳥取県", "32": "島根県", "33": "岡山県", "34": "広島県", "35": "山口県",
    "36": "徳島県", "37": "香川県", "38": "愛媛県", "39": "高知県", "40": "福岡県",
    "41": "佐賀県", "42": "長崎県", "43": "熊本県", "44": "大分県", "45": "宮崎県",
    "46": "鹿児島県", "47": "沖縄県",
}

EXPLORE_URL = "https://trends.google.com/trends/api/explore"
MULTI_URL = "https://trends.google.com/trends/api/widgetdata/multiline"

HEADERS = {
    "user-agent": os.getenv("TRENDS_UA", ""),
    "cookie": os.getenv("TRENDS_COOKIE", ""),
}

if not HEADERS["user-agent"] or not HEADERS["cookie"]:
    print("[ERR] TRENDS_UA または TRENDS_COOKIE が未設定です", file=sys.stderr)
    sys.exit(1)

SEPARATOR_RE = re.compile(r"^\)\]\}',?\n")

# ------------------------------------------------------------

def call_json(url: str, params: Dict[str, str], headers: Dict[str, str], timeout: int = 30):
    """GET API and return parsed JSON (after removing XSSI prefix)"""
    resp = requests.get(url, params=params, headers=headers, timeout=timeout)
    resp.raise_for_status()
    text = SEPARATOR_RE.sub("", resp.text)
    return json.loads(text)


def fetch_prefecture(keyword: str, pref_code: str, time_range: str, hl: str = "ja", tz: str = "-540") -> pd.DataFrame:
    """指定都道府県の Trends データを DataFrame で返す"""
    geo = f"JP-{pref_code}" if pref_code else "JP"

    req_payload = {
        "comparisonItem": [{"keyword": keyword, "geo": geo, "time": time_range}],
        "category": 0,
        "property": "",
    }

    params_exp = {
        "hl": hl,
        "tz": tz,
        "req": json.dumps(req_payload, separators=(",", ":")),
    }

    explore = call_json(EXPLORE_URL, params_exp, HEADERS)
    widget_ts = next(w for w in explore["widgets"] if w["id"] == "TIMESERIES")

    params_multi = {
        "hl": hl,
        "tz": tz,
        "token": widget_ts["token"],
        "req": json.dumps(widget_ts["request"], separators=(",", ":")),
    }

    multi = call_json(MULTI_URL, params_multi, HEADERS)
    timeline = multi["default"]["timelineData"]

    # convert to DataFrame
    records = []
    for row in timeline:
        ts = dt.datetime.utcfromtimestamp(int(row["time"]))
        value = row["value"][0]
        records.append({"date": ts.strftime("%Y-%m-%d %H:%M:%S"), "hits": value})

    df = pd.DataFrame(records)
    df["prefecture_code"] = pref_code
    df["prefecture"] = PREFS.get(pref_code, "JP")
    return df

# ------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Fetch Google Trends with manual Cookie")
    p.add_argument("--keyword", default="インフルエンザ", help="検索キーワード (default: インフルエンザ)")
    p.add_argument("--pref", nargs="*", help="都道府県コード(01-47)。省略時は全県")
    p.add_argument("--time_range", default="2013-01-01 2025-12-31", help="Trends API の time パラメータ")
    p.add_argument("--sleep", type=float, default=1.5, help="リクエスト間スリープ秒")
    p.add_argument("--out", default="data/raw/trends", help="出力ディレクトリ")
    return p.parse_args()


# ------------------------------------------------------------

def main():
    args = parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    target_codes: List[str]
    if not args.pref:
        target_codes = list(PREFS.keys())
    elif len(args.pref) == 1 and args.pref[0].lower() == "all":
        target_codes = list(PREFS.keys())
    else:
        target_codes = args.pref

    for code in target_codes:
        name = PREFS.get(code, code)
        out_path = out_dir / f"trends_{args.keyword}_{code}.csv"
        if out_path.exists():
            print(f"[SKIP] {name} ({code}) already fetched")
            continue
        print(f"[INFO] Fetching {name} ({code}) …")
        retries = 3
        for attempt in range(1, retries + 1):
            try:
                df = fetch_prefecture(args.keyword, code, args.time_range)
                if df.empty:
                    print(f"[WARN] Empty data for {name}")
                else:
                    df.to_csv(out_path, index=False)
                    print(f"[OK] saved → {out_path} ({len(df)} rows)")
                break  # success → exit retry loop
            except Exception as exc:
                print(f"[ERR] {name} attempt {attempt}/{retries}: {exc}")
                if attempt == retries:
                    print(f"[FAIL] giving up {name}")
                else:
                    time.sleep(5)
        time.sleep(args.sleep)


if __name__ == "__main__":
    main() 