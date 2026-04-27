"""download_search_trends.py
Google Trends から都道府県別検索量を週次で取得する。
pytrends を利用し、対象キーワードは以下。
    ・インフルエンザ
    ・風邪
    ・熱
    ・咳
出力は `search_trends_data/search_trends_{prefecture_code}.csv`

注意:
1. Google Trends API は 1 リクエストで最大 5 年間・週単位データが取得可能。
   本スクリプトでは 2013-01-01〜2025-12-31 を 5 年刻みで分割して取得。
2. 地域指定は `geo='JP-XX'` (例: 北海道 `JP-01`).
3. Google のレートリミットを避けるため、リクエスト毎に 1.5 秒スリープ。
"""

from datetime import datetime
from pathlib import Path
import time
import argparse

import pandas as pd
from pytrends.request import TrendReq

# ---- パラメータ設定 ---------------------------------------------------------
START = datetime(2013, 1, 1)
END = datetime(2025, 12, 31)
WIN_YEARS = 5  # 5 年刻みで取得

KEYWORDS = ["インフルエンザ", "風邪", "熱", "咳"]

# 県コード & 日本語県名
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

# ---- ディレクトリ -----------------------------------------------------------
OUT_DIR = Path("search_trends_data")
OUT_DIR.mkdir(exist_ok=True)

# ---- pytrends セッション -----------------------------------------------------
pytrends = TrendReq(hl="ja-JP", tz=540)  # JST=UTC+9

SLEEP_SEC = 1.5  # will be overridden by CLI


def daterange_chunks(start: datetime, end: datetime, years: int = 5):
    """start から end まで years 年刻みで (start_chunk, end_chunk) を yield"""
    cur_start = start
    while cur_start <= end:
        cur_end = min(datetime(cur_start.year + years, cur_start.month, cur_start.day) - pd.Timedelta(days=1), end)
        yield cur_start, cur_end
        cur_start = cur_end + pd.Timedelta(days=1)


def fetch_trend(pref_code: str) -> pd.DataFrame:
    """都道府県コード 2 桁の検索トレンドを取得し週次 DataFrame を返す"""
    geo = f"JP-{pref_code}"
    frames = []

    for s, e in daterange_chunks(START, END, WIN_YEARS):
        timeframe = f"{s.strftime('%Y-%m-%d')} {e.strftime('%Y-%m-%d')}"
        try:
            pytrends.build_payload(KEYWORDS, timeframe=timeframe, geo=geo, cat=0)
            df = pytrends.interest_over_time()
            if df.empty:
                print(f"[WARN] Empty trends for {geo} {timeframe}")
                continue
            df = df.drop(columns=["isPartial"], errors="ignore")
            frames.append(df)
        except Exception as exc:
            print(f"[ERR] pytrends failed for {geo} {timeframe}: {exc}")
            time.sleep(5)
            continue
        time.sleep(SLEEP_SEC)

    if not frames:
        return pd.DataFrame()

    full_df = pd.concat(frames)
    full_df = full_df.sort_index().reset_index().rename(columns={"date": "date"})
    full_df["prefecture_code"] = pref_code
    full_df["prefecture"] = PREFS[pref_code]

    # 週始まりを ISO 月曜 (pytrends は日曜基準) に合わせるため、
    # ここではそのまま使用し preprocess 側でマージ時に left join する
    return full_df


def parse_args():
    parser = argparse.ArgumentParser(description="Download Google Trends data for Japanese prefectures.")
    parser.add_argument("--only", nargs="*", help="Prefecture codes (01-47) to fetch. If omitted, fetch all.")
    parser.add_argument("--sleep", type=float, default=1.5, help="Seconds to sleep between Google requests (default 1.5s)")
    return parser.parse_args()


def main():
    args = parse_args()

    global SLEEP_SEC
    SLEEP_SEC = args.sleep

    target_codes = args.only if args.only else PREFS.keys()

    for code in target_codes:
        name = PREFS.get(code)
        if name is None:
            print(f"[WARN] Unknown prefecture code {code}; skip")
            continue
        out_file = OUT_DIR / f"search_trends_{code}.csv"
        if out_file.exists():
            print(f"[SKIP] {out_file} already exists")
            continue

        print(f"[INFO] Fetching trends for {name} ({code}) ...")
        df = fetch_trend(code)
        if df.empty:
            print(f"[WARN] No trend data for {name}")
            continue
        df.to_csv(out_file, index=False)
        print(f"[OK] Saved {len(df)} rows → {out_file}")


if __name__ == "__main__":
    main() 