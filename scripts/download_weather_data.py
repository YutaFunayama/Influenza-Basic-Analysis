"""download_weather_data.py
実際に気象データを取得し週次集計するスクリプト。
Meteostat API を利用し、都道府県庁所在地付近の観測所データ（日別）を取得して
・平均気温 (℃) : tavg の週平均
・相対湿度 (%)  : rhum の週平均（欠測の場合は NaN）
・降水量 (mm)   : prcp の週合計
を計算し CSV として保存する。
"""

from datetime import datetime
from pathlib import Path
import time

import pandas as pd
from meteostat import Daily, Point

# ---- 県コード → 緯度経度マップ -------------------------------------------
PREF_LOC = {
    "01": ("北海道", 43.06417, 141.34694),
    "02": ("青森県", 40.82444, 140.74),
    "03": ("岩手県", 39.70361, 141.1525),
    "04": ("宮城県", 38.26889, 140.87194),
    "05": ("秋田県", 39.71861, 140.1025),
    "06": ("山形県", 38.24056, 140.36333),
    "07": ("福島県", 37.75, 140.46778),
    "08": ("茨城県", 36.34139, 140.44667),
    "09": ("栃木県", 36.56583, 139.88361),
    "10": ("群馬県", 36.39111, 139.06083),
    "11": ("埼玉県", 35.85694, 139.64889),
    "12": ("千葉県", 35.60472, 140.12333),
    "13": ("東京都", 35.68944, 139.69167),
    "14": ("神奈川県", 35.44778, 139.6425),
    "15": ("新潟県", 37.90222, 139.02361),
    "16": ("富山県", 36.69528, 137.21139),
    "17": ("石川県", 36.59444, 136.62556),
    "18": ("福井県", 36.06528, 136.22194),
    "19": ("山梨県", 35.66389, 138.56833),
    "20": ("長野県", 36.65139, 138.18111),
    "21": ("岐阜県", 35.39111, 136.72222),
    "22": ("静岡県", 34.97694, 138.38306),
    "23": ("愛知県", 35.18028, 136.90667),
    "24": ("三重県", 34.73028, 136.50861),
    "25": ("滋賀県", 35.00444, 135.86833),
    "26": ("京都府", 35.02139, 135.75556),
    "27": ("大阪府", 34.68639, 135.52),
    "28": ("兵庫県", 34.69139, 135.18306),
    "29": ("奈良県", 34.68528, 135.83278),
    "30": ("和歌山県", 34.22611, 135.1675),
    "31": ("鳥取県", 35.50361, 134.23833),
    "32": ("島根県", 35.47222, 133.05056),
    "33": ("岡山県", 34.66167, 133.935),
    "34": ("広島県", 34.39639, 132.45944),
    "35": ("山口県", 34.18583, 131.47139),
    "36": ("徳島県", 34.06583, 134.55944),
    "37": ("香川県", 34.34028, 134.04333),
    "38": ("愛媛県", 33.84167, 132.76611),
    "39": ("高知県", 33.55972, 133.53111),
    "40": ("福岡県", 33.60639, 130.41806),
    "41": ("佐賀県", 33.24944, 130.29889),
    "42": ("長崎県", 32.74472, 129.87361),
    "43": ("熊本県", 32.78972, 130.74167),
    "44": ("大分県", 33.23806, 131.6125),
    "45": ("宮崎県", 31.91111, 131.42389),
    "46": ("鹿児島県", 31.56028, 130.55806),
    "47": ("沖縄県", 26.2125, 127.68111),
}

# ---- 期間設定 ------------------------------------------------------------
START = datetime(2013, 1, 1)
END = datetime(2025, 12, 31)

# ---- 出力ディレクトリ ------------------------------------------------------
OUT_DIR = Path("weather_data")
OUT_DIR.mkdir(exist_ok=True)


def fetch_prefecture_weather(code: str, name: str, lat: float, lon: float) -> pd.DataFrame:
    """指定座標の日別 Meteostat データを取得して週次集計 DataFrame を返す"""
    # 代表地点生成
    point = Point(lat, lon)
    daily = Daily(point, START, END)
    df_daily = daily.fetch()
    if df_daily.empty:
        print(f"[WARN] Meteostat returned empty data for pref {code}")
        return pd.DataFrame()

    # 列の存在チェックと欠損処理
    for col in ["tavg", "rhum", "prcp"]:
        if col not in df_daily.columns:
            df_daily[col] = pd.NA

    # 週次（月曜始まり）へ集計
    df_weekly = df_daily.resample("W-MON", label="left", closed="left").agg({
        "tavg": "mean",
        "rhum": "mean",
        "prcp": "sum",
    })

    df_weekly = df_weekly.rename(columns={
        "tavg": "temperature",
        "rhum": "humidity",
        "prcp": "precipitation",
    })
    df_weekly["prefecture_code"] = code
    df_weekly["prefecture"] = name
    df_weekly = df_weekly.reset_index().rename(columns={"time": "date"})

    # ISO 8601 形式の日付文字列に変更（統合処理側に合わせる）
    df_weekly["date"] = df_weekly["date"].dt.strftime("%Y-%m-%d")

    return df_weekly


def main():
    for code, (name, lat, lon) in PREF_LOC.items():
        out_file = OUT_DIR / f"weather_{code}.csv"
        if out_file.exists():
            print(f"[SKIP] {out_file} already exists")
            continue

        print(f"[INFO] Fetching weather for pref {code}...")
        df_week = fetch_prefecture_weather(code, name, lat, lon)
        if df_week.empty:
            print(f"[WARN] No data for pref {code}")
        else:
            df_week.to_csv(out_file, index=False)
            print(f"[OK] Saved {len(df_week)} weekly rows → {out_file}")

        # polite sleep to avoid overloading the API servers
        time.sleep(1)


if __name__ == "__main__":
    main() 