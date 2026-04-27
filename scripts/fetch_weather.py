#!/usr/bin/env python
"""fetch_weather.py

タスクリスト上の `scripts/fetch_weather.py` 互換ラッパー。
内部で `download_weather_data.py` の main() を呼び出すだけ。

例:
    python scripts/fetch_weather.py --start 2015 --out data/raw/weather/
（現在は引数を無視し、既定の設定で動作）
"""

import argparse
import runpy
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Fetch weather data (wrapper)")
    parser.add_argument("--start", type=int, help="開始年", default=2013)
    parser.add_argument("--out", type=str, help="出力ディレクトリ", default="weather_data/")
    _ = parser.parse_args()  # 現状は download_weather_data.py 側で固定

    # run the script in a new namespace (equivalent to python download_weather_data.py)
    runpy.run_path(str(Path(__file__).with_name("download_weather_data.py")))


if __name__ == "__main__":
    main() 