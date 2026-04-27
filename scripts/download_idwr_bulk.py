import os
import requests
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import time
from datetime import datetime

def get_max_weeks(year):
    # 2015年・2020年は53週、それ以外は52週
    if year in [2015, 2020]:
        return 53
    else:
        return 52

def get_url(year, week):
    """URLを生成する関数"""
    if year < 2023:
        return f"https://id-info.jihs.go.jp/niid/images/idwr/sokuho/idwr-{year}/{year}{week:02d}/idwr-{year}-{week:02d}-teiten.csv"
    elif year == 2025 and week >= 11:
        return f"https://id-info.jihs.go.jp/surveillance/idwr/jp/rapid/{year}/{week}/{year}-{week:02d}-teiten.csv"
    else:
        return f"https://id-info.jihs.go.jp/surveillance/idwr/rapid/{year}/{week}/{year}-{week:02d}-teiten.csv"

def download_weekly_data(year, week, save_dir):
    """指定された年度と週のデータをダウンロードする関数"""
    try:
        # 2013年～2022年のURLパターン
        if year <= 2022:
            url = f"https://id-info.jihs.go.jp/niid/images/idwr/sokuho/idwr-{year}/{year}{week:02d}/{year}-{week:02d}-teiten.csv"
        # 2023年以降のURLパターン
        else:
            url = f"https://id-info.jihs.go.jp/surveillance/idwr/rapid/{year}/{week}/{year}-{week:02d}-teiten.csv"
        
        # 保存先のパス
        save_path = save_dir / f"{year}_{week:02d}.csv"
        
        # ファイルが既に存在する場合はスキップ
        if save_path.exists():
            return 'exists', save_path
        
        # ダウンロード
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # ファイルサイズの取得
        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024
        
        # プログレスバー付きでダウンロード
        with open(save_path, 'wb') as f, tqdm(
            desc=f"{year}年{week}週目",
            total=total_size,
            unit='iB',
            unit_scale=True,
            leave=False
        ) as pbar:
            for data in response.iter_content(block_size):
                size = f.write(data)
                pbar.update(size)
        
        return 'success', save_path
        
    except Exception as e:
        return 'error', str(e)

def main():
    # 保存先ディレクトリの設定
    base_dir = Path("idwr_data")
    csv_dir = base_dir / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)
    
    # 年度・週ごとにダウンロード
    results = []
    for year in range(2013, 2026):
        max_week = get_max_weeks(year)
        print(f"\n==== {year}年（{max_week}週） ====")
        for week in range(1, max_week+1):
            status, info = download_weekly_data(year, week, csv_dir)
            if status == 'success':
                print(f"[OK] {year}年{week:02d}週: {info.name}")
            elif status == 'exists':
                print(f"[SKIP] {year}年{week:02d}週: 既に存在")
            else:
                print(f"[ERR] {year}年{week:02d}週: {info}")
            time.sleep(0.5)  # サーバー負荷軽減
            results.append({'year': year, 'week': week, 'status': status, 'info': info})
    # エラーまとめ
    errors = [r for r in results if r['status'] == 'error']
    if errors:
        print(f"\nエラーが発生した週一覧:")
        for e in errors:
            print(f"{e['year']}年{e['week']:02d}週: {e['info']}")
    else:
        print("\n全てのデータが正常にダウンロードされました")

if __name__ == "__main__":
    main() 