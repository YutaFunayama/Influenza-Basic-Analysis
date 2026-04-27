import os
import requests
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import time
from datetime import datetime

def download_weekly_data(year, week, save_dir):
    """指定された年度と週のデータをダウンロードする関数"""
    try:
        # 2013年～2022年のURLパターン
        if year <= 2022:
            url = f"https://id-info.jihs.go.jp/niid/images/idwr/sokuho/idwr-{year}/{year}{week:02d}/2013-{week:02d}-teiten.csv"
        # 2023年以降のURLパターン
        else:
            url = f"https://id-info.jihs.go.jp/surveillance/idwr/rapid/{year}/{week}/{year}-{week:02d}-teiten.csv"
        
        # 保存先のパス
        save_path = save_dir / f"{year}_{week:02d}.csv"
        
        # ファイルが既に存在する場合はスキップ
        if save_path.exists():
            print(f"ファイルは既に存在します: {save_path}")
            return True
        
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
            unit_scale=True
        ) as pbar:
            for data in response.iter_content(block_size):
                size = f.write(data)
                pbar.update(size)
        
        print(f"ダウンロード完了: {save_path}")
        return True
        
    except Exception as e:
        print(f"ダウンロードエラー: {year}年{week}週目")
        print(f"エラー内容: {str(e)}")
        return False

def main():
    # 保存先ディレクトリの設定
    base_dir = Path("idwr_data")
    csv_dir = base_dir / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)
    
    # 2013年のテスト（1週目のみ）
    year = 2013
    week = 1
    
    print(f"\n{year}年{week}週目のデータをダウンロードします...")
    success = download_weekly_data(year, week, csv_dir)
    
    if success:
        print("\nダウンロードが完了しました")
        # ダウンロードしたファイルの内容を確認
        file_path = csv_dir / f"{year}_{week:02d}.csv"
        if file_path.exists():
            df = pd.read_csv(file_path, encoding='cp932')
            print("\nファイルの内容:")
            print(f"行数: {len(df)}")
            print(f"列名: {df.columns.tolist()}")
            print("\n最初の5行:")
            print(df.head())
    else:
        print("\nダウンロードに失敗しました")

if __name__ == "__main__":
    main() 