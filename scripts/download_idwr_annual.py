import os
import requests
import zipfile
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import time
from bs4 import BeautifulSoup
import re

def get_csv_links(year):
    """指定年度のCSVリンクを取得する関数"""
    try:
        # 年度ページのURL
        url = f"https://idsc.niid.go.jp/idwr/IDWR/IDWR{year}/idwr{year}.html"
        response = requests.get(url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        csv_links = []
        
        # CSVリンクを探す
        for link in soup.find_all('a'):
            href = link.get('href', '')
            if href.endswith('.csv'):
                # 相対パスを絶対パスに変換
                if href.startswith('./'):
                    href = f"https://idsc.niid.go.jp/idwr/IDWR/IDWR{year}/{href[2:]}"
                elif href.startswith('../'):
                    href = f"https://idsc.niid.go.jp/idwr/IDWR/{href[3:]}"
                csv_links.append(href)
        
        return csv_links
    except Exception as e:
        print(f"CSVリンク取得エラー: {year}年度")
        print(f"エラー内容: {str(e)}")
        return []

def download_file(url, save_path):
    """ファイルをダウンロードする関数"""
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024
        
        with open(save_path, 'wb') as f, tqdm(
            desc=save_path.name,
            total=total_size,
            unit='iB',
            unit_scale=True
        ) as pbar:
            for data in response.iter_content(block_size):
                size = f.write(data)
                pbar.update(size)
        return True
    except Exception as e:
        print(f"ダウンロードエラー: {url}")
        print(f"エラー内容: {str(e)}")
        return False

def extract_zip(zip_path, extract_dir):
    """ZIPファイルを展開する関数"""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        return True
    except Exception as e:
        print(f"ZIP展開エラー: {zip_path}")
        print(f"エラー内容: {str(e)}")
        return False

def main():
    # ディレクトリの設定
    base_dir = Path("idwr_data")
    zip_dir = base_dir / "zip"
    csv_dir = base_dir / "csv"
    
    # ディレクトリの作成
    zip_dir.mkdir(parents=True, exist_ok=True)
    csv_dir.mkdir(parents=True, exist_ok=True)
    
    # 2013年から2024年までの年度データをダウンロード
    for year in range(2013, 2025):
        print(f"\n{year}年度のデータを処理中...")
        
        # ZIPファイルのURLと保存パス
        zip_url = f"https://www.niid.go.jp/niid/images/idwr/IDWR/{year}/idwr{year}.zip"
        zip_path = zip_dir / f"idwr{year}.zip"
        
        # ZIPファイルのダウンロード
        if not zip_path.exists():
            print(f"ZIPファイルをダウンロード中: {zip_url}")
            if download_file(zip_url, zip_path):
                print("ダウンロード完了")
            else:
                print("ダウンロード失敗")
                continue
        else:
            print("ZIPファイルは既に存在します")
        
        # ZIPファイルの展開
        year_csv_dir = csv_dir / str(year)
        if not year_csv_dir.exists():
            print(f"ZIPファイルを展開中: {zip_path}")
            if extract_zip(zip_path, year_csv_dir):
                print("展開完了")
            else:
                print("展開失敗")
                continue
        else:
            print("CSVファイルは既に展開済みです")
        
        # サーバー負荷軽減のため少し待機
        time.sleep(1)

if __name__ == "__main__":
    main() 