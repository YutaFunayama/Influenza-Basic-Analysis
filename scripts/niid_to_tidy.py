#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
NIIDデータをtidy形式に変換するスクリプト
入力: NIIDの週次報告CSVファイル（複数年のデータに対応）
出力: tidy形式のParquetファイル
"""

import argparse
import glob
import os
import re
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Dict, Any

import pandas as pd
import polars as pl
from tqdm import tqdm


def extract_week_info(week_str: str) -> Tuple[int, int, datetime, datetime]:
    """週次情報から年・週番号と日付範囲を抽出"""
    try:
        year = int(re.search(r'(\d{4})年', week_str).group(1))
        week = int(re.search(r'(\d{1,2})週', week_str).group(1))
        date_range = re.search(r'\((\d{1,2})月(\d{1,2})日〜(\d{1,2})月(\d{1,2})日\)', week_str)
        if not date_range:
            raise ValueError(f"日付範囲が見つかりません: {week_str}")
        
        start_date = datetime(year, int(date_range.group(1)), int(date_range.group(2)))
        end_date = datetime(year, int(date_range.group(3)), int(date_range.group(4)))
        return year, week, start_date, end_date
    except Exception as e:
        raise ValueError(f"週次情報の解析に失敗しました: {week_str} - {str(e)}")


def normalize_prefecture_name(pref: str) -> str:
    """都道府県名を正規化"""
    if pd.isna(pref):
        return "不明"
    pref = str(pref).strip()
    if pref == "総数":
        return "全国"
    return pref


def normalize_disease_name(disease: str) -> str:
    """疾病名を正規化"""
    if pd.isna(disease):
        return "不明"
    return str(disease).strip()


def normalize_metric_name(metric: str) -> str:
    """指標名を正規化"""
    if pd.isna(metric):
        return "不明"
    metric = str(metric).strip()
    if metric not in ["報告", "累積"]:
        return "不明"
    return metric


def normalize_value(val: Any) -> int:
    """値を正規化（数値に変換）"""
    if pd.isna(val) or str(val).strip() in ["－", "-", ""]:
        return 0
    try:
        return int(str(val).strip())
    except ValueError:
        return 0


def read_niid_csv(file_path: str) -> pd.DataFrame:
    """NIIDの週次報告CSVファイルを読み込む"""
    try:
        # ヘッダーなしで読み込み
        raw = pd.read_csv(file_path, encoding='shift-jis', header=None)
        
        # 対象週情報を抽出（行1）
        week_info = str(raw.iloc[1, 0])
        year, week, start_date, end_date = extract_week_info(week_info)
        
        # 疾病名（行2）と指標（行3）を取得
        disease_row = raw.iloc[2]
        metric_row = raw.iloc[3]
        
        # 都道府県データ（行4以降）を取得
        prefectures = [normalize_prefecture_name(p) for p in raw.iloc[4:, 0]]
        
        # 各列について、(疾病名, 指標)のペアを作成
        records = []
        for col in range(1, raw.shape[1]):
            disease = normalize_disease_name(disease_row[col])
            metric = normalize_metric_name(metric_row[col])
            
            for row in range(4, raw.shape[0]):
                pref = normalize_prefecture_name(raw.iloc[row, 0])
                val = normalize_value(raw.iloc[row, col])
                
                records.append({
                    'year': year,
                    'week': week,
                    'start_date': start_date,
                    'end_date': end_date,
                    'prefecture': pref,
                    'disease': disease,
                    'metric': metric,
                    'value': val
                })
        
        return pd.DataFrame(records)
    
    except Exception as e:
        raise ValueError(f"ファイルの読み込みに失敗しました: {file_path} - {str(e)}")


def main():
    parser = argparse.ArgumentParser(description='NIIDデータをtidy形式に変換')
    parser.add_argument('--src', type=str, required=True,
                      help='入力CSVファイルのディレクトリパス')
    parser.add_argument('--out', type=str, required=True,
                      help='出力Parquetファイルのパス')
    args = parser.parse_args()

    # 入力ディレクトリ内のCSVファイルを取得
    csv_files = glob.glob(os.path.join(args.src, '*.csv'))
    if not csv_files:
        raise FileNotFoundError(f'No CSV files found in {args.src}')

    # 各CSVファイルを読み込んで結合
    dfs = []
    for file_path in tqdm(sorted(csv_files), desc="Processing CSV files"):
        try:
            df = read_niid_csv(file_path)
            dfs.append(df)
        except Exception as e:
            print(f"Warning: Failed to process {file_path}: {str(e)}")
            continue

    if not dfs:
        raise ValueError("No valid data was processed")

    # データフレームを結合
    df_combined = pd.concat(dfs, ignore_index=True)
    
    # 出力ディレクトリが存在しない場合は作成
    out_dir = os.path.dirname(args.out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    # Polarsデータフレームに変換して保存
    df_tidy_pl = pl.from_pandas(df_combined)
    df_tidy_pl.write_parquet(args.out)
    print(f'Converted data saved to {args.out}')


if __name__ == '__main__':
    main() 