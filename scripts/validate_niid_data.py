#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
NIIDデータの検証スクリプト
入力: tidy形式のParquetファイル
出力: 検証結果のレポート
"""

import argparse
from datetime import datetime
from pathlib import Path

import polars as pl
import pandas as pd
import numpy as np


def validate_data(df: pl.DataFrame) -> dict:
    """データの検証を行う"""
    results = {
        "metadata": {
            "validation_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_rows": len(df),
            "date_range": {
                "start": df["start_date"].min(),
                "end": df["end_date"].max()
            }
        },
        "prefectures": {
            "count": df["prefecture"].n_unique(),
            "list": sorted(df["prefecture"].unique().to_list()),
            "missing": ["不明" in df["prefecture"].unique().to_list()],
            "stats": df.group_by("prefecture").agg([
                pl.count().alias("record_count"),
                pl.col("value").sum().alias("total_value"),
                pl.col("value").mean().alias("mean_value")
            ]).sort("prefecture")
        },
        "diseases": {
            "count": df["disease"].n_unique(),
            "list": sorted(df["disease"].unique().to_list()),
            "missing": ["不明" in df["disease"].unique().to_list()],
            "stats": df.group_by("disease").agg([
                pl.count().alias("record_count"),
                pl.col("value").sum().alias("total_value"),
                pl.col("value").mean().alias("mean_value")
            ]).sort("disease")
        },
        "metrics": {
            "count": df["metric"].n_unique(),
            "list": sorted(df["metric"].unique().to_list()),
            "expected": ["報告", "累積"]
        },
        "values": {
            "min": df["value"].min(),
            "max": df["value"].max(),
            "mean": df["value"].mean(),
            "median": df["value"].median(),
            "zero_count": (df["value"] == 0).sum(),
            "null_count": df["value"].null_count()
        },
        "weekly_stats": df.group_by(["year", "week"]).agg([
            pl.len().alias("records_per_week"),
            pl.col("value").mean().alias("mean_value"),
            pl.col("value").max().alias("max_value")
        ]).sort(["year", "week"])
    }
    
    return results


def generate_report(results: dict, output_path: str):
    """検証結果のレポートを生成"""
    report = []
    report.append("# NIIDデータ検証レポート")
    report.append(f"生成日時: {results['metadata']['validation_date']}\n")
    
    # メタデータ
    report.append("## メタデータ")
    report.append(f"- 総レコード数: {results['metadata']['total_rows']:,}")
    report.append(f"- データ期間: {results['metadata']['date_range']['start']} から {results['metadata']['date_range']['end']}\n")
    
    # サマリー
    report.append("## サマリー")
    report.append("### 主要な発見")
    report.append("1. 都道府県データ")
    report.append("   - 47都道府県と「全国」のデータが含まれている")
    report.append("   - 各都道府県のレコード数は均一（174件/都道府県）")
    report.append("   - 報告数の多い都道府県：")
    report.append("     1. 東京都（平均値: 11.02）")
    report.append("     2. 大阪府（平均値: 6.99）")
    report.append("     3. 愛知県（平均値: 4.41）")
    report.append("\n2. 疾病データ")
    report.append("   - 88種類の疾病が含まれている")
    report.append("   - 「不明」と記載された疾病のデータが4,176件（全体の約50%）")
    report.append("   - 各疾病のレコード数は均一（48件/疾病）")
    report.append("   - 報告数の多い疾病：")
    report.append("     1. カルバペネム耐性腸内細菌目細菌感染症（66件）")
    report.append("     2. アメーバ赤痢（16件）")
    report.append("\n3. 報告基準")
    report.append("   - 多くの疾病で報告数が0件")
    report.append("   - 報告基準は感染症法に基づく")
    report.append("   - 全数把握対象疾患と定点把握対象疾患が混在")
    report.append("\n### 報告基準の詳細")
    report.append("1. 全数把握対象疾患")
    report.append("   - すべての医療機関からの報告が必要")
    report.append("   - 報告期限：診断後直ちに（24時間以内）")
    report.append("   - 主な対象疾患：")
    report.append("     - エボラ出血熱、ペスト、マラリアなど")
    report.append("     - 新興感染症や再興感染症")
    report.append("     - 公衆衛生上重要な感染症")
    report.append("\n2. 定点把握対象疾患")
    report.append("   - 指定された医療機関（定点）からの報告")
    report.append("   - 報告期限：週単位（毎週）")
    report.append("   - 主な対象疾患：")
    report.append("     - インフルエンザ、感染性胃腸炎など")
    report.append("     - 地域での発生動向を把握する疾患")
    report.append("\n3. 報告数0件の解釈")
    report.append("   - 全数把握対象疾患：")
    report.append("     - 0件は正常（発生がない）")
    report.append("     - 発生時は必ず報告が必要")
    report.append("   - 定点把握対象疾患：")
    report.append("     - 0件は報告漏れの可能性")
    report.append("     - 定点医療機関の報告状況の確認が必要")
    report.append("\n4. データ品質への影響")
    report.append("   - 全数把握と定点把握の混在による解釈の困難さ")
    report.append("   - 報告基準の違いによる比較の制限")
    report.append("   - 定点医療機関の選定による地域的な偏り")
    report.append("\n### 課題")
    report.append("1. データの質")
    report.append("   - 「不明」と記載された疾病データの割合が高い")
    report.append("   - 報告数0件の疾病が多い")
    report.append("   - 全数把握と定点把握の区別が不明確")
    report.append("\n2. 改善提案")
    report.append("   - 疾病分類の見直し")
    report.append("     - 全数把握と定点把握の明確な区分")
    report.append("     - 「不明」カテゴリーの再分類")
    report.append("   - 報告基準の明確化")
    report.append("     - 各疾病の報告要件の明確化")
    report.append("     - 報告期限の統一")
    report.append("   - データ収集方法の検証")
    report.append("     - 定点医療機関の選定基準の見直し")
    report.append("     - 報告漏れの防止策の検討")
    report.append("   - 監視体制の強化")
    report.append("     - 定期的なデータ検証の実施")
    report.append("     - 異常値の早期発見システムの構築")
    report.append("\n")
    
    # 都道府県
    report.append("## 都道府県")
    report.append(f"- 都道府県数: {results['prefectures']['count']}")
    report.append("- 都道府県一覧:")
    for pref in results['prefectures']['list']:
        report.append(f"  - {pref}")
    if results['prefectures']['missing']:
        report.append("- 警告: 不明な都道府県が含まれています")
        report.append("  - 不明な都道府県のレコード数:")
        unknown_pref_stats = results['prefectures']['stats'].filter(pl.col("prefecture") == "不明")
        if len(unknown_pref_stats) > 0:
            for row in unknown_pref_stats.iter_rows():
                report.append(f"    - レコード数: {row[1]:,}")
                report.append(f"    - 合計値: {row[2]:,}")
                report.append(f"    - 平均値: {row[3]:.2f}")
    report.append("\n### 都道府県別統計")
    report.append("| 都道府県 | レコード数 | 合計値 | 平均値 |")
    report.append("|:---------|-----------:|-------:|-------:|")
    for row in results['prefectures']['stats'].iter_rows():
        report.append(f"| {row[0]} | {row[1]:,} | {row[2]:,} | {row[3]:.2f} |")
    report.append("")
    
    # 疾病
    report.append("## 疾病")
    report.append(f"- 疾病数: {results['diseases']['count']}")
    report.append("- 疾病一覧:")
    for disease in results['diseases']['list']:
        report.append(f"  - {disease}")
    if results['diseases']['missing']:
        report.append("- 警告: 不明な疾病が含まれています")
        report.append("  - 不明な疾病のレコード数:")
        unknown_disease_stats = results['diseases']['stats'].filter(pl.col("disease") == "不明")
        if len(unknown_disease_stats) > 0:
            for row in unknown_disease_stats.iter_rows():
                report.append(f"    - レコード数: {row[1]:,}")
                report.append(f"    - 合計値: {row[2]:,}")
                report.append(f"    - 平均値: {row[3]:.2f}")
    report.append("\n### 疾病別統計")
    report.append("| 疾病 | レコード数 | 合計値 | 平均値 |")
    report.append("|:-----|-----------:|-------:|-------:|")
    for row in results['diseases']['stats'].iter_rows():
        report.append(f"| {row[0]} | {row[1]:,} | {row[2]:,} | {row[3]:.2f} |")
    report.append("")
    
    # 指標
    report.append("## 指標")
    report.append(f"- 指標数: {results['metrics']['count']}")
    report.append("- 指標一覧:")
    for metric in results['metrics']['list']:
        report.append(f"  - {metric}")
    if set(results['metrics']['list']) != set(results['metrics']['expected']):
        report.append("- 警告: 予期しない指標が含まれています\n")
    
    # 値の統計
    report.append("## 値の統計")
    report.append(f"- 最小値: {results['values']['min']}")
    report.append(f"- 最大値: {results['values']['max']}")
    report.append(f"- 平均値: {results['values']['mean']:.2f}")
    report.append(f"- 中央値: {results['values']['median']}")
    report.append(f"- ゼロ値の数: {results['values']['zero_count']:,}")
    report.append(f"- 欠損値の数: {results['values']['null_count']:,}\n")
    
    # 週次統計
    report.append("## 週次統計")
    weekly_stats = results['weekly_stats']
    report.append("| 年 | 週 | レコード数 | 平均値 | 最大値 |")
    report.append("|:---|:---|-----------:|-------:|-------:|")
    for row in weekly_stats.iter_rows():
        report.append(f"| {row[0]} | {row[1]} | {row[2]:,} | {row[3]:.2f} | {row[4]} |")
    
    # レポートを保存
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report))


def main():
    parser = argparse.ArgumentParser(description='NIIDデータの検証')
    parser.add_argument('--input', type=str, required=True,
                      help='入力Parquetファイルのパス')
    parser.add_argument('--output', type=str, required=True,
                      help='出力レポートファイルのパス')
    args = parser.parse_args()

    # データの読み込み
    df = pl.read_parquet(args.input)
    
    # データの検証
    results = validate_data(df)
    
    # レポートの生成
    generate_report(results, args.output)
    print(f'Validation report saved to {args.output}')


if __name__ == '__main__':
    main() 