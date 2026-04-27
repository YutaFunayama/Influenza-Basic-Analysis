# CLAUDE.md — インフルエンザ感染症分析プロジェクト

親ディレクトリの `07_研究活動/CLAUDE.md` の共通ルールに加え、本プロジェクト固有のルールを定義する。

## プロジェクト概要

IDWRデータに基づくインフルエンザ感染者数の基本傾向分析プロジェクト。データ整備・可視化・レポート生成が主な目的。

## リポジトリ

https://github.com/YutaFunayama/Influenza-Basic-Analysis

## データソース

| データ | ソース | 格納先 |
|---|---|---|
| IDWR週報 (647 CSV) | 国立感染症研究所 | `data/raw/idwr/` |
| 前処理済みIDWR | 上記を整形 | `data/processed/idwr_tidy.parquet` |
| 気象データ (47 CSV) | Meteostat API | `data/raw/weather/` |
| 検索トレンド (47 CSV) | Google Trends | `data/raw/trends/` |
| 国勢調査 | 総務省 | `data/raw/2025-11-zensu.csv` |
| PatchTST HPOモデル | 既存研究 | `models/patchtst_prefecture_hpo/` |

**注意**: `data/raw/weather/` の気象データはMeteostat API由来。湿度が全欠損。PISID vs PINN-PatchTSTプロジェクトではOpen-Meteo (JMA+ERA5)で再取得済み。本プロジェクトでも気象データを使う分析を行う場合は、Open-Meteoデータの使用を検討すること。

## レポート生成

```bash
python scripts/generate_trend_report.py
# → reports/01_basic_trend_analysis.html
```
