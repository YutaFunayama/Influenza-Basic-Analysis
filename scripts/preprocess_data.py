import pandas as pd
import numpy as np
from pathlib import Path
import glob
from datetime import datetime
import polars as pl

def load_niid_data():
    """
    NIIDデータを読み込む関数
    
    Returns:
    --------
    pd.DataFrame
        整形されたNIIDデータ
    """
    # データの読み込み
    df = pl.read_parquet("idwr_data/processed/idwr_tidy.parquet")
    
    # インフルエンザのデータのみを抽出
    df = df.filter(pl.col("disease") == "インフルエンザ")
    
    # 日付列をdatetime型に変換
    df = df.with_columns(pl.col("date").cast(pl.Date))
    
    return df

def load_weather_data():
    """
    気象データを読み込む関数
    
    Returns:
    --------
    pd.DataFrame
        整形された気象データ
    """
    # 気象データのディレクトリ
    weather_dir = Path("weather_data")
    
    # すべての気象データファイルを読み込む
    dfs = []
    for file in weather_dir.glob("weather_*.csv"):
        df = pd.read_csv(file)
        dfs.append(df)
    
    # データフレームを結合
    weather_df = pd.concat(dfs, ignore_index=True)
    
    # 日付列をdatetime型に変換
    weather_df["date"] = pd.to_datetime(weather_df["date"])
    
    return weather_df

def load_search_trends():
    """
    検索トレンドデータを読み込む関数
    
    Returns:
    --------
    pd.DataFrame
        整形された検索トレンドデータ
    """
    # 検索トレンドデータのディレクトリ
    trends_dir = Path("search_trends_data")
    
    # すべての検索トレンドデータファイルを読み込む
    dfs = []
    for file in trends_dir.glob("search_trends_*.csv"):
        df = pd.read_csv(file)
        dfs.append(df)
    
    # データフレームを結合
    trends_df = pd.concat(dfs, ignore_index=True)
    
    # 日付列をdatetime型に変換
    trends_df["date"] = pd.to_datetime(trends_df["date"])
    
    return trends_df

def merge_data(niid_df, weather_df, trends_df):
    """
    データを統合する関数
    
    Parameters:
    -----------
    niid_df : pd.DataFrame
        NIIDデータ
    weather_df : pd.DataFrame
        気象データ
    trends_df : pd.DataFrame
        検索トレンドデータ
    
    Returns:
    --------
    pd.DataFrame
        統合されたデータ
    """
    # NIIDデータをPandas DataFrameに変換
    niid_df = niid_df.to_pandas()
    
    # データの結合
    # 1. NIIDデータと気象データの結合
    merged_df = pd.merge(
        niid_df,
        weather_df,
        on=["date", "prefecture"],
        how="left"
    )
    
    # 2. 検索トレンドデータの結合
    merged_df = pd.merge(
        merged_df,
        trends_df,
        on=["date", "prefecture"],
        how="left"
    )
    
    # 欠損値の処理
    merged_df = merged_df.fillna(method="ffill")  # 前方補完
    merged_df = merged_df.fillna(method="bfill")  # 後方補完
    merged_df = merged_df.fillna(0)  # 残りの欠損値を0で埋める
    
    return merged_df

def add_features(df):
    """
    特徴量を追加する関数
    
    Parameters:
    -----------
    df : pd.DataFrame
        統合されたデータ
    
    Returns:
    --------
    pd.DataFrame
        特徴量が追加されたデータ
    """
    # 日付関連の特徴量
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["week"] = df["date"].dt.isocalendar().week
    df["dayofweek"] = df["date"].dt.dayofweek
    
    # 季節性の特徴量
    df["is_winter"] = df["month"].isin([12, 1, 2]).astype(int)
    df["is_spring"] = df["month"].isin([3, 4, 5]).astype(int)
    df["is_summer"] = df["month"].isin([6, 7, 8]).astype(int)
    df["is_autumn"] = df["month"].isin([9, 10, 11]).astype(int)
    
    # 時系列の特徴量
    for col in ["reported_cases", "temperature", "humidity", "precipitation",
                "influenza_search", "cold_search", "fever_search", "cough_search"]:
        # 移動平均
        df[f"{col}_ma_2w"] = df.groupby("prefecture")[col].transform(
            lambda x: x.rolling(window=2, min_periods=1).mean()
        )
        df[f"{col}_ma_4w"] = df.groupby("prefecture")[col].transform(
            lambda x: x.rolling(window=4, min_periods=1).mean()
        )
        
        # 差分
        df[f"{col}_diff"] = df.groupby("prefecture")[col].transform(
            lambda x: x.diff()
        )
        
        # 変化率
        df[f"{col}_pct_change"] = df.groupby("prefecture")[col].transform(
            lambda x: x.pct_change()
        )
    
    return df

def main():
    # 出力ディレクトリの作成
    output_dir = Path("processed_data")
    output_dir.mkdir(exist_ok=True)
    
    # データの読み込み
    print("Loading NIID data...")
    niid_df = load_niid_data()
    
    print("Loading weather data...")
    weather_df = load_weather_data()
    
    print("Loading search trends data...")
    trends_df = load_search_trends()
    
    # データの統合
    print("Merging data...")
    merged_df = merge_data(niid_df, weather_df, trends_df)
    
    # 特徴量の追加
    print("Adding features...")
    final_df = add_features(merged_df)
    
    # データの保存
    print("Saving processed data...")
    final_df.to_parquet(output_dir / "processed_data.parquet", index=False)
    print("Done!")

if __name__ == "__main__":
    main() 