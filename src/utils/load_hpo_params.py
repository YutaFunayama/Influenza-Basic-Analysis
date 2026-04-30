"""load_hpo_params.py

HPO結果CSV (`configs/patchtst_hpo_best_params.csv`) から、指定都道府県・ホライゾンの
ベストパラメータを取得するヘルパー。
"""

import json
from pathlib import Path
from typing import Tuple

import pandas as pd

DEFAULT_CSV = Path(__file__).resolve().parents[2] / "configs" / "patchtst_hpo_best_params.csv"

# PrefecturePatchTST のコンストラクタが受け取るキーのみを抽出するためのホワイトリスト
_MODEL_ARCH_KEYS = {"d_model", "n_heads", "patch_len", "dropout", "revin", "input_norm", "output_norm"}


def get_best_params(prefecture_code: int, horizon: int, csv_path: Path = DEFAULT_CSV) -> Tuple[dict, dict]:
    """指定 (都道府県コード, ホライゾン) のHPOベストパラメータを返す。

    Returns
    -------
    model_kwargs : dict
        `PrefecturePatchTST(...)` にそのまま渡せる引数（d_model, n_heads, patch_len, dropout, revin, input_norm, output_norm）
    train_meta : dict
        学習時のみ使用するパラメータ（lr, batch_size, zero_weight 等）。推論には不要。
    """
    df = pd.read_csv(csv_path)
    row = df[(df.prefecture_code == prefecture_code) & (df.horizon == horizon)]
    if row.empty:
        raise ValueError(f"No HPO entry for prefecture_code={prefecture_code}, horizon={horizon}")

    params = json.loads(row.iloc[0]["best_params"])
    model_kwargs = {k: v for k, v in params.items() if k in _MODEL_ARCH_KEYS}
    train_meta = {k: v for k, v in params.items() if k not in _MODEL_ARCH_KEYS}
    return model_kwargs, train_meta
