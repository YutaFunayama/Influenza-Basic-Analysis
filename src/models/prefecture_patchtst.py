"""prefecture_patchtst.py

都道府県別PatchTSTモデルのクラス定義。

`models/patchtst_prefecture_hpo/patchtst_hpo_pref_XX.pth` をロードする際に必要。
元実装: 感染症拡大シミュレーション/scripts/hpo_patchtst_prefecture_pilot.py の HPO版クラス。

使用例:
    import torch
    from src.models.prefecture_patchtst import PrefecturePatchTST
    from src.utils.device import get_device

    device = get_device()
    model = PrefecturePatchTST(seq_len=64, n_features=N, d_model=128, n_heads=8,
                               patch_len=16, dropout=0.1)
    model.load_state_dict(torch.load("models/patchtst_prefecture_hpo/patchtst_hpo_pref_13.pth",
                                     map_location=device))
    model.to(device).eval()

依存: torch, tsai (`pip install tsai`)
"""

import torch.nn as nn
from tsai.models.PatchTST import PatchTST


class PrefecturePatchTST(nn.Module):
    """都道府県別PatchTSTモデル（HPO版）"""

    def __init__(self, seq_len: int, n_features: int, d_model: int = 128,
                 n_heads: int = 8, patch_len: int = 16, dropout: float = 0.1,
                 revin: bool = False, input_norm: bool = False, output_norm: bool = False):
        super().__init__()
        self.core = PatchTST(
            c_in=n_features,
            c_out=1,
            seq_len=seq_len,
            pred_dim=1,
            d_model=d_model,
            n_heads=n_heads,
            patch_len=patch_len,
            dropout=dropout,
            revin=revin,
        )
        self.input_norm = nn.LayerNorm(n_features) if input_norm else None
        self.output_norm = nn.LayerNorm(1) if output_norm else None

    def forward(self, x):  # (bs, seq_len, n_features)
        if self.input_norm:
            x = self.input_norm(x)

        x = x.permute(0, 2, 1)   # (bs, n_features, seq_len)
        y = self.core(x)          # (bs, n_features, 1)
        y = y[:, 0, 0]            # (bs,) - インフルエンザ症例数のみ

        if self.output_norm:
            y = self.output_norm(y.unsqueeze(-1)).squeeze(-1)

        return y
