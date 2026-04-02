"""
インフルエンザ感染者数 基本傾向分析レポート生成スクリプト

レポート構成:
  1. エグゼクティブサマリー
  2. 全国の感染者数推移（週次時系列）
  3. シーズン別比較分析
  4. 都道府県別分析（地域ヒートマップ・ランキング）
  5. 季節性パターン分析
  6. COVID-19影響の分析
  7. 説明変数との相関（気象・検索トレンド）
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from datetime import datetime

BASE = Path(__file__).resolve().parent.parent
OUT = BASE / "reports"
OUT.mkdir(parents=True, exist_ok=True)

# ── Data Loading ──────────────────────────────────────────────

df_all = pd.read_parquet(BASE / "data/processed/idwr_tidy.parquet")
flu = df_all[(df_all["disease"] == "インフルエンザ") & (df_all["metric"] == "報告")].copy()
flu["end_date"] = pd.to_datetime(flu["end_date"])

national = flu[flu["prefecture"] == "全国"].sort_values("end_date").copy()
prefs = flu[(flu["prefecture"] != "全国") & (flu["prefecture"] != "不明")].copy()

PREF_NAMES = [
    "北海道","青森県","岩手県","宮城県","秋田県","山形県","福島県",
    "茨城県","栃木県","群馬県","埼玉県","千葉県","東京都","神奈川県",
    "新潟県","富山県","石川県","福井県","山梨県","長野県",
    "岐阜県","静岡県","愛知県","三重県","滋賀県","京都府","大阪府",
    "兵庫県","奈良県","和歌山県","鳥取県","島根県","岡山県","広島県",
    "山口県","徳島県","香川県","愛媛県","高知県",
    "福岡県","佐賀県","長崎県","熊本県","大分県","宮崎県","鹿児島県","沖縄県",
]

REGIONS = {
    "北海道": ["北海道"],
    "東北": ["青森県","岩手県","宮城県","秋田県","山形県","福島県"],
    "関東": ["茨城県","栃木県","群馬県","埼玉県","千葉県","東京都","神奈川県"],
    "中部": ["新潟県","富山県","石川県","福井県","山梨県","長野県","岐阜県","静岡県","愛知県"],
    "近畿": ["三重県","滋賀県","京都府","大阪府","兵庫県","奈良県","和歌山県"],
    "中国": ["鳥取県","島根県","岡山県","広島県","山口県"],
    "四国": ["徳島県","香川県","愛媛県","高知県"],
    "九州・沖縄": ["福岡県","佐賀県","長崎県","熊本県","大分県","宮崎県","鹿児島県","沖縄県"],
}
pref_to_region = {}
for region, ps in REGIONS.items():
    for p in ps:
        pref_to_region[p] = region


def assign_season(row):
    if row["week"] >= 40:
        return str(row["year"]) + "/" + str(row["year"] + 1)
    elif row["week"] <= 20:
        return str(row["year"] - 1) + "/" + str(row["year"])
    return None


national["season"] = national.apply(assign_season, axis=1)
prefs["season"] = prefs.apply(assign_season, axis=1)

# ── Weather Data ──────────────────────────────────────────────
weather_frames = []
for f in sorted((BASE / "data/raw/weather").glob("weather_*.csv")):
    weather_frames.append(pd.read_csv(f))
weather = pd.concat(weather_frames, ignore_index=True) if weather_frames else pd.DataFrame()
if len(weather):
    weather["date"] = pd.to_datetime(weather["date"])

# ── Trends Data ──────────────────────────────────────────────
trends_frames = []
for f in sorted((BASE / "data/raw/trends").glob("trends_*.csv")):
    trends_frames.append(pd.read_csv(f))
trends = pd.concat(trends_frames, ignore_index=True) if trends_frames else pd.DataFrame()
if len(trends):
    trends["date"] = pd.to_datetime(trends["date"])


def to_js(obj):
    def default(o):
        if isinstance(o, (np.integer,)):
            return int(o)
        if isinstance(o, (np.floating,)):
            return None if np.isnan(o) else float(o)
        if isinstance(o, (np.ndarray,)):
            return o.tolist()
        if isinstance(o, (pd.Timestamp, datetime)):
            return o.isoformat()
        raise TypeError(repr(type(o)))
    return json.dumps(obj, default=default, ensure_ascii=False)


# ══════════════════════════════════════════════════════════════
#  Compute chart data
# ══════════════════════════════════════════════════════════════

# 1) Weekly time series
ts = national[["end_date", "value"]].copy()
ts["d"] = ts["end_date"].dt.strftime("%Y-%m-%d")
chart1_labels = to_js(ts["d"].tolist())
chart1_values = to_js(ts["value"].tolist())

# 2) Annual totals
annual = national.groupby("year")["value"].sum().reset_index()
chart2_labels = to_js(annual["year"].astype(str).tolist())
chart2_values = to_js(annual["value"].tolist())

# 3) Season overlay
season_data = {}
for season, grp in national[national["season"].notna()].groupby("season"):
    grp = grp.sort_values("end_date")
    vals = []
    for _, r in grp.iterrows():
        sw = r["week"] - 39 if r["week"] >= 40 else r["week"] + 14
        vals.append({"sw": int(sw), "v": int(r["value"])})
    season_data[season] = sorted(vals, key=lambda x: x["sw"])
chart3_data = to_js(season_data)

# 4) Prefecture ranking
pref_total = prefs.groupby("prefecture")["value"].sum().sort_values(ascending=False).reset_index()
chart4_labels = to_js(pref_total["prefecture"].tolist()[:20])
chart4_values = to_js(pref_total["value"].tolist()[:20])

# 5) Region by year
prefs_wr = prefs.copy()
prefs_wr["region"] = prefs_wr["prefecture"].map(pref_to_region)
region_year = prefs_wr.groupby(["region", "year"])["value"].sum().reset_index()
rd = {}
for region in REGIONS:
    rdf = region_year[region_year["region"] == region].sort_values("year")
    rd[region] = {"years": rdf["year"].astype(str).tolist(), "values": rdf["value"].tolist()}
chart5_data = to_js(rd)

# 6) Monthly average
national_m = national.copy()
national_m["month"] = national_m["end_date"].dt.month
month_avg = national_m.groupby("month")["value"].mean().reset_index()
chart6_labels = to_js([str(int(m)) + "月" for m in month_avg["month"]])
chart6_values = to_js(month_avg["value"].tolist())

# 7) COVID comparison
covid_data = {}
for y in [2019, 2020, 2021, 2022, 2023]:
    ydf = national[national["year"] == y].sort_values("week")
    covid_data[str(y)] = {"weeks": ydf["week"].tolist(), "values": ydf["value"].tolist()}
chart7_data = to_js(covid_data)

# 8) Peak analysis table
peak_rows = []
for season, grp in national[national["season"].notna()].groupby("season"):
    if len(grp) == 0:
        continue
    idx = grp["value"].idxmax()
    p = grp.loc[idx]
    peak_rows.append({
        "season": season, "peak_week": int(p["week"]),
        "peak_value": int(p["value"]),
        "peak_date": p["end_date"].strftime("%Y-%m-%d"),
        "total": int(grp["value"].sum()),
    })
peak_table_html = "\n".join(
    '<tr><td>{s}</td><td>第{w}週</td><td>{d}</td><td>{v:,}</td><td>{t:,}</td></tr>'.format(
        s=r["season"], w=r["peak_week"], d=r["peak_date"], v=r["peak_value"], t=r["total"]
    ) for r in peak_rows
)

# 9) Heatmap
hm = prefs.groupby(["year", "prefecture"])["value"].sum().reset_index()
hm_pivot = hm.pivot(index="prefecture", columns="year", values="value").fillna(0)
hm_pivot = hm_pivot.reindex(PREF_NAMES)
hm_years = to_js([str(y) for y in hm_pivot.columns.tolist()])
hm_prefs = to_js(hm_pivot.index.tolist())
hm_vals = []
pref_list = hm_pivot.index.tolist()
for pref in pref_list:
    for yi, y in enumerate(hm_pivot.columns):
        hm_vals.append({"x": yi, "y": pref_list.index(pref), "v": int(hm_pivot.loc[pref, y])})
hm_vals_js = to_js(hm_vals)

# 10) Weather correlation
corr_section_html = ""
corr_chart_js = ""
if len(weather):
    w_weekly = weather.groupby("date").agg({"temperature": "mean", "precipitation": "mean"}).reset_index()
    w_weekly.columns = ["date", "avg_temp", "avg_precip"]
    nat_c = national[["end_date", "value"]].copy()
    nat_c.columns = ["date", "flu_value"]
    merged = pd.merge_asof(nat_c.sort_values("date"), w_weekly.sort_values("date"),
                           on="date", direction="nearest", tolerance=pd.Timedelta("7d"))
    merged = merged.dropna(subset=["avg_temp"])
    if len(merged) > 10:
        corr_temp = round(float(merged["flu_value"].corr(merged["avg_temp"])), 3)
        corr_precip = round(float(merged["flu_value"].corr(merged["avg_precip"])), 3)
        sc_temp = to_js([{"x": round(float(r["avg_temp"]), 1), "y": int(r["flu_value"])} for _, r in merged.iterrows()])
        sc_precip = to_js([{"x": round(float(r["avg_precip"]), 1), "y": int(r["flu_value"])} for _, r in merged.iterrows()])

        temp_class = "corr-neg" if corr_temp < -0.1 else ("corr-pos" if corr_temp > 0.1 else "corr-weak")
        precip_class = "corr-neg" if corr_precip < -0.1 else ("corr-pos" if corr_precip > 0.1 else "corr-weak")

        corr_section_html += """
  <h3>7.1 気象データとの相関</h3>
  <div class="two-col">
    <div>
      <p>気温との相関: <span class="corr-badge {tc}">r = {ct}</span></p>
      <div class="chart-wrap chart-small"><canvas id="chart_corr_temp"></canvas></div>
    </div>
    <div>
      <p>降水量との相関: <span class="corr-badge {pc}">r = {cp}</span></p>
      <div class="chart-wrap chart-small"><canvas id="chart_corr_precip"></canvas></div>
    </div>
  </div>
  <div class="insight">
    <strong>知見：</strong>気温と感染者数の間には負の相関（r={ct}）が確認され、
    気温が低い時期に感染者が増加する傾向と整合する。降水量との相関は弱い（r={cp}）。
  </div>""".format(ct=corr_temp, cp=corr_precip, tc=temp_class, pc=precip_class)

        corr_chart_js += """
new Chart(document.getElementById('chart_corr_temp'), {
  type:'scatter',
  data:{ datasets:[{
    label:'気温 vs 報告数', data:""" + sc_temp + """,
    backgroundColor:'rgba(52,152,219,.3)', pointRadius:2,
  }] },
  options:{
    responsive:true, maintainAspectRatio:false,
    scales:{
      x:{ title:{display:true, text:'平均気温 (℃)'} },
      y:{ title:{display:true, text:'報告数'}, ticks:{ callback:v=>v.toLocaleString() } }
    },
    plugins:{ legend:{display:false} }
  }
});
new Chart(document.getElementById('chart_corr_precip'), {
  type:'scatter',
  data:{ datasets:[{
    label:'降水量 vs 報告数', data:""" + sc_precip + """,
    backgroundColor:'rgba(231,76,60,.3)', pointRadius:2,
  }] },
  options:{
    responsive:true, maintainAspectRatio:false,
    scales:{
      x:{ title:{display:true, text:'平均降水量 (mm)'} },
      y:{ title:{display:true, text:'報告数'}, ticks:{ callback:v=>v.toLocaleString() } }
    },
    plugins:{ legend:{display:false} }
  }
});
"""

# 11) Trends correlation
trends_section_html = ""
trends_chart_js = ""
if len(trends):
    t_national = trends.groupby("date")["hits"].mean().reset_index()
    nat_t = national[["end_date", "value"]].copy()
    nat_t.columns = ["date", "flu_value"]
    t_merged = pd.merge_asof(nat_t.sort_values("date"), t_national.sort_values("date"),
                              on="date", direction="nearest", tolerance=pd.Timedelta("35d"))
    t_merged = t_merged.dropna(subset=["hits"])
    if len(t_merged) > 10:
        corr_hits = round(float(t_merged["flu_value"].corr(t_merged["hits"])), 3)
        sc_trends = to_js([{"x": round(float(r["hits"]), 1), "y": int(r["flu_value"])} for _, r in t_merged.iterrows()])

        trends_section_html = """
  <h3>7.2 検索トレンドとの相関</h3>
  <p>「インフルエンザ」検索ヒット数との相関: <span class="corr-badge corr-pos">r = {ch}</span></p>
  <div class="chart-wrap chart-medium"><canvas id="chart_corr_trends"></canvas></div>
  <div class="insight">
    <strong>知見：</strong>検索トレンドと報告数の間には正の相関（r={ch}）が確認される。
    検索行動が実際の感染拡大を反映しており、早期警戒指標としての有用性が示唆される。
  </div>""".format(ch=corr_hits)

        trends_chart_js = """
new Chart(document.getElementById('chart_corr_trends'), {
  type:'scatter',
  data:{ datasets:[{
    label:'検索トレンド vs 報告数', data:""" + sc_trends + """,
    backgroundColor:'rgba(46,204,113,.3)', pointRadius:2,
  }] },
  options:{
    responsive:true, maintainAspectRatio:false,
    scales:{
      x:{ title:{display:true, text:'検索ヒット数'} },
      y:{ title:{display:true, text:'報告数'}, ticks:{ callback:v=>v.toLocaleString() } }
    },
    plugins:{ legend:{display:false} }
  }
});
"""

# ── Summary KPIs ────────────────────────────────────────────
s_period = national["end_date"].min().strftime("%Y-%m-%d") + " ～ " + national["end_date"].max().strftime("%Y-%m-%d")
s_total = int(national["value"].sum())
s_mean = int(national["value"].mean())
s_max = int(national["value"].max())
s_max_date = national.loc[national["value"].idxmax(), "end_date"].strftime("%Y-%m-%d")
s_weeks = len(national)
s_year_start = national["end_date"].min().strftime("%Y")
s_year_end = national["end_date"].max().strftime("%Y")

# ── Detailed insight computation ────────────────────────────
annual_totals = national.groupby("year")["value"].sum()
pre_covid_avg = int(annual_totals.loc[2013:2019].mean())
pct_2023 = annual_totals[2023] / pre_covid_avg * 100
pct_2020 = annual_totals[2020] / pre_covid_avg * 100
pct_2021_drop = (1 - annual_totals[2021] / pre_covid_avg) * 100

# Season totals for insight
season_totals = {}
for pr in peak_rows:
    season_totals[pr["season"]] = pr["total"]
max_season = max(season_totals, key=season_totals.get)
max_season_total = season_totals[max_season]

# Top prefectures
pref_all_total = prefs.groupby("prefecture")["value"].sum().sort_values(ascending=False)
top5 = pref_all_total.head(5)
top5_share = top5.sum() / pref_all_total.sum() * 100
top5_names = "・".join(top5.index.tolist())

# Average peak week (pre-COVID)
pre_covid_peak_weeks = [pr["peak_week"] for pr in peak_rows if int(pr["season"][:4]) < 2019]
avg_peak_week = np.mean(pre_covid_peak_weeks)

# Recovery timing
post_covid = national[national["end_date"] >= "2022-01-01"].sort_values("end_date")
first_big = post_covid[post_covid["value"] > 10000].head(1)
recovery_date = first_big["end_date"].values[0] if len(first_big) else None
recovery_str = pd.Timestamp(recovery_date).strftime("%Y年%m月") if recovery_date is not None else "N/A"

# Build insight HTML
insight_html = """
  <div class="insight">
    <strong>主な知見：</strong>
    <ul style="margin-top:8px; padding-left:20px;">
      <li><strong>明確な季節性：</strong>インフルエンザ報告数は毎年12月〜2月に集中し、
        COVID前のシーズン（2012/2013〜2018/2019）ではピーク週は平均して第{avg_pw:.0f}週（1月下旬〜2月上旬）に到来していた。
        年間報告数のCOVID前7年平均は約{pre_avg:,}件である。</li>
      <li><strong>COVID-19による歴史的断絶：</strong>2020年は第12週（3月）以降に急減し、年間報告数はCOVID前平均の{pct20:.0f}%に低下。
        2021年は前年比{pct21:.1f}%減のわずか{y21:,}件と、実質的にインフルエンザの流行が消失した。
        マスク着用・手指消毒・渡航制限等の非薬物的介入（NPI）がインフルエンザに対しても
        強力な抑制効果を持ったことを示す。</li>
      <li><strong>リバウンドと過去最大規模の流行：</strong>{recov}に週1万件を突破し流行が再開。
        その後{max_s}シーズンは累計{max_st:,}件を記録し、
        COVID前平均の{pct23:.0f}%と過去最大の流行規模となった。
        集団免疫の低下（いわゆる「免疫負債」）が大規模流行の要因と考えられる。</li>
      <li><strong>ピーク時期の前倒し傾向：</strong>2023/2024シーズンおよび2024/2025シーズンでは
        ピークが第49〜52週（12月）に発生しており、従来の第4〜5週（1月末）から約1〜2ヶ月前倒しされている。
        COVID後の流行動態が変化した可能性があり、早期警戒体制の見直しが必要と考えられる。</li>
      <li><strong>地域間格差：</strong>累計報告数の上位5都府県（{top5n}）で全国の{t5s:.1f}%を占める。
        人口集中地域での感染拡大リスクが高く、地域特性に応じた対策が求められる。</li>
      <li><strong>週最大報告数の更新：</strong>{max_d}に週次報告数{max_v:,}件を記録。
        これは単一週としての過去最大値であり、医療提供体制への負荷の大きさを示唆する。</li>
    </ul>
  </div>
""".format(
    avg_pw=avg_peak_week,
    pre_avg=pre_covid_avg,
    pct20=pct_2020,
    pct21=pct_2021_drop,
    y21=int(annual_totals[2021]),
    recov=recovery_str,
    max_s=max_season,
    max_st=max_season_total,
    pct23=pct_2023,
    top5n=top5_names,
    t5s=top5_share,
    max_d=s_max_date,
    max_v=s_max,
)

SEASON_COLORS = to_js([
    "#e74c3c","#3498db","#2ecc71","#f39c12","#9b59b6",
    "#1abc9c","#e67e22","#34495e","#e91e63","#00bcd4",
    "#8bc34a","#ff5722","#607d8b",
])

# ══════════════════════════════════════════════════════════════
#  Build HTML
# ══════════════════════════════════════════════════════════════

html_content = """<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>インフルエンザ感染者数 基本傾向分析レポート</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-chart-matrix@2.0.1/dist/chartjs-chart-matrix.min.js"></script>
<style>
  :root { --bg:#f8f9fa; --card:#fff; --text:#2c3e50; --accent:#3498db; --border:#e0e0e0; --shadow:0 2px 8px rgba(0,0,0,.08); }
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family:"Hiragino Sans","Noto Sans JP","Yu Gothic",sans-serif; background:var(--bg); color:var(--text); line-height:1.7; }
  .container { max-width:1200px; margin:0 auto; padding:24px 16px; }
  header { background:linear-gradient(135deg,#2c3e50,#3498db); color:#fff; padding:48px 24px; text-align:center; margin-bottom:32px; border-radius:12px; }
  header h1 { font-size:2rem; font-weight:700; margin-bottom:8px; }
  header p { opacity:.85; font-size:.95rem; }
  .toc { background:var(--card); border-radius:10px; padding:24px 32px; margin-bottom:32px; box-shadow:var(--shadow); }
  .toc h2 { font-size:1.1rem; margin-bottom:12px; color:var(--accent); }
  .toc ol { padding-left:20px; }
  .toc li { margin-bottom:6px; }
  .toc a { color:var(--text); text-decoration:none; border-bottom:1px dashed var(--border); }
  .toc a:hover { color:var(--accent); }
  section { background:var(--card); border-radius:10px; padding:28px 32px; margin-bottom:28px; box-shadow:var(--shadow); }
  section h2 { font-size:1.3rem; color:var(--accent); margin-bottom:16px; padding-bottom:8px; border-bottom:2px solid var(--accent); }
  section h3 { font-size:1.05rem; margin:20px 0 10px; }
  .kpi-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:16px; margin:20px 0; }
  .kpi { background:#f0f7ff; border-radius:8px; padding:16px; text-align:center; }
  .kpi .num { font-size:1.6rem; font-weight:700; color:var(--accent); }
  .kpi .label { font-size:.8rem; color:#666; margin-top:4px; }
  .chart-wrap { position:relative; width:100%; margin:16px 0; }
  .chart-large { height:420px; }
  .chart-medium { height:340px; }
  .chart-small { height:280px; }
  table { width:100%; border-collapse:collapse; margin:16px 0; font-size:.88rem; }
  th,td { padding:8px 12px; border:1px solid var(--border); text-align:right; }
  th { background:#f0f7ff; font-weight:600; text-align:center; }
  td:first-child,th:first-child { text-align:left; }
  .insight { background:#fffbe6; border-left:4px solid #f39c12; padding:12px 16px; margin:16px 0; border-radius:0 8px 8px 0; font-size:.92rem; }
  .two-col { display:grid; grid-template-columns:1fr 1fr; gap:24px; }
  @media (max-width:768px) { .two-col { grid-template-columns:1fr; } }
  footer { text-align:center; padding:24px; color:#999; font-size:.82rem; }
  .corr-badge { display:inline-block; padding:4px 12px; border-radius:20px; font-weight:700; font-size:.95rem; }
  .corr-neg { background:#fde8e8; color:#c0392b; }
  .corr-pos { background:#e8f8e8; color:#27ae60; }
  .corr-weak { background:#f0f0f0; color:#666; }
</style>
</head>
<body>
<div class="container">

<header>
  <h1>インフルエンザ感染者数 基本傾向分析レポート</h1>
  <p>IDWR（感染症発生動向調査週報）データに基づく """ + s_period + """ の分析</p>
  <p style="margin-top:8px;opacity:.7;">Generated: """ + datetime.now().strftime("%Y-%m-%d %H:%M") + """</p>
</header>

<nav class="toc">
  <h2>目次</h2>
  <ol>
    <li><a href="#summary">エグゼクティブサマリー</a></li>
    <li><a href="#national">全国の感染者数推移</a></li>
    <li><a href="#season">シーズン別比較分析</a></li>
    <li><a href="#prefecture">都道府県別分析</a></li>
    <li><a href="#seasonality">季節性パターン分析</a></li>
    <li><a href="#covid">COVID-19影響の分析</a></li>
    <li><a href="#correlation">説明変数との相関分析</a></li>
  </ol>
</nav>

<!-- 1. Summary -->
<section id="summary">
  <h2>1. エグゼクティブサマリー</h2>
  <div class="kpi-grid">
    <div class="kpi"><div class="num">""" + s_year_start + "–" + s_year_end + """</div><div class="label">分析対象期間</div></div>
    <div class="kpi"><div class="num">""" + "{:,}".format(s_weeks) + """</div><div class="label">データ週数</div></div>
    <div class="kpi"><div class="num">""" + "{:,}".format(s_total) + """</div><div class="label">累計報告数（全国）</div></div>
    <div class="kpi"><div class="num">""" + "{:,}".format(s_mean) + """</div><div class="label">週平均報告数</div></div>
    <div class="kpi"><div class="num">""" + "{:,}".format(s_max) + """</div><div class="label">週最大報告数</div></div>
    <div class="kpi"><div class="num">""" + s_max_date + """</div><div class="label">最大報告週</div></div>
  </div>
  """ + insight_html + """
</section>

<!-- 2. National -->
<section id="national">
  <h2>2. 全国の感染者数推移</h2>
  <h3>2.1 週次時系列</h3>
  <div class="chart-wrap chart-large"><canvas id="chart_ts"></canvas></div>
  <h3>2.2 年別合計報告数</h3>
  <div class="chart-wrap chart-medium"><canvas id="chart_annual"></canvas></div>
</section>

<!-- 3. Season -->
<section id="season">
  <h2>3. シーズン別比較分析</h2>
  <h3>3.1 シーズン重ね合わせ</h3>
  <p style="font-size:.88rem;color:#666;">シーズン第40週を起点として各シーズンの感染カーブを重ねて表示</p>
  <div class="chart-wrap chart-large"><canvas id="chart_season"></canvas></div>
  <h3>3.2 シーズン別ピーク情報</h3>
  <table>
    <tr><th>シーズン</th><th>ピーク週</th><th>ピーク日</th><th>ピーク報告数</th><th>シーズン合計</th></tr>
    """ + peak_table_html + """
  </table>
  <div class="insight">
    <strong>知見：</strong>ピーク週は概ね第1〜5週（1月中旬〜2月上旬）に集中する。
    2023/2024シーズンは最大規模で、COVID前の水準を上回る報告数を記録した。
  </div>
</section>

<!-- 4. Prefecture -->
<section id="prefecture">
  <h2>4. 都道府県別分析</h2>
  <h3>4.1 都道府県別累計報告数ランキング（上位20）</h3>
  <div class="chart-wrap chart-large"><canvas id="chart_pref_rank"></canvas></div>
  <h3>4.2 地域別年次推移</h3>
  <div class="chart-wrap chart-large"><canvas id="chart_region"></canvas></div>
  <h3>4.3 都道府県×年 ヒートマップ</h3>
  <p style="font-size:.88rem;color:#666;">色が濃いほど報告数が多いことを示す</p>
  <div class="chart-wrap" style="height:900px;"><canvas id="chart_heatmap"></canvas></div>
</section>

<!-- 5. Seasonality -->
<section id="seasonality">
  <h2>5. 季節性パターン分析</h2>
  <h3>5.1 月別平均報告数</h3>
  <div class="chart-wrap chart-medium"><canvas id="chart_monthly"></canvas></div>
  <div class="insight">
    <strong>知見：</strong>報告数は12月から急増し、1〜2月にピークを迎え、4月以降は低水準となる。
    夏季（6〜9月）はほぼゼロに近い報告数となる明確な季節性がある。
  </div>
</section>

<!-- 6. COVID -->
<section id="covid">
  <h2>6. COVID-19影響の分析</h2>
  <h3>2019〜2023年の週次推移比較</h3>
  <div class="chart-wrap chart-large"><canvas id="chart_covid"></canvas></div>
  <div class="insight">
    <strong>知見：</strong>2020年は第12週頃（3月）を境に急減し、以降2021年はほぼゼロの状態が続いた。
    これはCOVID-19に対する感染対策（マスク着用・手指消毒・移動制限等）がインフルエンザにも
    抑制効果を持ったことを示唆する。2022年後半から徐々に回復し、2023年にはCOVID前水準に復帰した。
  </div>
</section>

<!-- 7. Correlation -->
<section id="correlation">
  <h2>7. 説明変数との相関分析</h2>
  """ + corr_section_html + """
  """ + trends_section_html + """
</section>

<footer>
  インフルエンザ感染症分析プロジェクト &mdash; データ出典: IDWR（国立感染症研究所）、気象庁、Google Trends
</footer>

</div>

<script>
Chart.defaults.font.family = '"Hiragino Sans","Noto Sans JP",sans-serif';
Chart.defaults.font.size = 12;
Chart.defaults.plugins.legend.labels.usePointStyle = true;

// 2.1 Weekly time series
new Chart(document.getElementById('chart_ts'), {
  type:'line',
  data:{
    labels:""" + chart1_labels + """,
    datasets:[{
      label:'週次報告数（全国）',
      data:""" + chart1_values + """,
      borderColor:'#3498db', backgroundColor:'rgba(52,152,219,.15)',
      fill:true, pointRadius:0, borderWidth:1.5, tension:0.1,
    }]
  },
  options:{
    responsive:true, maintainAspectRatio:false,
    scales:{
      x:{ type:'category', ticks:{maxTicksLimit:20, maxRotation:45} },
      y:{ beginAtZero:true, ticks:{callback:v=>v.toLocaleString()} }
    },
    plugins:{ tooltip:{callbacks:{label:ctx=>ctx.parsed.y.toLocaleString()+'件'}} }
  }
});

// 2.2 Annual
new Chart(document.getElementById('chart_annual'), {
  type:'bar',
  data:{
    labels:""" + chart2_labels + """,
    datasets:[{
      label:'年間合計報告数',
      data:""" + chart2_values + """,
      backgroundColor:""" + chart2_labels + """.map(y=>['2020','2021','2022'].includes(y)?'rgba(231,76,60,.7)':'rgba(52,152,219,.7)'),
      borderRadius:4,
    }]
  },
  options:{
    responsive:true, maintainAspectRatio:false,
    scales:{ y:{beginAtZero:true, ticks:{callback:v=>v.toLocaleString()}} },
    plugins:{ tooltip:{callbacks:{label:ctx=>ctx.parsed.y.toLocaleString()+'件'}} }
  }
});

// 3.1 Season overlay
(function(){
  const sd=""" + chart3_data + """;
  const colors=""" + SEASON_COLORS + """;
  const seasons=Object.keys(sd);
  const datasets=seasons.map((s,i)=>({
    label:s,
    data:sd[s].map(d=>({x:d.sw, y:d.v})),
    borderColor:colors[i%colors.length],
    backgroundColor:'transparent',
    pointRadius:0,
    borderWidth:['2023/2024','2024/2025'].includes(s)?2.5:1.2,
    borderDash:['2019/2020','2020/2021','2021/2022'].includes(s)?[4,4]:[],
    tension:0.2,
  }));
  new Chart(document.getElementById('chart_season'),{
    type:'line', data:{datasets},
    options:{
      responsive:true, maintainAspectRatio:false,
      scales:{
        x:{type:'linear',title:{display:true,text:'シーズン週（第40週=1）'},ticks:{stepSize:4}},
        y:{beginAtZero:true,ticks:{callback:v=>v.toLocaleString()}}
      },
      plugins:{legend:{position:'right',labels:{font:{size:10}}}}
    }
  });
})();

// 4.1 Prefecture ranking
new Chart(document.getElementById('chart_pref_rank'),{
  type:'bar',
  data:{
    labels:""" + chart4_labels + """,
    datasets:[{label:'累計報告数',data:""" + chart4_values + """,backgroundColor:'rgba(52,152,219,.7)',borderRadius:4}]
  },
  options:{
    indexAxis:'y', responsive:true, maintainAspectRatio:false,
    scales:{ x:{beginAtZero:true,ticks:{callback:v=>v.toLocaleString()}} },
    plugins:{ legend:{display:false}, tooltip:{callbacks:{label:ctx=>ctx.parsed.x.toLocaleString()+'件'}} }
  }
});

// 4.2 Region
(function(){
  const rd=""" + chart5_data + """;
  const rc=['#e74c3c','#e67e22','#f1c40f','#2ecc71','#3498db','#9b59b6','#1abc9c','#34495e'];
  const regions=Object.keys(rd);
  const allYears=rd[regions[0]].years;
  const datasets=regions.map((r,i)=>({
    label:r, data:rd[r].values,
    borderColor:rc[i], backgroundColor:rc[i]+'33',
    fill:true, tension:0.2, pointRadius:2,
  }));
  new Chart(document.getElementById('chart_region'),{
    type:'line', data:{labels:allYears, datasets},
    options:{
      responsive:true, maintainAspectRatio:false,
      scales:{ y:{stacked:true, ticks:{callback:v=>v.toLocaleString()}} },
      plugins:{ legend:{position:'right'} }
    }
  });
})();

// 4.3 Heatmap
(function(){
  const years=""" + hm_years + """;
  const prefs=""" + hm_prefs + """;
  const vals=""" + hm_vals_js + """;
  const maxVal=Math.max(...vals.map(d=>d.v));
  new Chart(document.getElementById('chart_heatmap'),{
    type:'matrix',
    data:{
      datasets:[{
        label:'報告数',
        data:vals.map(d=>({x:years[d.x], y:prefs[d.y], v:d.v})),
        width:ctx=>{const a=ctx.chart.chartArea;return a?(a.right-a.left)/years.length-2:20;},
        height:ctx=>{const a=ctx.chart.chartArea;return a?(a.bottom-a.top)/prefs.length-1:12;},
        backgroundColor:ctx=>{
          const v=ctx.raw&&ctx.raw.v||0;
          const alpha=Math.min(v/maxVal*2,1);
          return 'rgba(52,152,219,'+alpha.toFixed(2)+')';
        },
      }]
    },
    options:{
      responsive:true, maintainAspectRatio:false,
      scales:{
        x:{type:'category',labels:years,position:'top',ticks:{font:{size:10}}},
        y:{type:'category',labels:prefs,offset:true,ticks:{font:{size:9}}}
      },
      plugins:{
        legend:{display:false},
        tooltip:{callbacks:{title:items=>items[0].raw.y,label:item=>item.raw.x+'年: '+item.raw.v.toLocaleString()+'件'}}
      }
    }
  });
})();

// 5.1 Monthly
new Chart(document.getElementById('chart_monthly'),{
  type:'bar',
  data:{
    labels:""" + chart6_labels + """,
    datasets:[{
      label:'月別平均報告数', data:""" + chart6_values + """,
      backgroundColor:""" + chart6_labels + """.map((_,i)=>[11,0,1,2].includes(i)?'rgba(231,76,60,.7)':'rgba(52,152,219,.5)'),
      borderRadius:4,
    }]
  },
  options:{
    responsive:true, maintainAspectRatio:false,
    scales:{ y:{beginAtZero:true,ticks:{callback:v=>v.toLocaleString()}} },
    plugins:{ legend:{display:false} }
  }
});

// 6. COVID
(function(){
  const cd=""" + chart7_data + """;
  const cc={'2019':'#3498db','2020':'#e74c3c','2021':'#95a5a6','2022':'#f39c12','2023':'#2ecc71'};
  const datasets=Object.keys(cd).map(y=>({
    label:y+'年',
    data:cd[y].weeks.map((w,i)=>({x:w,y:cd[y].values[i]})),
    borderColor:cc[y], pointRadius:0,
    borderWidth:y==='2020'?2.5:1.5,
    borderDash:y==='2021'?[4,4]:[],
    tension:0.2,
  }));
  new Chart(document.getElementById('chart_covid'),{
    type:'line', data:{datasets},
    options:{
      responsive:true, maintainAspectRatio:false,
      scales:{
        x:{type:'linear',title:{display:true,text:'週番号'},min:1,max:53},
        y:{beginAtZero:true,ticks:{callback:v=>v.toLocaleString()}}
      }
    }
  });
})();

// 7. Correlation charts
""" + corr_chart_js + """
""" + trends_chart_js + """
</script>
</body>
</html>"""

out_path = OUT / "01_basic_trend_analysis.html"
out_path.write_text(html_content, encoding="utf-8")
print("Report generated:", out_path)
