# -*- coding: utf-8 -*-
"""
Vulkanisations-Planer – Streamlit App (Shift-Scheduler: synchrone Zellenstarts je Schicht, Ad-hoc Zellen)
"""

from __future__ import annotations

import itertools
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, time
from io import BytesIO
from typing import Dict, List, Tuple, Set, Optional

import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------- UI ----------------
st.set_page_config(page_title="Vulkanisations-Planer", layout="wide")
st.title("🧰 Vulkanisations-Planer")
st.caption("Zellenbildung + schichtweise Planung mit synchronen Starts, Rüstungen & Ad-hoc-Zellen")

with st.sidebar:
    st.header("1) Daten laden")
    up = st.file_uploader(
        "Excel-Vorlage (*.xlsx)",
        type=["xlsx"],
        help="Sheets: Pressen, Staende, Freigaben_Werkzeug_Presse, Zykluszeiten, Bedarfe_Woche, Kapazitaet_Woche. Optional: Werkzeuge (Artikel, Anzahl_Werkzeuge).",
    )

    st.header("2) Regeln / Zellen")
    sum_r_limit = st.slider("Σr-Limit pro Zelle", 0.80, 1.20, 1.00, 0.01)
    max_spread = st.slider("max. Schließzeit-Spread in Zelle", 0.0, 0.30, 0.15, 0.01, help="(Smax−Smin)/Smax")
    cell_strategy = st.selectbox("Zell-Strategie", ["3er → 2er → Einzel", "nur 2er → Einzel", "nur Einzel"])
    respect_stand_limit = st.checkbox("MaxMachinesPerWerker je Stand respektieren", True)
    allow_multiple_cells_per_stand = st.checkbox("Mehrere Zellen je Stand zulassen", True)
    enforce_freigefahren = st.checkbox("Nur freigefahrene Werkzeuge zulassen", True)

    st.header("3) Werkzeuge & Rüstzeiten")
    setup_minutes = st.number_input("Rüstzeit (Minuten) pro Werkzeugwechsel", 0, 24*60, 240, 10)
    limit_by_tools = st.checkbox("Anzahl gleichzeitig nutzbarer Werkzeuge je Artikel begrenzen (Sheet „Werkzeuge“)", True)

    st.header("4) Vorziehen (Zellen länger fahren)")
    allow_pull_ahead = st.checkbox("Bedarf aus KW+1/+2 vorziehen", True)
    pull_weeks = st.number_input("Wie viele KWs nach vorn ziehen (max.)", 0, 8, 2)

    st.header("5) Gantt & Schichten")
    gantt_year = st.number_input("Jahr (ISO für KW/Schichten)", 2020, 2100, date.today().year, 1)
    shift_start_hour = st.number_input("Schichtstart Mo (Stunde)", 0, 23, 6)
    show_shift_grid = st.checkbox("Schichtgitter (3×8h) anzeigen", True)

    st.header("6) Ad-hoc Zellen in Schicht")
    ad_hoc_relaxed_sum_r = st.slider("Relaxed Σr-Limit (ad-hoc in Schicht)", 0.80, 1.50, 1.20, 0.01)
    ad_hoc_relaxed_spread = st.slider("Relaxed Spread (ad-hoc in Schicht)", 0.0, 0.60, 0.30, 0.01)
    ad_hoc_enable = st.checkbox("Ad-hoc Zellen in laufender Schicht erlauben", True)

    exclude_default = {"UTK0035", "U0210160"}

# -------------- helpers --------------
@st.cache_data(show_spinner=False)
def read_workbook(file) -> Dict[str, pd.DataFrame]:
    xls = pd.ExcelFile(file)
    return {name: pd.read_excel(xls, name) for name in xls.sheet_names}

@dataclass
class PlanParams:
    kw: int
    sum_r_limit: float
    max_spread: float
    cell_strategy: str
    respect_stand_limit: bool
    allow_multiple_cells_per_stand: bool
    enforce_freigefahren: bool
    exclude: Set[str]
    setup_minutes: int
    limit_by_tools: bool
    allow_pull_ahead: bool
    pull_weeks: int
    gantt_year: int
    shift_start_hour: int
    show_shift_grid: bool
    ad_hoc_relaxed_sum_r: float
    ad_hoc_relaxed_spread: float
    ad_hoc_enable: bool

def normalize_flags(pressen: pd.DataFrame, freigaben: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    pressen = pressen.copy(); freigaben = freigaben.copy()
    for df in (pressen, freigaben):
        df["Pressenname"] = df["Pressenname"].astype(str).str.strip()
    pressen["Aktiv_bool"] = pressen["Aktiv"].astype(str).str.strip().str.lower().eq("ja")
    freigaben["Freigefahren_bool"] = (
        freigaben["Freigefahren"].astype(str).str.strip().str.lower()
        .map({"ja": True, "nein": False, "1": True, "0": False, "true": True, "false": False})
        .fillna(False)
    )
    return pressen, freigaben

def build_time_features(zyklen: pd.DataFrame) -> pd.DataFrame:
    z = zyklen.copy()
    if "Gesamtzyklus" not in z.columns:
        z["Gesamtzyklus"] = z[["Bedienzeit","Nebenzeit","Schließzeit"]].sum(axis=1)
    z["Servicezeit"] = z["Bedienzeit"] + z["Nebenzeit"]
    z["r"] = z["Servicezeit"] / z["Schließzeit"]
    return z

def agg_zyklen_unique(zyklen: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in ["Artikel","Gesamtzyklus","Schließzeit","r"] if c in zyklen.columns]
    z = zyklen[cols].copy()
    z = z.groupby("Artikel", as_index=False).agg({"Gesamtzyklus":"max","Schließzeit":"max","r":"max"})
    return z

def demand_minutes(bedarfe_kw: pd.DataFrame, zyklen_unique: pd.DataFrame, cavity_per_art: Dict[str, float]) -> pd.DataFrame:
    dem = bedarfe_kw.merge(zyklen_unique, on="Artikel", how="left")
    dem = dem[dem["Artikel"].isin(cavity_per_art.keys())].copy()
    dem["Cavity"] = dem["Artikel"].map(cavity_per_art)
    dem["Min_pro_Stk"] = (dem["Gesamtzyklus"]/dem["Cavity"])/60.0
    dem["Bedarfsminuten"] = dem["Bedarf"] * dem["Min_pro_Stk"]
    return dem

def spread_from_s(s_list: List[float]) -> float:
    smin, smax = min(s_list), max(s_list)
    return (smax - smin)/smax if smax else 0.0

# ---- Zellen-Builder (mit CellID und Werkzeuglimit) ----
def build_cells(
    k: int,
    articles: List[str],
    remain_min: Dict[str, float],
    r_map: Dict[str, float],
    s_map: Dict[str, float],
    allowed_by_stand: Dict[str, Dict[str, Set[str]]],
    presses_by_stand: Dict[str, List[str]],
    capacity_left: Dict[str, float],
    sum_r_limit: float,
    max_spread: float,
    respect_stand_limit: bool,
    stand_limit_map: Dict[str, int],
    allow_multiple_cells_per_stand: bool,
    tools_limit: Dict[str, int],
    presses_for_article: Dict[str, Set[str]],
) -> Tuple[pd.DataFrame, List[Dict]]:
    cell_rows: List[Dict] = []
    plan_rows_local: List[Dict] = []

    def add_plan(stand_id: str, press: str, artikel: str, minutes: float, cell_id: str):
        if press not in presses_for_article[artikel] and len(presses_for_article[artikel]) >= tools_limit[artikel]:
            return 0.0
        take = min(minutes, capacity_left.get(press, 0.0), remain_min.get(artikel, 0.0))
        if take <= 0:
            return 0.0
        plan_rows_local.append({
            "StandID": stand_id, "Presse": press, "Artikel": artikel,
            "Dauer_min": float(take), "CellID": cell_id, "QuelleKW": None
        })
        capacity_left[press] = capacity_left.get(press, 0.0) - take
        remain_min[artikel] = remain_min.get(artikel, 0.0) - take
        presses_for_article[artikel].add(press)
        return take

    used_stands_in_round: Set[str] = set()
    progress = True; cell_counter = 1

    while progress:
        progress = False; used_stands_in_round.clear()
        arts_left = [a for a in articles if remain_min.get(a,0.0)>1e-6]
        if len(arts_left) < k: break

        candidates: List[Tuple[Tuple[str,...], float, float]] = []
        for comb in itertools.combinations(arts_left, k):
            svals = [s_map.get(a) for a in comb]
            if any(pd.isna(s) for s in svals): continue
            sum_r = sum(r_map.get(a,0) for a in comb)
            sp = spread_from_s(svals)
            if sum_r <= sum_r_limit and sp <= max_spread:
                candidates.append((comb, sum_r, sp))
        if not candidates: break
        candidates.sort(key=lambda x: (-x[1], x[2], -sum(remain_min.get(a,0) for a in x[0])))

        for stand_id, presses in presses_by_stand.items():
            if respect_stand_limit and int(stand_limit_map.get(stand_id,k) or k) < k: continue
            if len(presses) < k: continue
            if not allow_multiple_cells_per_stand and stand_id in used_stands_in_round: continue

            chosen=None
            for comb, sum_r, sp in candidates:
                used=set(); mapping={}; feas=True
                for a in comb:
                    poss=[p for p in (allowed_by_stand[stand_id].get(a,set()) - used)
                          if p in presses and capacity_left.get(p,0)>1e-6]
                    if len(presses_for_article[a]) >= tools_limit[a]:
                        poss=[p for p in poss if p in presses_for_article[a]]
                    if not poss: feas=False; break
                    poss.sort(key=lambda p: -capacity_left.get(p,0))
                    pick=poss[0]; mapping[a]=pick; used.add(pick)
                if feas and len(mapping)==k:
                    chosen=(comb,sum_r,sp,mapping); break
            if not chosen: continue

            comb,sum_r,sp,mapping = chosen
            cell_id=f"{k}er-{stand_id}-{cell_counter}"; cell_counter+=1
            cell_rows.append({
                "CellID": cell_id, "StandID": stand_id, "Typ": f"{k}er",
                "Kombi_Artikel": " + ".join(comb), "Summe_r": sum_r, "Spread": sp,
                **{f"Art{i+1}": comb[i] for i in range(k)},
                **{f"Presse{i+1}": mapping[comb[i]] for i in range(k)},
            })
            for a in comb:
                add_plan(stand_id, mapping[a], a, remain_min.get(a,0.0), cell_id)
            used_stands_in_round.add(stand_id); progress=True

    return pd.DataFrame(cell_rows), plan_rows_local

# ---- Planung (inkl. Vorziehen) ----
def plan_all(sheets: Dict[str, pd.DataFrame], params: PlanParams, bedarfe_override_kw: Optional[pd.DataFrame] = None):
    # --- Read ---
    pressen = sheets["Pressen"].copy()
    staende = sheets["Staende"].copy()
    freigaben = sheets["Freigaben_Werkzeug_Presse"].copy()
    zyklen = sheets["Zykluszeiten"].copy()
    bedarfe = sheets["Bedarfe_Woche"].copy()
    kap = sheets["Kapazitaet_Woche"].copy()

    # optionales Werkzeuge-Sheet
    tools_map: Dict[str,int] = defaultdict(lambda: 9999)
    w_sheet = None
    for cand in ["Werkzeuge", "Anzahl_Werkzeuge"]:
        if cand in sheets: w_sheet = sheets[cand].copy(); break
    if w_sheet is not None and params.limit_by_tools:
        w_sheet.columns=[c.strip() for c in w_sheet.columns]
        art_col="Artikel"
        if "Anzahl_Werkzeuge" in w_sheet.columns:
            cnt_col="Anzahl_Werkzeuge"
        else:
            cnt_col=[c for c in w_sheet.columns if "anzahl" in c.lower()][0]
        tools_map.update({str(r[art_col]): int(r[cnt_col]) for _, r in w_sheet[[art_col,cnt_col]].dropna().iterrows()})

    # --- Normalize ---
    for df in (pressen, staende, freigaben, zyklen, bedarfe, kap):
        if "Artikel" in df.columns:
            df["Artikel"] = df["Artikel"].astype(str).str.strip()
        if "Pressenname" in df.columns:
            df["Pressenname"] = df["Pressenname"].astype(str).str.strip()

    pressen, freigaben = normalize_flags(pressen, freigaben)
    zyklen = build_time_features(zyklen)
    zyklen_u = agg_zyklen_unique(zyklen)

    # --- Filter ---
    if params.enforce_freigefahren:
        freigaben = freigaben[freigaben["Freigefahren_bool"]]
    if params.exclude:
        freigaben = freigaben[~freigaben["Artikel"].isin(params.exclude])]
        # ^^^ Tippfehler fixen, falls nötig (eckige Klammer stimmt)
        zyklen_u = zyklen_u[~zyklen_u["Artikel"].isin(params.exclude)]
        bedarfe  = bedarfe[~bedarfe["Artikel"].isin(params.exclude)]

    bedarfe["KW_num"] = pd.to_numeric(bedarfe["KW"], errors="coerce")
    if params.kw not in set(bedarfe["KW_num"].dropna().astype(int)):
        raise ValueError(f"KW {params.kw} ist in Bedarfe_Woche nicht vorhanden.")

    # Bedarfe-Override (Editor)
    if bedarfe_override_kw is not None:
        bedarfe = bedarfe.copy()
        mask = bedarfe["KW_num"]==params.kw
        bed_map = dict(zip(bedarfe_override_kw["Artikel"], bedarfe_override_kw["Bedarf"]))
        bedarfe.loc[mask, "Bedarf"] = bedarfe.loc[mask, "Artikel"].map(bed_map).fillna(bedarfe.loc[mask, "Bedarf"])

    # Kapazität
    pressen_active = pressen[pressen["Aktiv_bool"]][["PressID","Pressenname","StandID"]]
    kap_active = kap.merge(pressen_active[["PressID"]], left_on="Presse_ID", right_on="PressID", how="inner")
    kap_active["Avail_min"] = kap_active["Verfuegbare_Minuten"].fillna(0) - kap_active["Wartung_Minuten"].fillna(0)
    capacity_left: Dict[str, float] = dict(zip(kap_active["Presse_ID"], kap_active["Avail_min"]))

    # Relevante Kombinationen
    rel = (
        bedarfe[bedarfe["KW_num"]==params.kw][["Artikel","Bedarf"]]
        .merge(freigaben[["Artikel","Pressenname","Cavity"]], on="Artikel", how="inner")
        .merge(pressen_active, on="Pressenname", how="inner")
        .merge(zyklen_u[["Artikel","Schließzeit","r","Gesamtzyklus"]], on="Artikel", how="inner")
    )
    if rel.empty:
        raise RuntimeError("Keine relevanten Datensätze (Bedarf + zulässige Pressen + Zeiten).")

    # Demand-Minuten
    cavity_per_art = rel.groupby("Artikel")["Cavity"].max().to_dict()
    dem = demand_minutes(bedarfe[bedarfe["KW_num"]==params.kw][["Artikel","Bedarf"]], zyklen_u, cavity_per_art)

    r_map = dem.set_index("Artikel")["r"].to_dict()
    s_map = dem.set_index("Artikel")["Schließzeit"].to_dict()
    min_per_piece = dem.set_index("Artikel")["Min_pro_Stk"].to_dict()
    remain_min: Dict[str, float] = dem.set_index("Artikel")["Bedarfsminuten"].to_dict()

    # Pull-ahead Pools (KW+1..+N)
    future_pool_by_kw: Dict[int, Dict[str,float]] = {}
    if params.allow_pull_ahead and params.pull_weeks>0:
        future_kws = [params.kw + i for i in range(1, params.pull_weeks+1)]
        bed_future = bedarfe[bedarfe["KW_num"].isin(future_kws)][["Artikel","KW_num","Bedarf"]].copy()
        if not bed_future.empty:
            cav_map = (freigaben.merge(pressen_active, on="Pressenname", how="inner").groupby("Artikel")["Cavity"].max().to_dict())
            gz_map = zyklen_u.set_index("Artikel")["Gesamtzyklus"].to_dict()
            bed_future["Cavity"] = bed_future["Artikel"].map(cav_map)
            bed_future["GZ"]     = bed_future["Artikel"].map(gz_map)
            bed_future = bed_future.dropna(subset=["Cavity","GZ"]).copy()
            bed_future["Min_pro_Stk"]   = (bed_future["GZ"]/bed_future["Cavity"])/60.0
            bed_future["Bedarfsminuten"] = bed_future["Bedarf"]*bed_future["Min_pro_Stk"]
            for kwv, g in bed_future.groupby("KW_num"):
                future_pool_by_kw[int(kwv)] = dict(zip(g["Artikel"], g["Bedarfsminuten"]))

    def pull_from_future(artikel: str, need_min: float) -> List[Tuple[int, float]]:
        pulled=[]
        if need_min<=0: return pulled
        for kwv in sorted(future_pool_by_kw.keys()):
            avail = future_pool_by_kw[kwv].get(artikel, 0.0)
            if avail<=1e-6: continue
            take = min(need_min, avail)
            if take>0:
                future_pool_by_kw[kwv][artikel] = avail - take
                pulled.append((kwv, take))
                need_min -= take
                if need_min<=1e-6: break
        return pulled

    # allowed/presses/limits
    allowed_by_stand: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))
    for _, row in rel.iterrows():
        allowed_by_stand[row["StandID"]][row["Artikel"]].add(str(row["PressID"]))
    presses_by_stand = pressen_active.groupby("StandID")["PressID"].apply(lambda s: list(map(str, s))).to_dict()
    stand_limit_map = staende.set_index("StandID")["MaxMachinesPerWerker"].to_dict()

    articles = sorted(remain_min.keys())
    tools_limit = {a: int(tools_map.get(a, 9999)) for a in articles}
    presses_for_article: Dict[str, Set[str]] = {a: set() for a in articles}

    # Zellen bilden
    cell_frames=[]; plan_rows_all: List[Dict] = []
    def run_cells(k:int):
        nonlocal cell_frames, plan_rows_all
        c,p = build_cells(
            k, articles, remain_min, r_map, s_map, allowed_by_stand, presses_by_stand, capacity_left,
            params.sum_r_limit, params.max_spread, params.respect_stand_limit, stand_limit_map,
            params.allow_multiple_cells_per_stand, tools_limit, presses_for_article
        )
        if not c.empty: cell_frames.append(c)
        plan_rows_all.extend(p)

    if "3er" in params.cell_strategy: run_cells(3)
    if ("2er" in params.cell_strategy) or (params.cell_strategy=="nur 2er → Einzel"): run_cells(2)

    # Restbedarf → Einzel
    def add_plan(stand_id: str, press: str, artikel: str, minutes: float, cell_id: str):
        if press not in presses_for_article[artikel] and len(presses_for_article[artikel]) >= tool
