# -*- coding: utf-8 -*-
"""
Vulkanisations-Planer – Streamlit App
Zellen • Rüstzeiten • Werkzeuge • Vorziehen • Schicht-Planung • Gantt mit Ständen

Neu:
- Schichtweise Planung (Mo 06:00, 3×8h) mit synchronem Anlauf je Zelle in jeder Schicht
- Rüstzeiten vor Produktionsstart pro Presse; Rüstblöcke immer grau
- Erweiterte Slider-Bereiche für Σr und Spread

Start:
  python -m pip install --upgrade pip
  pip install streamlit pandas openpyxl xlsxwriter plotly
  python -m streamlit run app.py
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
st.caption("Excel laden, Bedarfe & Parameter setzen → Planung mit Zellen, Rüstzeiten, Vorziehen & Gantt")

with st.sidebar:
    st.header("1) Daten laden")
    up = st.file_uploader(
        "Excel-Vorlage (*.xlsx)",
        type=["xlsx"],
        help="Sheets: Pressen, Staende, Freigaben_Werkzeug_Presse, Zykluszeiten, Bedarfe_Woche, Kapazitaet_Woche. Optional: Werkzeuge (Artikel, Anzahl_Werkzeuge).",
    )

    st.header("2) Regeln / Zellen")
    # erweitere Bereiche
    sum_r_limit = st.slider("Σr-Limit pro Zelle", 0.50, 1.50, 1.10, 0.01)
    max_spread = st.slider("max. Schließzeit-Spread in Zelle", 0.0, 0.60, 0.25, 0.01, help="(Smax−Smin)/Smax")
    cell_strategy = st.selectbox("Zell-Strategie", ["3er → 2er → Einzel", "nur 2er → Einzel", "nur Einzel"])
    respect_stand_limit = st.checkbox("MaxMachinesPerWerker je Stand respektieren", True)
    allow_multiple_cells_per_stand = st.checkbox("Mehrere Zellen je Stand zulassen", True)
    enforce_freigefahren = st.checkbox("Nur freigefahrene Werkzeuge zulassen", True)

    st.header("3) Werkzeuge & Rüstzeiten")
    setup_minutes = st.number_input("Rüstzeit (Minuten) pro Werkzeugwechsel", 0, 24*60, 240, 10)
    limit_by_tools = st.checkbox("Anzahl gleichzeitig nutzbarer Werkzeuge je Artikel begrenzen (Sheet „Werkzeuge“)", True)

    st.header("4) Vorziehen (Zellen länger fahren)")
    allow_pull_ahead = st.checkbox("Bedarf aus KW+1/+2 vorziehen, um gute Zellen zu verlängern", True)
    pull_weeks = st.number_input("Wie viele KWs nach vorn ziehen (max.)", 0, 8, 2)

    st.header("5) Gantt & Schichten")
    gantt_year = st.number_input("Jahr (ISO für KW/Schichten)", 2020, 2100, date.today().year, 1)
    shift_grid = st.checkbox("Schichtgitter (3×8h) anzeigen", True)
    shift_start_hour = st.number_input("Schichtstart Mo (Stunde)", 0, 23, 6)
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
    shift_grid: bool
    shift_start_hour: int

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
    z = zyklen[["Artikel","Gesamtzyklus","Schließzeit","r"]].copy()
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

def build_shifts(year: int, kw: int, start_hour: int) -> List[Tuple[datetime, datetime, str]]:
    """Liefer 21 Schichten (Mo..So × Früh/Spät/Nacht) ab Start Mo <start_hour>."""
    anchor = datetime.combine(date.fromisocalendar(year, kw, 1), time(hour=start_hour))
    names = ["Früh","Spät","Nacht"]
    shifts=[]
    for d in range(7):
        for s in range(3):
            s0 = anchor + timedelta(days=d, hours=8*s)
            s1 = s0 + timedelta(hours=8)
            shifts.append((s0, s1, f"{names[s]} Tag{d+1}"))
    return shifts

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
def plan_base(sheets: Dict[str, pd.DataFrame], params: PlanParams, bedarfe_override_kw: Optional[pd.DataFrame] = None):
    # Read
    pressen = sheets["Pressen"].copy()
    staende = sheets["Staende"].copy()
    freigaben = sheets["Freigaben_Werkzeug_Presse"].copy()
    zyklen = sheets["Zykluszeiten"].copy()
    bedarfe = sheets["Bedarfe_Woche"].copy()
    kap = sheets["Kapazitaet_Woche"].copy()

    # Werkzeuge
    tools_map: Dict[str,int] = defaultdict(lambda: 9999)
    w_sheet = None
    for cand in ["Werkzeuge", "Anzahl_Werkzeuge"]:
        if cand in sheets: w_sheet = sheets[cand].copy(); break
    if w_sheet is not None and params.limit_by_tools:
        w_sheet.columns=[c.strip() for c in w_sheet.columns]
        art_col="Artikel"
        cnt_col="Anzahl_Werkzeuge" if "Anzahl_Werkzeuge" in w_sheet.columns else \
                [c for c in w_sheet.columns if "anzahl" in c.lower()][0]
        tools_map.update({str(r[art_col]): int(r[cnt_col]) for _, r in w_sheet[[art_col,cnt_col]].dropna().iterrows()})

    # Normalize
    for df in (pressen, staende, freigaben, zyklen, bedarfe, kap):
        if "Artikel" in df.columns:
            df["Artikel"] = df["Artikel"].astype(str).str.strip()
        if "Pressenname" in df.columns:
            df["Pressenname"] = df["Pressenname"].astype(str).str.strip()

    pressen, freigaben = normalize_flags(pressen, freigaben)
    zyklen = build_time_features(zyklen)
    zyklen_u = agg_zyklen_unique(zyklen)

    # Filter
    if params.enforce_freigefahren:
        freigaben = freigaben[freigaben["Freigefahren_bool"]]
    if params.exclude:
        freigaben = freigaben[~freigaben["Artikel"].isin(params.exclude)]
        zyklen_u = zyklen_u[~zyklen_u["Artikel"].isin(params.exclude)]
        bedarfe  = bedarfe[~bedarfe["Artikel"].isin(params.exclude)]

    bedarfe["KW_num"] = pd.to_numeric(bedarfe["KW"], errors="coerce")
    if params.kw not in set(bedarfe["KW_num"].dropna().astype(int)):
        raise ValueError(f"KW {params.kw} ist in Bedarfe_Woche nicht vorhanden.")
    if bedarfe_override_kw is not None:
        bed_map = dict(zip(bedarfe_override_kw["Artikel"], bedarfe_override_kw["Bedarf"]))
        mask = bedarfe["KW_num"]==params.kw
        bedarfe.loc[mask, "Bedarf"] = bedarfe.loc[mask, "Artikel"].map(bed_map).fillna(bedarfe.loc[mask, "Bedarf"])

    # Kapazität
    pressen_active = pressen[pressen["Aktiv_bool"]][["PressID","Pressenname","StandID"]]
    kap_active = kap.merge(pressen_active[["PressID"]], left_on="Presse_ID", right_on="PressID", how="inner")
    kap_active["Avail_min"] = kap_active["Verfuegbare_Minuten"].fillna(0) - kap_active["Wartung_Minuten"].fillna(0)

    # Zulässige Kombinationen (aktuelle KW)
    rel = (
        bedarfe[bedarfe["KW_num"]==params.kw][["Artikel","Bedarf"]]
        .merge(freigaben[["Artikel","Pressenname","Cavity"]], on="Artikel", how="inner")
        .merge(pressen_active, on="Pressenname", how="inner")
        .merge(zyklen_u[["Artikel","Schließzeit","r","Gesamtzyklus"]], on="Artikel", how="inner")
    )
    if rel.empty:
        raise RuntimeError("Keine relevanten Datensätze (Bedarf + zulässige Pressen + Zeiten).")

    # Demand-Minuten KW
    cavity_per_art = rel.groupby("Artikel")["Cavity"].max().to_dict()
    dem = demand_minutes(bedarfe[bedarfe["KW_num"]==params.kw][["Artikel","Bedarf"]], zyklen_u, cavity_per_art)

    r_map = dem.set_index("Artikel")["r"].to_dict()
    s_map = dem.set_index("Artikel")["Schließzeit"].to_dict()
    remain_min: Dict[str, float] = dem.set_index("Artikel")["Bedarfsminuten"].to_dict()

    # Pull-ahead Pools
    future_pool_by_kw: Dict[int, Dict[str,float]] = {}
    if params.allow_pull_ahead and params.pull_weeks>0:
        future_kws = [params.kw + i for i in range(1, params.pull_weeks+1)]
        bed_future = bedarfe[bedarfe["KW_num"].isin(future_kws)][["Artikel","KW_num","Bedarf"]].copy()
        if not bed_future.empty:
            cav_map = (
                freigaben.merge(pressen_active, on="Pressenname", how="inner")
                         .groupby("Artikel")["Cavity"].max()
                         .to_dict()
            )
            gz_map = zyklen_u.set_index("Artikel")["Gesamtzyklus"].to_dict()
            bed_future["Cavity"] = bed_future["Artikel"].map(cav_map)
            bed_future["GZ"]     = bed_future["Artikel"].map(gz_map)
            bed_future = bed_future.dropna(subset=["Cavity","GZ"]).copy()
            bed_future["Min_pro_Stk"]   = (bed_future["GZ"]/bed_future["Cavity"])/60.0
            bed_future["Bedarfsminuten"] = bed_future["Bedarf"]*bed_future["Min_pro_Stk"]
            for kwv, g in bed_future.groupby("KW_num"):
                future_pool_by_kw[int(kwv)] = dict(zip(g["Artikel"], g["Bedarfsminuten"]))

    def pull_from_future(artikel: str, needed_min: float) -> List[Tuple[int, float]]:
        pulled=[]
        if needed_min<=0: return pulled
        for kwv in sorted(future_pool_by_kw.keys()):
            avail = future_pool_by_kw[kwv].get(artikel, 0.0)
            if avail<=1e-6: continue
            take = min(needed_min, avail)
            if take>0:
                future_pool_by_kw[kwv][artikel] = avail - take
                pulled.append((kwv, take))
                needed_min -= take
                if needed_min<=1e-6: break
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

    # Kapazität
    capacity_left: Dict[str, float] = dict(zip(kap_active["Presse_ID"], kap_active["Avail_min"]))

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
        if press not in presses_for_article[artikel] and len(presses_for_article[artikel]) >= tools_limit[artikel]:
            return 0.0
        take = min(minutes, capacity_left.get(press,0.0), remain_min.get(artikel,0.0))
        if take<=0: return 0.0
        plan_rows_all.append({"StandID":stand_id,"Presse":press,"Artikel":artikel,
                              "Dauer_min":float(take),"CellID":cell_id,"QuelleKW":None})
        capacity_left[press] = capacity_left.get(press,0.0) - take
        remain_min[artikel] = remain_min.get(artikel,0.0) - take
        presses_for_article[artikel].add(press)
        return take

    for a in articles:
        need = remain_min.get(a,0.0)
        if need<=1e-6: continue
        poss = rel[rel["Artikel"]==a][["StandID","PressID"]].drop_duplicates().copy()
        poss["Cap_left"] = poss["PressID"].astype(str).map(capacity_left).fillna(0.0)
        poss = poss.sort_values("Cap_left", ascending=False)
        for _, r in poss.iterrows():
            if need<=1e-6: break
            take = add_plan(str(r["StandID"]), str(r["PressID"]), a, need, cell_id=f"Single-{a}")
            need -= take

    cells_df = pd.concat(cell_frames, ignore_index=True) if cell_frames else pd.DataFrame(columns=["Hinweis"], data=[["Keine Zellen gebildet"]])
    plan_df = pd.DataFrame(plan_rows_all, columns=["StandID","Presse","Artikel","Dauer_min","CellID","QuelleKW"])

    # Vorziehen auf Zellen-Pressen
    pulled_log = []
    if params.allow_pull_ahead and future_pool_by_kw and not cells_df.empty:
        # Cell → (Art,Presse)
        ext_mappings=[]
        for _, row in cells_df.iterrows():
            art_cols = sorted([c for c in row.index if str(c).startswith("Art")], key=lambda x: int(''.join(filter(str.isdigit, x)) or 0))
            prs_cols = sorted([c for c in row.index if str(c).startswith("Presse")], key=lambda x: int(''.join(filter(str.isdigit, x)) or 0))
            pairs=[]
            for ac, pc in zip(art_cols, prs_cols):
                a = row.get(ac); p = row.get(pc)
                if pd.notna(a) and pd.notna(p):
                    pairs.append((str(a), str(p)))
            if pairs:
                ext_mappings.append((row["StandID"], row["CellID"], pairs))

        for stand_id, cell_id, pairs in ext_mappings:
            for art, press in pairs:
                cap = capacity_left.get(press, 0.0)
                if cap <= 1e-6: continue
                want = cap
                pulled = pull_from_future(art, want)
                for kw_src, mins in pulled:
                    plan_df.loc[len(plan_df)] = [stand_id, press, art, float(mins), cell_id, int(kw_src)]
                    capacity_left[press] = capacity_left.get(press,0.0) - mins
                    pulled_log.append((art, kw_src, params.kw, mins, stand_id, press, cell_id))

    # Namen ergänzen
    name_map = pressen_active.set_index("PressID")["Pressenname"].to_dict()
    stand_name_map = staende.set_index("StandID")["StandName"].to_dict()
    plan_df["Pressenname"] = plan_df["Presse"].astype(str).map(name_map)
    plan_df["StandName"]   = plan_df["StandID"].map(stand_name_map)

    kap_active = kap_active[["Presse_ID","Avail_min"]]

    return cells_df, plan_df, pulled_log, pressen_active, kap_active, stand_name_map, name_map

# ---- Schicht-Scheduler mit synchronem Zellen-Anlauf ----
def build_shift_schedule(plan_df: pd.DataFrame,
                         kap_active: pd.DataFrame,
                         params: PlanParams) -> pd.DataFrame:
    """Erzeugt schedule_df schichtweise. Zellen starten pro Schicht synchron."""
    shifts = build_shifts(params.gantt_year, params.kw, params.shift_start_hour)

    # Remaining je (CellID, Presse, Artikel)
    base = plan_df.groupby(["CellID","Presse","StandID","Pressenname","StandName","Artikel","QuelleKW"], dropna=False)["Dauer_min"].sum().reset_index()

    # Zellen & Singles trennen
    is_single = base["CellID"].fillna("").str.startswith("Single")
    cells = base[~is_single].copy()
    singles = base[is_single].copy()

    # Map CellID → Liste Pressen mit Artikeln
    cell_map: Dict[str, List[Dict]] = {}
    for _, r in cells.iterrows():
        cid = str(r["CellID"])
        cell_map.setdefault(cid, []).append({
            "Presse": str(r["Presse"]),
            "Pressenname": r["Pressenname"],
            "StandID": r["StandID"],
            "StandName": r["StandName"],
            "Artikel": r["Artikel"],
            "QuelleKW": r["QuelleKW"],
            "rem": float(r["Dauer_min"])
        })

    # Singles: pro Presse FIFO
    single_map: Dict[str, List[Dict]] = defaultdict(list)
    for _, r in singles.iterrows():
        single_map[str(r["Presse"])].append({
            "Presse": str(r["Presse"]),
            "Pressenname": r["Pressenname"],
            "StandID": r["StandID"],
            "StandName": r["StandName"],
            "Artikel": r["Artikel"],
            "QuelleKW": r["QuelleKW"],
            "rem": float(r["Dauer_min"]),
            "CellID": str(r["CellID"])
        })

    # Reihenfolgen stabilisieren
    for k in single_map:
        single_map[k].sort(key=lambda x: (-x["rem"], x["Artikel"]))

    # Stand → Zellenliste (nur solche mit Rest)
    stand_cells: Dict[str, List[str]] = defaultdict(list)
    for cid, lst in cell_map.items():
        if not lst: continue
        stand_id = str(lst[0]["StandID"])
        if cid not in stand_cells[stand_id]:
            stand_cells[stand_id].append(cid)
    # sortiere Zellen pro Stand nach Summe Restminuten
    for sid in stand_cells:
        stand_cells[sid].sort(key=lambda cid: -sum(x["rem"] for x in cell_map[cid]))

    # Kapazität je Presse (für Info – Schichten begrenzen ohnehin)
    cap_map = dict(zip(kap_active["Presse_ID"].astype(str), kap_active["Avail_min"]))

    # prev-Artikel je Presse (für Rüst)
    prev_art: Dict[str, Optional[str]] = defaultdict(lambda: None)
    # Zeitzeiger je Presse – pro Schicht wird auf Shift-Start zurückgesetzt
    # (innerhalb einer Schicht läuft der Zeiger vor)
    sched_rows = []

    for (s0, s1, sname) in shifts:
        # Zeitzeiger zum Schichtbeginn
        t_ptr: Dict[str, datetime] = defaultdict(lambda: s0)

        # 1) Zellen pro Stand nacheinander einplanen
        for sid in sorted(stand_cells.keys(), key=lambda x: (int(x) if str(x).isdigit() else 9999)):
            for cid in list(stand_cells[sid]):  # iteriere über Zellen mit Rest
                lst = cell_map.get(cid, [])
                # Prüfe, ob in dieser Zelle auf allen Pressen noch Rest existiert
                active = [x for x in lst if x["rem"] > 1e-6]
                if len(active) == 0:
                    continue
                # Wir planen NUR, wenn mind. 2 Pressen Rest haben (Zelle sinnvoll)
                if len(active) == 1:
                    continue

                # Ready-Time je Presse inkl. Rüst (falls Artikelwechsel)
                ready_times = {}
                needs_setup = {}
                for x in active:
                    p = x["Presse"]; art = x["Artikel"]
                    needs = (prev_art[p] is not None and prev_art[p] != art)
                    needs_setup[p] = needs
                    rdy = t_ptr[p] + timedelta(minutes=params.setup_minutes) if needs and params.setup_minutes>0 else t_ptr[p]
                    ready_times[p] = rdy

                t0 = max(ready_times.values())
                if t0 >= s1:
                    continue  # in dieser Schicht kein Platz mehr

                # Blockdauer: synchron → min(Rest über aktive Pressen), zusätzlich Schichtrest
                min_rem = min(x["rem"] for x in active)
                block_max = (s1 - t0).total_seconds() / 60.0
                dur = max(0.0, min(min_rem, block_max))
                if dur <= 0.0:
                    continue

                # Rüstblöcke vorher je Presse (dort wo nötig) – evtl. führt das dazu,
                # dass einzelne Pressen vor t0 rüsten und dann warten (okay).
                for x in active:
                    p = x["Presse"]
                    if needs_setup.get(p, False) and params.setup_minutes>0:
                        # RÜSTEN von t_ptr[p] bis ready_times[p]
                        sched_rows.append({
                            "StandID": x["StandID"], "StandName": x["StandName"],
                            "Presse": p, "Pressenname": x["Pressenname"],
                            "Artikel": "RÜSTEN", "CellID": "SETUP",
                            "Start": t_ptr[p], "Ende": ready_times[p],
                            "Dauer_min": (ready_times[p] - t_ptr[p]).total_seconds()/60.0
                        })
                        t_ptr[p] = ready_times[p]

                # Produktionsblöcke synchron von t0 bis t0+dur
                for x in active:
                    p = x["Presse"]; art = x["Artikel"]; kw_src = x["QuelleKW"]
                    end = t0 + timedelta(minutes=dur)
                    label = art
                    if pd.notna(kw_src) and kw_src:
                        label = f"{art} [vorgezogen aus KW {int(kw_src)}]"
                    sched_rows.append({
                        "StandID": x["StandID"], "StandName": x["StandName"],
                        "Presse": p, "Pressenname": x["Pressenname"],
                        "Artikel": label, "CellID": cid,
                        "Start": t0, "Ende": end,
                        "Dauer_min": dur
                    })
                    x["rem"] -= dur
                    t_ptr[p] = end
                    prev_art[p] = art

        # 2) Rest-Schichtzeit je Presse mit Singles füllen
        for p, queue in single_map.items():
            # solange Schichtzeit übrig und Queue hat Rest
            while t_ptr[p] < s1 and any(item["rem"] > 1e-6 for item in queue):
                # pick Item mit größter Restdauer
                item = max(queue, key=lambda it: it["rem"])
                if item["rem"] <= 1e-6:
                    break
                # ggf. Rüstzeit
                art = item["Artikel"]
                t_ready = t_ptr[p]
                if prev_art[p] is not None and prev_art[p] != art and params.setup_minutes>0:
                    t_ready = t_ready + timedelta(minutes=params.setup_minutes)
                    if t_ready > s1:  # keine Zeit mehr in dieser Schicht
                        break
                    # Rüstblock
                    sched_rows.append({
                        "StandID": item["StandID"], "StandName": item["StandName"],
                        "Presse": p, "Pressenname": item["Pressenname"],
                        "Artikel": "RÜSTEN", "CellID": "SETUP",
                        "Start": t_ptr[p], "Ende": t_ready,
                        "Dauer_min": (t_ready - t_ptr[p]).total_seconds()/60.0
                    })
                    t_ptr[p] = t_ready
                # Produktionsblock
                free = (s1 - t_ptr[p]).total_seconds()/60.0
                if free <= 0: break
                dur = min(item["rem"], free)
                end = t_ptr[p] + timedelta(minutes=dur)
                label = art
                if pd.notna(item["QuelleKW"]) and item["QuelleKW"]:
                    label = f"{art} [vorgezogen aus KW {int(item['QuelleKW'])}]"
                sched_rows.append({
                    "StandID": item["StandID"], "StandName": item["StandName"],
                    "Presse": p, "Pressenname": item["Pressenname"],
                    "Artikel": label, "CellID": item["CellID"],
                    "Start": t_ptr[p], "Ende": end,
                    "Dauer_min": dur
                })
                item["rem"] -= dur
                t_ptr[p] = end
                prev_art[p] = art

    schedule_df = pd.DataFrame(sched_rows).sort_values(["StandID","Presse","Start"])
    return schedule_df

# -------- Ablauf --------
if up is None:
    st.info("Bitte Excel laden – danach Bedarfs-Editor, Parameter & Ergebnisse.")
    st.stop()

sheets = read_workbook(up)
need = {"Pressen","Staende","Freigaben_Werkzeug_Presse","Zykluszeiten","Bedarfe_Woche","Kapazitaet_Woche"}
missing = sorted(need - set(sheets))
if missing:
    st.error(f"Fehlende Sheets: {', '.join(missing)}")
    st.stop()

bedarfe_df = sheets["Bedarfe_Woche"].copy()
bedarfe_df["KW_num"] = pd.to_numeric(bedarfe_df["KW"], errors="coerce")
kw_options = sorted(set(bedarfe_df["KW_num"].dropna().astype(int)))
selected_kw = st.sidebar.selectbox("Kalenderwoche (KW)", kw_options, index=max(0, len(kw_options)-1))

kw_articles = sorted(set(bedarfe_df[bedarfe_df["KW_num"]==selected_kw]["Artikel"]))
selected_exclude = st.sidebar.multiselect("Artikel ausschließen", options=kw_articles, default=[a for a in kw_articles if a in exclude_default])

st.subheader(f"Bedarfe editieren – KW {selected_kw}")
bedarfe_kw = bedarfe_df[bedarfe_df["KW_num"]==selected_kw][["Artikel","Bedarf"]].copy().reset_index(drop=True)
edited_bedarfe_kw = st.data_editor(bedarfe_kw, num_rows="fixed", use_container_width=True, key="bedarfs_editor")

go = st.button("🔁 Planung erstellen / aktualisieren", type="primary")
if not go:
    st.info("Bedarfe ggf. anpassen und auf „Planung erstellen / aktualisieren“ klicken.")
    st.stop()

with st.spinner("Plane…"):
    params = PlanParams(
        kw=int(selected_kw),
        sum_r_limit=float(sum_r_limit),
        max_spread=float(max_spread),
        cell_strategy=str(cell_strategy),
        respect_stand_limit=bool(respect_stand_limit),
        allow_multiple_cells_per_stand=bool(allow_multiple_cells_per_stand),
        enforce_freigefahren=bool(enforce_freigefahren),
        exclude=set(selected_exclude),
        setup_minutes=int(setup_minutes),
        limit_by_tools=bool(limit_by_tools),
        allow_pull_ahead=bool(allow_pull_ahead),
        pull_weeks=int(pull_weeks),
        gantt_year=int(gantt_year),
        shift_grid=bool(shift_grid),
        shift_start_hour=int(shift_start_hour),
    )
    cells_df, plan_df, pulled_log, pressen_active, kap_active, stand_name_map, name_map = plan_base(sheets, params, bedarfe_override_kw=edited_bedarfe_kw)
    # Schichtweise Schedule
    schedule_df = build_shift_schedule(plan_df, kap_active, params)

# -------- Coverage & KPIs (aus Schedule) --------
if not schedule_df.empty:
    prod = schedule_df[schedule_df["Artikel"]!="RÜSTEN"].copy()
    prod["Artikel_join"] = prod["Artikel"].str.replace(r"\s+\[vorgezogen.*\]","", regex=True)
    prod_minutes = prod.groupby("Artikel_join")["Dauer_min"].sum()
else:
    prod_minutes = pd.Series(dtype=float)

# Demand erneut berechnen für Coverage
# zyklen_u + Bedarfe von oben nochmal (leicht redundant, aber simpel)
zyklen_u = agg_zyklen_unique(build_time_features(sheets["Zykluszeiten"].copy()))
bedarfe_now = sheets["Bedarfe_Woche"].copy()
bedarfe_now["KW_num"] = pd.to_numeric(bedarfe_now["KW"], errors="coerce")
bed_now_kw = bedarfe_now[bedarfe_now["KW_num"]==params.kw][["Artikel","Bedarf"]].copy()
rel_tmp = (
    bed_now_kw
    .merge(sheets["Freigaben_Werkzeug_Presse"][["Artikel","Pressenname","Cavity"]], on="Artikel", how="left")
)
cavity_per_art = rel_tmp.groupby("Artikel")["Cavity"].max().dropna().to_dict()
dem_now = demand_minutes(bed_now_kw, zyklen_u, cavity_per_art)
dem_now["Geplante_Minuten"] = dem_now["Artikel"].map(prod_minutes).fillna(0.0)
dem_now["Geplante_Stk"] = dem_now["Geplante_Minuten"] / dem_now["Min_pro_Stk"]
dem_now["Deckungsgrad_%"] = (dem_now["Geplante_Minuten"] / dem_now["Bedarfsminuten"]).clip(upper=1.0) * 100.0
dem_now["Restminuten"] = (dem_now["Bedarfsminuten"] - dem_now["Geplante_Minuten"]).clip(lower=0.0)
dem_now["Restmenge_Stk"] = dem_now["Restminuten"] / dem_now["Min_pro_Stk"]
art_df = dem_now[["Artikel","Bedarf","Min_pro_Stk","Bedarfsminuten","Geplante_Minuten","Geplante_Stk","Deckungsgrad_%","Restminuten","Restmenge_Stk"]]

# KPIs
total_demand_min = float(art_df["Bedarfsminuten"].sum())
total_planned_min = float(art_df["Geplante_Minuten"].sum())
total_cap_min = float(kap_active["Avail_min"].sum())
kpis = {
    "KW": params.kw,
    "Σ Bedarfsmin": total_demand_min,
    "Σ geplant (min)": total_planned_min,
    "Σ Kapazität (min)": total_cap_min,
    "Deckungsgrad gesamt": (total_planned_min / total_demand_min * 100.0) if total_demand_min>0 else 0.0,
}

# -------- Anzeigen --------
c1,c2,c3,c4 = st.columns(4)
c1.metric("KW", kpis["KW"])
c2.metric("Σ Bedarf (min)", f"{kpis['Σ Bedarfsmin']:.0f}")
c3.metric("Σ geplant (min)", f"{kpis['Σ geplant (min)']:.0f}")
c4.metric("Deckungsgrad gesamt", f"{kpis['Deckungsgrad gesamt']:.1f}%")

st.subheader("Gebildete Zellen")
st.dataframe(cells_df, use_container_width=True)

st.subheader("Plan je Presse / Artikel (Minuten)")
st.dataframe(plan_df[["StandID","Presse","Pressenname","Artikel","Dauer_min","CellID","QuelleKW"]], use_container_width=True)

st.subheader("Deckung je Artikel")
st.dataframe(art_df.sort_values(["Deckungsgrad_%","Bedarfsminuten"], ascending=[True, False]), use_container_width=True)

if not schedule_df.empty:
    # Y-Achse sortieren: StandID ↑, innerhalb Stand nach Pressenname ↑
    schedule_df = schedule_df.copy()
    schedule_df["YLabel"] = schedule_df.apply(lambda r: f"S{r['StandID']} | {r['StandName']} | {r['Pressenname']}", axis=1)
    yorder = schedule_df.drop_duplicates(subset=["Presse","YLabel"]).sort_values(["StandID","Pressenname"])["YLabel"].tolist()
    # Farb-Logik: RÜSTEN = grau; sonst eine Farbe pro Zelle (CellID)
    schedule_df["ColorKey"] = schedule_df["CellID"].where(schedule_df["Artikel"]!="RÜSTEN", other="SETUP")

    st.subheader("Ablaufplan je Presse (schichtweise, synchroner Zellenstart)")
    st.dataframe(schedule_df, use_container_width=True)

    # Gantt
    color_map = {"SETUP": "rgb(130,130,130)"}  # feste Farbe für Rüstwechsel
    fig = px.timeline(
        schedule_df,
        x_start="Start", x_end="Ende",
        y="YLabel",
        color="ColorKey",
        color_discrete_map=color_map,
        hover_data=["StandName","Presse","Artikel","Dauer_min","CellID"],
        title=f"Gantt – KW {selected_kw} (Start Mo {params.shift_start_hour:02d}:00)"
    )
    fig.update_yaxes(autorange="reversed", categoryorder="array", categoryarray=yorder)

    if params.shift_grid:
        # Schichtgitter (Mo Start)
        anchor = datetime.combine(date.fromisocalendar(params.gantt_year, params.kw, 1), time(hour=params.shift_start_hour))
        shapes=[]; labels=[]; names=["Früh","Spät","Nacht"]
        for d in range(7):
            for s in range(3):
                sh_start = anchor + timedelta(days=d, hours=8*s)
                sh_end   = sh_start + timedelta(hours=8)
                color = "rgba(120,120,120,0.06)" if s%2==0 else "rgba(170,170,170,0.06)"
                shapes.append(dict(type="rect", xref="x", yref="paper", x0=sh_start, x1=sh_end, y0=0, y1=1,
                                   fillcolor=color, line=dict(width=0)))
                labels.append((sh_start + timedelta(hours=4), names[s]))
        fig.update_layout(shapes=shapes)
        for x, txt in labels:
            fig.add_annotation(x=x, y=1.02, xref="x", yref="paper", text=txt, showarrow=False, font=dict(size=10))

    st.plotly_chart(fig, use_container_width=True, theme="streamlit")

# Vorzüge anzeigen
pulled_df = pd.DataFrame(pulled_log, columns=["Artikel","QuelleKW","ZielKW","Minuten","StandID","Presse","CellID"]) if 'pulled_log' in locals() else pd.DataFrame()
if not pulled_df.empty:
    pulled_df["Pressenname"] = pulled_df["Presse"].astype(str).map(name_map)
    pulled_df["StandName"] = pulled_df["StandID"].map(stand_name_map)
    st.subheader("Vorzeitig gedeckte Bedarfe (aus zukünftigen KWs vorgezogen)")
    st.dataframe(pulled_df.groupby(["Artikel","QuelleKW"]).agg(Minuten=("Minuten","sum")).reset_index(), use_container_width=True)

# Downloads
st.subheader("Downloads")
def to_xlsx_bytes() -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        (cells_df if not cells_df.empty else pd.DataFrame({"Hinweis":["Keine Zellen gebildet"]})).to_excel(writer, sheet_name="Zellen", index=False)
        plan_df.to_excel(writer, sheet_name="Plan_je_Presse", index=False)
        (schedule_df if not schedule_df.empty else pd.DataFrame({"Hinweis":["Kein Schedule"]})).to_excel(writer, sheet_name="Schedule", index=False)
        if not pulled_df.empty:
            pulled_df.to_excel(writer, sheet_name="Vorzuege", index=False)
        art_df.to_excel(writer, sheet_name="Coverage_je_Artikel", index=False)
    return output.getvalue()

st.download_button("📥 Excel-Export", data=to_xlsx_bytes(),
                   file_name=f"Plan_KW{selected_kw}.xlsx",
                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
st.download_button("📥 Plan (CSV)", data=plan_df.to_csv(index=False).encode("utf-8"),
                   file_name=f"Plan_KW{selected_kw}.csv", mime="text/csv")
