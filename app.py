# -*- coding: utf-8 -*-
"""
Vulkanisations-Planer – Streamlit App (Schichtplanung & synchronisierte Zellen)

Neu:
- Schichtbasierte Planung (Früh/Spät/Nacht, Start Mo 06:00, 8h)
- Zellen starten pro Schicht synchron (alle Pressen gleichzeitig)
- Ready-first: Zellen ohne Rüstbedarf bevorzugen; sonst Zellen mit Rüstungen, dann 2er/Singles
- Ad-hoc Fallback-Zellen je Schicht mit gelockerten Σr/Spread (optional Vorziehen aus KW+1/+2)
- RÜSTEN-Blöcke immer grau, Gantt nach Stand sortiert

Voraussetzungen:
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
st.caption("Schichtbasierte Planung mit synchronen Zellen, Rüstzeiten, Vorziehen & Gantt")

with st.sidebar:
    st.header("1) Datei")
    up = st.file_uploader(
        "Excel-Vorlage (*.xlsx)",
        type=["xlsx"],
        help="Sheets: Pressen, Staende, Freigaben_Werkzeug_Presse, Zykluszeiten, Bedarfe_Woche, Kapazitaet_Woche. Optional: Werkzeuge (Artikel, Anzahl_Werkzeuge).",
    )

    st.header("2) Zellen-Regeln")
    sum_r_limit = st.slider("Σr-Limit pro Zelle", 0.80, 1.20, 1.00, 0.01)
    max_spread = st.slider("max. Schließzeit-Spread", 0.0, 0.30, 0.15, 0.01, help="(Smax−Smin)/Smax")
    cell_strategy = st.selectbox("Zell-Strategie", ["3er → 2er → Einzel", "nur 2er → Einzel", "nur Einzel"])
    respect_stand_limit = st.checkbox("MaxMachinesPerWerker je Stand respektieren", True)
    allow_multiple_cells_per_stand = st.checkbox("Mehrere Zellen je Stand zulassen", True)
    enforce_freigefahren = st.checkbox("Nur freigefahrene Werkzeuge zulassen", True)

    st.header("3) Werkzeuge & Rüstzeiten")
    setup_minutes = st.number_input("Rüstzeit pro Wechsel (min)", 0, 24*60, 240, 10)
    limit_by_tools = st.checkbox("Werkzeuganzahl je Artikel begrenzen (Sheet „Werkzeuge“)", True)

    st.header("4) Vorziehen / Fallback")
    allow_pull_ahead = st.checkbox("Vorzuzug aus KW+1/+2 erlauben", True)
    pull_weeks = st.number_input("Max. KWs vorziehen", 0, 8, 2)
    relax_factor_sumr = st.slider("Relax Σr (nur Fallback)", 1.00, 1.50, 1.20, 0.01)
    relax_spread_add = st.slider("Relax Spread (nur Fallback, +…)", 0.00, 0.40, 0.10, 0.01)

    st.header("5) Schichten & Gantt")
    gantt_year = st.number_input("Jahr (ISO)", 2020, 2100, date.today().year, 1)
    shift_start_hour = st.number_input("Schichtstart Mo (Stunde)", 0, 23, 6)
    show_shift_grid = st.checkbox("Schichtgitter anzeigen", True)

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
    relax_factor_sumr: float
    relax_spread_add: float
    gantt_year: int
    shift_start_hour: int
    show_shift_grid: bool

def normalize_flags(pressen: pd.DataFrame, freigaben: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    pressen = pressen.copy(); freigaben = freigaben.copy()
    for df in (pressen, freigaben):
        if "Pressenname" in df.columns:
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

def make_shifts(year:int, kw:int, start_hour:int) -> List[Tuple[int, datetime, datetime, int, str]]:
    """21 Schichten ab Mo start_hour: 3×8h pro Tag, 7 Tage."""
    base = datetime.combine(date.fromisocalendar(year, kw, 1), time(hour=start_hour))
    names = ["Früh","Spät","Nacht"]
    shifts=[]
    idx=0
    for d in range(7):
        for s in range(3):
            stt = base + timedelta(days=d, hours=8*s)
            end = stt + timedelta(hours=8)
            shifts.append((idx, stt, end, d, names[s]))
            idx += 1
    return shifts

# ---- Zellen-Builder (pressenfix) für Vorplanung der Minuten ----
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
        if take <= 0: return 0.0
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
    # --- Daten laden ---
    pressen = sheets["Pressen"].copy()
    staende = sheets["Staende"].copy()
    freigaben = sheets["Freigaben_Werkzeug_Presse"].copy()
    zyklen = sheets["Zykluszeiten"].copy()
    bedarfe = sheets["Bedarfe_Woche"].copy()
    kap = sheets["Kapazitaet_Woche"].copy()

    # optional: Werkzeuge
    tools_map: Dict[str,int] = defaultdict(lambda: 9999)
    w_sheet = None
    for cand in ["Werkzeuge", "Anzahl_Werkzeuge"]:
        if cand in sheets: w_sheet = sheets[cand].copy(); break
    if w_sheet is not None and params.limit_by_tools:
        w_sheet.columns=[c.strip() for c in w_sheet.columns]
        art_col="Artikel"
        cnt_col = "Anzahl_Werkzeuge" if "Anzahl_Werkzeuge" in w_sheet.columns else \
                  [c for c in w_sheet.columns if "anzahl" in c.lower()][0]
        tools_map.update({str(r[art_col]): int(r[cnt_col]) for _, r in w_sheet[[art_col,cnt_col]].dropna().iterrows()})

    # Normalisieren
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
        zyklen_u  = zyklen_u[~zyklen_u["Artikel"].isin(params.exclude)]
        bedarfe   = bedarfe[~bedarfe["Artikel"].isin(params.exclude)]

    bedarfe["KW_num"] = pd.to_numeric(bedarfe["KW"], errors="coerce")
    if params.kw not in set(bedarfe["KW_num"].dropna().astype(int)):
        raise ValueError(f"KW {params.kw} ist in Bedarfe_Woche nicht vorhanden.")

    # Bedarfe-Override aus Editor
    if bedarfe_override_kw is not None:
        bedarfe = bedarfe.copy()
        mask = bedarfe["KW_num"]==params.kw
        bed_map = dict(zip(bedarfe_override_kw["Artikel"], bedarfe_override_kw["Bedarf"]))
        bedarfe.loc[mask, "Bedarf"] = bedarfe.loc[mask, "Artikel"].map(bed_map).fillna(bedarfe.loc[mask, "Bedarf"])

    # Kapazitäten
    pressen_active = pressen[pressen["Aktiv_bool"]][["PressID","Pressenname","StandID"]]
    kap_active = kap.merge(pressen_active[["PressID"]], left_on="Presse_ID", right_on="PressID", how="inner")
    kap_active["Avail_min"] = kap_active["Verfuegbare_Minuten"].fillna(0) - kap_active["Wartung_Minuten"].fillna(0)
    capacity_left: Dict[str, float] = dict(zip(kap_active["Presse_ID"], kap_active["Avail_min"]))

    # Zulässigkeiten/Zeiten
    rel = (
        bedarfe[bedarfe["KW_num"]==params.kw][["Artikel","Bedarf"]]
        .merge(freigaben[["Artikel","Pressenname","Cavity"]], on="Artikel", how="inner")
        .merge(pressen_active, on="Pressenname", how="inner")
        .merge(zyklen_u[["Artikel","Schließzeit","r","Gesamtzyklus"]], on="Artikel", how="inner")
    )
    if rel.empty:
        raise RuntimeError("Keine relevanten Datensätze (Bedarf + zulässige Pressen + Zeiten).")

    cavity_per_art = rel.groupby("Artikel")["Cavity"].max().to_dict()
    dem = demand_minutes(bedarfe[bedarfe["KW_num"]==params.kw][["Artikel","Bedarf"]], zyklen_u, cavity_per_art)

    r_map = dem.set_index("Artikel")["r"].to_dict()
    s_map = dem.set_index("Artikel")["Schließzeit"].to_dict()
    remain_min: Dict[str, float] = dem.set_index("Artikel")["Bedarfsminuten"].to_dict()

    # Pull-ahead Pools (KW+1..+N)
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
    def pull_from_future(artikel: str, need_min: float) -> List[Tuple[int, float]]:
        out=[]
        for kwv in sorted(future_pool_by_kw.keys()):
            if need_min<=1e-6: break
            avail = future_pool_by_kw[kwv].get(artikel, 0.0)
            if avail<=1e-6: continue
            take = min(need_min, avail)
            future_pool_by_kw[kwv][artikel] = avail - take
            out.append((kwv, take))
            need_min -= take
        return out

    # Standstruktur
    allowed_by_stand: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))
    for _, row in rel.iterrows():
        allowed_by_stand[row["StandID"]][row["Artikel"]].add(str(row["PressID"]))
    presses_by_stand = pressen_active.groupby("StandID")["PressID"].apply(lambda s: list(map(str, s))).to_dict()
    stand_limit_map = staende.set_index("StandID")["MaxMachinesPerWerker"].to_dict()

    articles = sorted(remain_min.keys())
    tools_limit = {a: int(tools_map.get(a, 9999)) for a in articles}
    presses_for_article: Dict[str, Set[str]] = {a: set() for a in articles}

    # Vorplanung: Zellen/Einzel auf Minuten verteilen (ohne Schichten)
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

    # Rest als Singles
    def add_single(stand_id: str, press: str, artikel: str, minutes: float):
        if press not in presses_for_article[artikel] and len(presses_for_article[artikel]) >= tools_limit[artikel]:
            return 0.0
        take = min(minutes, capacity_left.get(press,0.0), remain_min.get(artikel,0.0))
        if take<=0: return 0.0
        plan_rows_all.append({"StandID":stand_id,"Presse":press,"Artikel":artikel,
                              "Dauer_min":float(take),"CellID":f"Single-{artikel}","QuelleKW":None})
        capacity_left[press] = capacity_left.get(press,0.0) - take
        remain_min[artikel] = remain_min.get(artikel,0.0) - take
        presses_for_article[artikel].add(press)
        return take

    # naive Singles-Auffüllung
    for a in articles:
        need = remain_min.get(a,0.0)
        if need<=1e-6: continue
        poss = rel[rel["Artikel"]==a][["StandID","PressID"]].drop_duplicates().copy()
        poss["Cap_left"] = poss["PressID"].astype(str).map(capacity_left).fillna(0.0)
        poss = poss.sort_values("Cap_left", ascending=False)
        for _, r in poss.iterrows():
            if need<=1e-6: break
            take = add_single(str(r["StandID"]), str(r["PressID"]), a, need)
            need -= take

    cells_df = pd.concat(cell_frames, ignore_index=True) if cell_frames else pd.DataFrame(columns=["Hinweis"], data=[["Keine Zellen gebildet"]])
    plan_df = pd.DataFrame(plan_rows_all, columns=["StandID","Presse","Artikel","Dauer_min","CellID","QuelleKW"])

    # Namen
    name_map = pressen_active.set_index("PressID")["Pressenname"].to_dict()
    stand_name_map = staende.set_index("StandID")["StandName"].to_dict()
    plan_df["Pressenname"] = plan_df["Presse"].astype(str).map(name_map)
    plan_df["StandName"]   = plan_df["StandID"].map(stand_name_map)

    # ---- SCHICHTPLANER (synchron) ----
    shifts = make_shifts(params.gantt_year, params.kw, params.shift_start_hour)
    # Minutenpools je (CellID, Presse, Artikel)
    pool = plan_df.groupby(["CellID","Presse","Artikel"])["Dauer_min"].sum().to_dict()
    # Singles ermitteln
    single_keys = [k for k in pool.keys() if str(k[0]).startswith("Single-")]

    # Zellen-Mapping je Stand
    cells_by_stand: Dict[str, List[Dict]] = defaultdict(list)
    if not cells_df.empty and "CellID" in cells_df.columns:
        for _, row in cells_df.iterrows():
            # Art/Presse-Paare extrahieren
            arts = [c for c in row.index if str(c).startswith("Art")]
            prss = [c for c in row.index if str(c).startswith("Presse")]
            pairs=[]
            for i in range(min(len(arts), len(prss))):
                a = row.get(arts[i]); p = row.get(prss[i])
                if pd.notna(a) and pd.notna(p):
                    pairs.append((str(p), str(a)))
            if pairs:
                cells_by_stand[str(row["StandID"])].append({"cell_id": row["CellID"], "pairs": pairs})

    # Hilfen
    kap_total = kap_active.set_index("Presse_ID")["Avail_min"].to_dict()
    cap_left_for_schedule = kap_total.copy()
    prev_art_on_press: Dict[str, Optional[str]] = defaultdict(lambda: None)

    sched_rows=[]
    pulled_rows=[]

    def all_pairs_have_minutes(pairs):
        return all(pool.get((cell_id, p, a), 0.0) > 1e-6 for (p,a) in pairs)

    def feasible_runtime(pairs, shift_minutes):
        # gleicher Laufzeitblock für alle Pressen in der Zelle
        mins = min(
            [pool.get((cell_id, p, a), 0.0) for (p,a) in pairs] +
            [cap_left_for_schedule.get(p,0.0) for (p,_) in pairs] +
            [shift_minutes]
        )
        return float(max(0.0, mins))

    # Fallback-Kombinatorik (ad-hoc) mit Relax
    def build_fallback_cell(stand_id: str, available_presses: List[str], shift_minutes: float):
        # k bevorzugt 3, sonst 2, sonst 1
        for k in [3,2,1]:
            if len(available_presses) < k: continue
            # Kandidaten-Artikel: solche, die auf mind. einer freien Presse im Stand zugelassen sind
            arts = set()
            for a,_ in r_map.items():
                if any(p in allowed_by_stand[stand_id].get(a, set()) for p in available_presses):
                    arts.add(a)
            arts = sorted(list(arts))
            if len(arts) < k: continue
            best=None
            for comb in itertools.combinations(arts, k):
                # Σr/Spread mit relax prüfen
                sumr = sum(r_map.get(a,0.0) for a in comb)
                svals = [s_map.get(a,0.0) for a in comb]
                sp = spread_from_s(svals)
                if sumr <= params.sum_r_limit*params.relax_factor_sumr and sp <= min(0.5, params.max_spread+params.relax_spread_add):
                    # Pressen zuordnen (greedy nach Kapazität)
                    used=set(); mapping=[]
                    for a in comb:
                        poss=[p for p in allowed_by_stand[stand_id].get(a, set()) if p in available_presses and p not in used and cap_left_for_schedule.get(p,0.0)>1e-6]
                        if not poss: mapping=[]; break
                        poss.sort(key=lambda p: -cap_left_for_schedule.get(p,0.0))
                        mapping.append((poss[0], a)); used.add(poss[0])
                    if len(mapping)==k:
                        best = mapping; break
            if best:
                # Minutenquelle = future_pool (falls erlaubt), sonst 0
                need = min(shift_minutes, *[cap_left_for_schedule.get(p,0.0) for p,_ in best])
                # Pull pro Artikel
                pulled_total=[]
                for p,a in best:
                    pulled = pull_from_future(a, need)
                    if not pulled: pulled_total=[]; break
                    pulled_total.append((p,a,pulled))
                if pulled_total:
                    # schreibe in pool (CellID = Fallback-StandID-<idx>)
                    fallback_id = f"Fallback-{stand_id}"
                    take = min([sum(x[2][0][1] for x in pulled_total)] + [need])  # grob
                    for p,a,pul in pulled_total:
                        got = sum(q for _,q in pul)
                        pool[(fallback_id, p, a)] = pool.get((fallback_id,p,a), 0.0) + min(got, take)
                        for kwsrc, q in pul:
                            pulled_rows.append((a, kwsrc, params.kw, q, stand_id, p, fallback_id))
                    return {"cell_id": fallback_id, "pairs": best}
        return None

    # Haupt-Schleife: pro Stand, pro Schicht
    for stand_id, press_list in presses_by_stand.items():
        press_list = [str(p) for p in press_list]
        cell_list = cells_by_stand.get(str(stand_id), [])
        for (shift_idx, sh_start, sh_end, day, sh_name) in shifts:
            t = sh_start
            while t < sh_end:
                # Freie Pressen (mit Restkapazität) zu Beginn dieses Blocks
                free_presses = [p for p in press_list if cap_left_for_schedule.get(p,0.0) > 1e-6]
                if not free_presses:
                    break
                remaining_in_shift = (sh_end - t).total_seconds()/60.0
                if remaining_in_shift <= 1e-6:
                    break

                # 1) Ready 3er/2er-Zellen ohne Rüstbedarf (alle Pressen gleiches prev_art)
                cand=[]
                for cell in cell_list:
                    cell_id = cell["cell_id"]; pairs = [(p,a) for (p,a) in cell["pairs"] if p in free_presses]
                    # brauchbar nur, wenn alle Paarungen abgedeckt sind (volle Zelle) und Minuten vorhanden
                    if len(pairs) != len(cell["pairs"]): continue
                    if not all_pairs_have_minutes(pairs): continue
                    # ready? kein Rüstbedarf auf irgendeiner Presse
                    if any(prev_art_on_press.get(p) not in (None, a) for (p,a) in pairs):
                        continue
                    run = feasible_runtime(pairs, remaining_in_shift)
                    if run > 1e-6:
                        cand.append((run, cell_id, pairs))
                if cand:
                    # längsten Block fahren
                    cand.sort(key=lambda x: -x[0])
                    run, cell_id, pairs = cand[0]
                    # schreiben
                    for (p,a) in pairs:
                        sched_rows.append({
                            "StandID": stand_id, "StandName": stand_name_map.get(stand_id, f"S{stand_id}"),
                            "Presse": p, "Pressenname": name_map.get(p, p),
                            "Artikel": a, "CellID": cell_id,
                            "Start": t, "Ende": t + timedelta(minutes=run), "Dauer_min": run
                        })
                        pool[(cell_id,p,a)] = pool.get((cell_id,p,a),0.0) - run
                        cap_left_for_schedule[p] = cap_left_for_schedule.get(p,0.0) - run
                        prev_art_on_press[p] = a
                    t = t + timedelta(minutes=run)
                    continue

                # 2) Zellen mit Rüstbedarf (synchron RÜSTEN -> dann Lauf)
                cand=[]
                for cell in cell_list:
                    cell_id = cell["cell_id"]; pairs = [(p,a) for (p,a) in cell["pairs"] if p in free_presses]
                    if len(pairs) != len(cell["pairs"]): continue
                    if not all_pairs_have_minutes(pairs): continue
                    # wenn Rüstzeit in dieser Schicht nicht mehr reinpasst, überspringen
                    if remaining_in_shift <= params.setup_minutes+1:
                        continue
                    run = feasible_runtime(pairs, remaining_in_shift - params.setup_minutes)
                    if run > 1e-6:
                        cand.append((run, cell_id, pairs))
                if cand:
                    cand.sort(key=lambda x: -x[0])
                    run, cell_id, pairs = cand[0]
                    # RÜSTEN synchron
                    for (p, _) in pairs:
                        sched_rows.append({
                            "StandID": stand_id, "StandName": stand_name_map.get(stand_id, f"S{stand_id}"),
                            "Presse": p, "Pressenname": name_map.get(p, p),
                            "Artikel": "RÜSTEN", "CellID": "SETUP",
                            "Start": t, "Ende": t + timedelta(minutes=params.setup_minutes), "Dauer_min": float(params.setup_minutes)
                        })
                        cap_left_for_schedule[p] = cap_left_for_schedule.get(p,0.0) - params.setup_minutes
                        # prev_art bleibt bestehen; Rüstwechsel wird beim Start gesetzt
                    t = t + timedelta(minutes=params.setup_minutes)
                    # Produktion synchron
                    for (p,a) in pairs:
                        sched_rows.append({
                            "StandID": stand_id, "StandName": stand_name_map.get(stand_id, f"S{stand_id}"),
                            "Presse": p, "Pressenname": name_map.get(p, p),
                            "Artikel": a, "CellID": cell_id,
                            "Start": t, "Ende": t + timedelta(minutes=run), "Dauer_min": run
                        })
                        pool[(cell_id,p,a)] = pool.get((cell_id,p,a),0.0) - run
                        cap_left_for_schedule[p] = cap_left_for_schedule.get(p,0.0) - run
                        prev_art_on_press[p] = a
                    t = t + timedelta(minutes=run)
                    continue

                # 3) 2er-Zellen oder Singles aus dem Pool (geplant) – ohne Setup bevorzugen
                # 3a) Singles ohne Rüst
                single_cand=[]
                for (cell_id,p,a) in single_keys:
                    if p not in free_presses: continue
                    if pool.get((cell_id,p,a),0.0) <= 1e-6: continue
                    if prev_art_on_press.get(p) not in (None, a): continue
                    run = min(pool.get((cell_id,p,a),0.0), cap_left_for_schedule.get(p,0.0), remaining_in_shift)
                    if run>1e-6:
                        single_cand.append((run, cell_id, p, a))
                if single_cand:
                    single_cand.sort(key=lambda x: -x[0])
                    run, cell_id, p, a = single_cand[0]
                    sched_rows.append({
                        "StandID": stand_id, "StandName": stand_name_map.get(stand_id, f"S{stand_id}"),
                        "Presse": p, "Pressenname": name_map.get(p, p),
                        "Artikel": a, "CellID": cell_id,
                        "Start": t, "Ende": t + timedelta(minutes=run), "Dauer_min": run
                    })
                    pool[(cell_id,p,a)] -= run
                    cap_left_for_schedule[p] -= run
                    prev_art_on_press[p] = a
                    t = t + timedelta(minutes=run)
                    continue

                # 3b) Singles mit Setup (falls noch Zeit)
                single_cand=[]
                for (cell_id,p,a) in single_keys:
                    if p not in free_presses: continue
                    if pool.get((cell_id,p,a),0.0) <= 1e-6: continue
                    if remaining_in_shift <= params.setup_minutes+1: continue
                    run = min(pool.get((cell_id,p,a),0.0), cap_left_for_schedule.get(p,0.0), remaining_in_shift - params.setup_minutes)
                    if run>1e-6:
                        single_cand.append((run, cell_id, p, a))
                if single_cand:
                    single_cand.sort(key=lambda x: -x[0])
                    run, cell_id, p, a = single_cand[0]
                    # Rüst
                    sched_rows.append({
                        "StandID": stand_id, "StandName": stand_name_map.get(stand_id, f"S{stand_id}"),
                        "Presse": p, "Pressenname": name_map.get(p, p),
                        "Artikel": "RÜSTEN", "CellID": "SETUP",
                        "Start": t, "Ende": t + timedelta(minutes=params.setup_minutes), "Dauer_min": float(params.setup_minutes)
                    })
                    cap_left_for_schedule[p] -= params.setup_minutes
                    t = t + timedelta(minutes=params.setup_minutes)
                    # Prod
                    sched_rows.append({
                        "StandID": stand_id, "StandName": stand_name_map.get(stand_id, f"S{stand_id}"),
                        "Presse": p, "Pressenname": name_map.get(p, p),
                        "Artikel": a, "CellID": cell_id,
                        "Start": t, "Ende": t + timedelta(minutes=run), "Dauer_min": run
                    })
                    pool[(cell_id,p,a)] -= run
                    cap_left_for_schedule[p] -= run
                    prev_art_on_press[p] = a
                    t = t + timedelta(minutes=run)
                    continue

                # 4) Ad-hoc Fallback-Zelle (mit relax + Vorziehen), wenn nichts anderes geht
                fb = build_fallback_cell(str(stand_id), free_presses, remaining_in_shift)
                if fb:
                    cell_id = fb["cell_id"]; pairs = fb["pairs"]
                    # ready?
                    needs_setup = any(prev_art_on_press.get(p) not in (None, a) for (p,a) in pairs)
                    if needs_setup and remaining_in_shift <= params.setup_minutes+1:
                        break  # diese Schicht nichts mehr sinnvoll
                    max_run = min([pool.get((cell_id,p,a),0.0) for (p,a) in pairs] + [cap_left_for_schedule.get(p,0.0) for (p,_) in pairs])
                    if needs_setup:
                        # Rüst synchron
                        for (p,_) in pairs:
                            sched_rows.append({
                                "StandID": stand_id, "StandName": stand_name_map.get(stand_id, f"S{stand_id}"),
                                "Presse": p, "Pressenname": name_map.get(p, p),
                                "Artikel": "RÜSTEN", "CellID": "SETUP",
                                "Start": t, "Ende": t + timedelta(minutes=params.setup_minutes), "Dauer_min": float(params.setup_minutes)
                            })
                            cap_left_for_schedule[p] -= params.setup_minutes
                        t = t + timedelta(minutes=params.setup_minutes)
                        remaining_in_shift = (sh_end - t).total_seconds()/60.0
                    run = min(max_run, remaining_in_shift)
                    if run>1e-6:
                        for (p,a) in pairs:
                            sched_rows.append({
                                "StandID": stand_id, "StandName": stand_name_map.get(stand_id, f"S{stand_id}"),
                                "Presse": p, "Pressenname": name_map.get(p, p),
                                "Artikel": f"{a} [ad-hoc]", "CellID": cell_id,
                                "Start": t, "Ende": t + timedelta(minutes=run), "Dauer_min": run
                            })
                            pool[(cell_id,p,a)] = pool.get((cell_id,p,a),0.0) - run
                            cap_left_for_schedule[p] -= run
                            prev_art_on_press[p] = a
                        t = t + timedelta(minutes=run)
                        continue

                # 5) Nichts mehr machbar → Schichtende für diesen Stand
                break

    schedule_df = pd.DataFrame(sched_rows).sort_values(["StandID","Presse","Start"])
    pulled_df = pd.DataFrame(pulled_rows, columns=["Artikel","QuelleKW","ZielKW","Minuten","StandID","Presse","CellID"]) if pulled_rows else pd.DataFrame()

    # Coverage aus Schedule (RÜSTEN / ad-hoc gekennzeichnete Namen säubern)
    prod = schedule_df[schedule_df["Artikel"]!="RÜSTEN"].copy()
    prod["Artikel_join"] = prod["Artikel"].str.replace(r"\s+\[.*\]","", regex=True)
    prod_minutes = prod.groupby("Artikel_join")["Dauer_min"].sum()
    dem2 = dem.copy()
    dem2["Geplante_Minuten"] = dem2["Artikel"].map(prod_minutes).fillna(0.0)
    dem2["Geplante_Stk"] = dem2["Geplante_Minuten"] / dem2["Min_pro_Stk"]
    dem2["Deckungsgrad_%"] = (dem2["Geplante_Minuten"] / dem2["Bedarfsminuten"]).clip(upper=1.0) * 100.0
    dem2["Restminuten"] = (dem2["Bedarfsminuten"] - dem2["Geplante_Minuten"]).clip(lower=0.0)
    dem2["Restmenge_Stk"] = dem2["Restminuten"] / dem2["Min_pro_Stk"]

    art_df = dem2[["Artikel","Bedarf","Min_pro_Stk","Bedarfsminuten","Geplante_Minuten","Geplante_Stk","Deckungsgrad_%","Restminuten","Restmenge_Stk"]]

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

    # Ergebnis-Plan (aus Schedule aggregiert)
    plan_df_final = (prod.groupby(["StandID","Presse","Artikel_join"])
                        ["Dauer_min"].sum().reset_index()
                        .rename(columns={"Artikel_join":"Artikel"}))
    plan_df_final["Pressenname"] = plan_df_final["Presse"].map(name_map)
    plan_df_final["StandName"] = plan_df_final["StandID"].map(stand_name_map)
    plan_df_final = plan_df_final[["StandID","StandName","Presse","Pressenname","Artikel","Dauer_min"]]

    return cells_df, plan_df_final, art_df, schedule_df, kpis, pulled_df

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
        relax_factor_sumr=float(relax_factor_sumr),
        relax_spread_add=float(relax_spread_add),
        gantt_year=int(gantt_year),
        shift_start_hour=int(shift_start_hour),
        show_shift_grid=bool(show_shift_grid),
    )
    cells_df, plan_df, art_df, schedule_df, kpis, pulled_df = plan_all(sheets, params, bedarfe_override_kw=edited_bedarfe_kw)

# -------- Anzeigen --------
c1,c2,c3,c4 = st.columns(4)
c1.metric("KW", kpis["KW"])
c2.metric("Σ Bedarf (min)", f"{kpis['Σ Bedarfsmin']:.0f}")
c3.metric("Σ geplant (min)", f"{kpis['Σ geplant (min)']:.0f}")
c4.metric("Deckungsgrad gesamt", f"{kpis['Deckungsgrad gesamt']:.1f}%")

st.subheader("Gebildete Zellen")
st.dataframe(cells_df, use_container_width=True)

st.subheader("Plan je Presse / Artikel (Minuten) – aus Schichtplan")
st.dataframe(plan_df, use_container_width=True)

st.subheader("Deckung je Artikel")
st.dataframe(art_df.sort_values(["Deckungsgrad_%","Bedarfsminuten"], ascending=[True, False]), use_container_width=True)

if not schedule_df.empty:
    schedule_df = schedule_df.copy()
    schedule_df["YLabel"] = schedule_df.apply(lambda r: f"S{r['StandID']} | {r['StandName']} | {r['Pressenname']}", axis=1)
    yorder = schedule_df.drop_duplicates(subset=["Presse","YLabel"]).sort_values(["StandID","Pressenname"])["YLabel"].tolist()
    # Farb-Logik: RÜSTEN grau, jede CellID sonst eigene Farbe
    schedule_df["ColorKey"] = schedule_df["CellID"].where(schedule_df["Artikel"]!="RÜSTEN", other="SETUP")

    st.subheader("Schicht-Ablauf je Presse (synchron)")
    st.dataframe(schedule_df, use_container_width=True)

    color_map = {"SETUP": "rgb(130,130,130)"}  # RÜSTEN immer grau
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

    if params.show_shift_grid:
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

if pulled_df is not None and not pulled_df.empty:
    st.subheader("Vorgezogen (aus zukünftigen KWs)")
    grp = pulled_df.groupby(["Artikel","QuelleKW"]).agg(Minuten=("Minuten","sum")).reset_index()
    st.dataframe(grp, use_container_width=True)

# Downloads
st.subheader("Downloads")
def to_xlsx_bytes() -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        (cells_df if not cells_df.empty else pd.DataFrame({"Hinweis":["Keine Zellen gebildet"]})).to_excel(writer, sheet_name="Zellen", index=False)
        plan_df.to_excel(writer, sheet_name="Plan_je_Presse", index=False)
        art_df.to_excel(writer, sheet_name="Coverage_je_Artikel", index=False)
        (schedule_df if not schedule_df.empty else pd.DataFrame({"Hinweis":["Kein Schedule"]})).to_excel(writer, sheet_name="Schedule", index=False)
        if pulled_df is not None and not pulled_df.empty:
            pulled_df.to_excel(writer, sheet_name="Vorzuege", index=False)
    return output.getvalue()

st.download_button("📥 Excel-Export", data=to_xlsx_bytes(),
                   file_name=f"Plan_KW{selected_kw}.xlsx",
                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
st.download_button("📥 Plan (CSV)", data=plan_df.to_csv(index=False).encode("utf-8"),
                   file_name=f"Plan_KW{selected_kw}.csv", mime="text/csv")
