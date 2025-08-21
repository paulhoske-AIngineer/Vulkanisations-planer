# -*- coding: utf-8 -*-
"""
Vulkanisations-Planer – Streamlit App (Zellen synchron, Rüstzeiten, Werkzeuge, Vorziehen, Gantt)

Features:
- Excel laden (Sheets: Pressen, Staende, Freigaben_Werkzeug_Presse, Zykluszeiten,
  Bedarfe_Woche, Kapazitaet_Woche; optional: Werkzeuge mit 'Artikel', 'Anzahl_Werkzeuge')
- Bedarfe für die gewählte KW im UI editieren
- Zellenbildung (3er → 2er → Einzel) mit Σr-Limit und Schließzeit-Spread
- Werkzeuglimit: max. gleichzeitige Pressen pro Artikel
- Rüstzeiten zwischen Artikelwechseln (Standard 240 min)
- Zellen-Scheduling: Pressen einer Zelle starten synchron und laufen möglichst lange ohne Wechsel
- Vorziehen: Bedarfe aus KW+1/+2, um gute Zellen zu verlängern (im Gantt als „[vorgezogen]“ kenntlich + Report)
- Gantt: nach Stand sortiert, Farbe pro Zelle, Rüstungen grau, Schichtgitter (Mo 06:00, 3×8h)

Start (im Ordner der app.py):
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
        z["Gesamtzyklus"] = z[["Bedienzeit", "Nebenzeit", "Schließzeit"]].sum(axis=1)
    z["Servicezeit"] = z["Bedienzeit"] + z["Nebenzeit"]
    z["r"] = z["Servicezeit"] / z["Schließzeit"]
    return z


def agg_zyklen_unique(zyklen: pd.DataFrame) -> pd.DataFrame:
    """Eindeutige Zeiten je Artikel (max), vermeidet InvalidIndexError bei map()."""
    cols = [c for c in ["Artikel", "Gesamtzyklus", "Schließzeit", "r"] if c in zyklen.columns]
    z = zyklen[cols].copy()
    z = z.groupby("Artikel", as_index=False).agg({"Gesamtzyklus": "max", "Schließzeit": "max", "r": "max"})
    return z


def demand_minutes(bedarfe_kw: pd.DataFrame, zyklen_unique: pd.DataFrame, cavity_per_art: Dict[str, float]) -> pd.DataFrame:
    dem = bedarfe_kw.merge(zyklen_unique, on="Artikel", how="left")
    dem = dem[dem["Artikel"].isin(cavity_per_art.keys())].copy()
    dem["Cavity"] = dem["Artikel"].map(cavity_per_art)
    dem["Min_pro_Stk"] = (dem["Gesamtzyklus"] / dem["Cavity"]) / 60.0
    dem["Bedarfsminuten"] = dem["Bedarf"] * dem["Min_pro_Stk"]
    return dem


def spread_from_s(s_list: List[float]) -> float:
    smin, smax = min(s_list), max(s_list)
    return (smax - smin) / smax if smax else 0.0


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
        arts_left = [a for a in articles if remain_min.get(a, 0.0) > 1e-6]
        if len(arts_left) < k:
            break

        candidates: List[Tuple[Tuple[str, ...], float, float]] = []
        for comb in itertools.combinations(arts_left, k):
            svals = [s_map.get(a) for a in comb]
            if any(pd.isna(s) for s in svals):
                continue
            sum_r = sum(r_map.get(a, 0) for a in comb)
            sp = spread_from_s(svals)
            if sum_r <= sum_r_limit and sp <= max_spread:
                candidates.append((comb, sum_r, sp))
        if not candidates:
            break
        candidates.sort(key=lambda x: (-x[1], x[2], -sum(remain_min.get(a, 0) for a in x[0])))

        for stand_id, presses in presses_by_stand.items():
            if respect_stand_limit and int(stand_limit_map.get(stand_id, k) or k) < k:
                continue
            if len(presses) < k:
                continue
            if not allow_multiple_cells_per_stand and stand_id in used_stands_in_round:
                continue

            chosen = None
            for comb, sum_r, sp in candidates:
                used = set(); mapping = {}; feas = True
                for a in comb:
                    poss = [p for p in (allowed_by_stand[stand_id].get(a, set()) - used)
                            if p in presses and capacity_left.get(p, 0) > 1e-6]
                    if len(presses_for_article[a]) >= tools_limit[a]:
                        poss = [p for p in poss if p in presses_for_article[a]]
                    if not poss:
                        feas = False; break
                    poss.sort(key=lambda p: -capacity_left.get(p, 0))
                    pick = poss[0]; mapping[a] = pick; used.add(pick)
                if feas and len(mapping) == k:
                    chosen = (comb, sum_r, sp, mapping); break
            if not chosen:
                continue

            comb, sum_r, sp, mapping = chosen
            cell_id = f"{k}er-{stand_id}-{cell_counter}"; cell_counter += 1
            cell_rows.append({
                "CellID": cell_id, "StandID": stand_id, "Typ": f"{k}er",
                "Kombi_Artikel": " + ".join(comb), "Summe_r": sum_r, "Spread": sp,
                **{f"Art{i+1}": comb[i] for i in range(k)},
                **{f"Presse{i+1}": mapping[comb[i]] for i in range(k)},
            })
            for a in comb:
                add_plan(stand_id, mapping[a], a, remain_min.get(a, 0.0), cell_id)
            used_stands_in_round.add(stand_id); progress = True

    return pd.DataFrame(cell_rows), plan_rows_local


# ---- Neuer Scheduler: Zellen synchron & lang laufen lassen ----
def build_schedule_with_synchronized_cells(
    plan_df: pd.DataFrame,
    cells_df: pd.DataFrame,
    kap_active: pd.DataFrame,
    params: PlanParams,
    start_dt: datetime,
    capacity_left: Dict[str, float],
    future_pool_by_kw: Optional[Dict[int, Dict[str, float]]] = None,
):
    """Zellen (2er/3er) je Stand synchron anlaufen lassen und möglichst lange ohne Wechsel fahren.
       Vorziehen aus KW+1/+2 zur Verlängerung guter Zellen. Rüstungen passend einfügen.
    """
    cap_total = kap_active.set_index("Presse_ID")["Avail_min"].to_dict()
    press_time: Dict[str, datetime] = {}       # aktueller Endzeitpunkt je Presse
    press_sched_min: Dict[str, float] = {}     # geplante Minuten je Presse
    prev_art: Dict[str, Optional[str]] = {}    # letzter Artikel je Presse
    sch_rows = []
    used_in_cell: Dict[Tuple[str, str, str], float] = {}  # (Presse, Artikel, CellID) -> Minuten
    pulled_log_local: List[Tuple[str, int, int, float, str, str, str]] = []  # (Artikel, QuelleKW, ZielKW, Min, StandID, Presse, CellID)
    pulled_pairs: Set[Tuple[str, str, str]] = set()  # (Presse, Artikel, CellID) die Pull hatten

    def get_t(press: str) -> datetime:
        return press_time.get(press, start_dt)

    def add_minutes(press: str, mins: float):
        press_sched_min[press] = press_sched_min.get(press, 0.0) + mins

    def ensure_setup_before(press: str, pressname: str, stand_id: str, standname: str, t_target: datetime, new_art: str):
        t_cur = get_t(press)
        needs_setup = (prev_art.get(press) is not None and prev_art[press] != new_art and params.setup_minutes > 0)
        if not needs_setup:
            return t_target
        setup_dur = timedelta(minutes=params.setup_minutes)
        setup_start = max(t_cur, t_target - setup_dur)
        left_cap = cap_total.get(press, 0.0) - press_sched_min.get(press, 0.0)
        if left_cap <= 1e-6:
            return t_target
        real_setup_min = min(params.setup_minutes, left_cap)
        setup_end = setup_start + timedelta(minutes=real_setup_min)
        sch_rows.append({
            "StandID": stand_id, "StandName": standname,
            "Presse": press, "Pressenname": pressname,
            "Artikel": "RÜSTEN", "CellID": "SETUP",
            "Start": setup_start, "Ende": setup_end, "Dauer_min": float(real_setup_min)
        })
        press_time[press] = setup_end
        add_minutes(press, float(real_setup_min))
        return max(t_target, setup_end)

    # --- Zellenliste aufbauen (Paare, aktuelle Minuten, Pull-Potenzial, Score) ---
    cell_jobs = []
    if cells_df is not None and not cells_df.empty:
        for _, row in cells_df.iterrows():
            cell_id = str(row["CellID"])
            stand_id = str(row["StandID"])
            art_cols = sorted([c for c in row.index if str(c).startswith("Art")], key=lambda x: int(''.join(filter(str.isdigit, x)) or 0))
            prs_cols = sorted([c for c in row.index if str(c).startswith("Presse")], key=lambda x: int(''.join(filter(str.isdigit, x)) or 0))
            pairs = []
            for ac, pc in zip(art_cols, prs_cols):
                a = row.get(ac); p = row.get(pc)
                if pd.notna(a) and pd.notna(p):
                    pairs.append((str(a), str(p)))
            if not pairs:
                continue
            sub = plan_df[(plan_df["CellID"] == cell_id)]
            cur_min = {}
            for a, p in pairs:
                cur = float(sub[(sub["Presse"].astype(str) == p) & (sub["Artikel"] == a)]["Dauer_min"].sum())
                if cur > 1e-6:
                    cur_min[(a, p)] = cur
            if not cur_min:
                continue
            base_common = min(cur_min.values())
            pot = {}
            for (a, p) in cur_min.keys():
                pot_cap = max(0.0, capacity_left.get(p, 0.0))
                pot_future = 0.0
                if future_pool_by_kw:
                    for kwv in sorted(future_pool_by_kw.keys()):
                        pot_future += future_pool_by_kw[kwv].get(a, 0.0)
                pot[(a, p)] = max(0.0, min(pot_cap, pot_future))
            target = min(cur_min[(a, p)] + pot[(a, p)] for (a, p) in cur_min.keys())
            score = target
            cell_jobs.append((stand_id, cell_id, pairs, cur_min, pot, score))
    cell_jobs.sort(key=lambda x: (x[0], -x[5]))  # nach Stand, dann lange Zellen zuerst

    # --- Zellen blockweise synchron planen ---
    for stand_id, cell_id, pairs, cur_min, pot, _score in cell_jobs:
        # Metadaten
        meta = {}
        for a, p in pairs:
            row_any = plan_df[(plan_df["Presse"].astype(str) == p)].head(1)
            pressname = row_any["Pressenname"].iloc[0] if not row_any.empty else p
            standname = row_any["StandName"].iloc[0] if not row_any.empty else ""
            meta[(a, p)] = (pressname, standname)

        # Gemeinsamer Start: max(Verfügbarkeit + ggf. Rüstzeit)
        t_candidates = []
        for a, p in pairs:
            t_cur = get_t(p)
            needs_setup = (prev_art.get(p) is not None and prev_art[p] != a and params.setup_minutes > 0)
            t_candidates.append(t_cur + (timedelta(minutes=params.setup_minutes) if needs_setup else timedelta(0)))
        t_cell = max(t_candidates) if t_candidates else start_dt
        # Rüstungen so einfügen, dass Start wirklich synchron wird
        for a, p in pairs:
            pressname, standname = meta[(a, p)]
            t_cell = ensure_setup_before(p, pressname, stand_id, standname, t_cell, a)

        # Kapazität je Presse begrenzt Blocklänge
        cap_limits = []
        for a, p in pairs:
            cap_left = max(0.0, cap_total.get(p, 0.0) - press_sched_min.get(p, 0.0))
            cap_limits.append(cap_left)
        cap_limit_all = min(cap_limits) if cap_limits else float("inf")

        # Ziel-Laufzeit je Paar: cur + (feasible pull), dann Minimum über alle → gemeinsame Länge
        feasible_extra = {}
        for (a, p) in cur_min.keys():
            extra_cap = max(0.0, min(pot[(a, p)], capacity_left.get(p, 0.0)))
            feasible_extra[(a, p)] = extra_cap
        target_len = min(cur_min[(a, p)] + feasible_extra[(a, p)] for (a, p) in cur_min.keys())
        target_len = max(0.0, min(target_len, cap_limit_all))
        if target_len <= 1e-6:
            continue

        # Pull aus Zukunft, um Paare auf target_len zu heben
        if future_pool_by_kw:
            for (a, p) in list(cur_min.keys()):
                need = max(0.0, target_len - cur_min[(a, p)])
                if need <= 1e-6:
                    continue
                need = min(need, capacity_left.get(p, 0.0))
                if need <= 1e-6:
                    continue
                for kwv in sorted(future_pool_by_kw.keys()):  # KW+1 → KW+2 …
                    avail = future_pool_by_kw[kwv].get(a, 0.0)
                    if avail <= 1e-6:
                        continue
                    take = min(need, avail)
                    if take <= 1e-6:
                        continue
                    future_pool_by_kw[kwv][a] = avail - take
                    capacity_left[p] = capacity_left.get(p, 0.0) - take
                    cur_min[(a, p)] += take
                    need -= take
                    pulled_log_local.append((a, kwv, params.kw, take, stand_id, p, cell_id))
                    pulled_pairs.add((p, a, cell_id))
                    if need <= 1e-6:
                        break

        # finale Länge nach Pull, kapazitätsbegrenzt
        target_len = min([cur_min[(a, p)] for (a, p) in pairs] + [cap_limit_all])

        # Synchroner Block je Presse/Artikel erzeugen
        for a, p in pairs:
            pressname, standname = meta[(a, p)]
            start = t_cell; end = t_cell + timedelta(minutes=target_len)
            label = a + (" [vorgezogen]" if (p, a, cell_id) in pulled_pairs else "")
            sch_rows.append({
                "StandID": stand_id, "StandName": standname,
                "Presse": p, "Pressenname": pressname,
                "Artikel": label, "CellID": cell_id,
                "Start": start, "Ende": end, "Dauer_min": float(target_len)
            })
            press_time[p] = end
            add_minutes(p, float(target_len))
            prev_art[p] = a
            used_in_cell[(p, a, cell_id)] = used_in_cell.get((p, a, cell_id), 0.0) + float(target_len)

    # --- Restliche Minuten (Cell-Reste & Singles) anhängen ---
    agg = (plan_df.groupby(["Presse", "Artikel", "CellID", "StandID", "StandName", "Pressenname"], dropna=False)["Dauer_min"]
           .sum().reset_index())

    for _, r in agg.iterrows():
        p = str(r["Presse"]); a = str(r["Artikel"]); cell_id = str(r["CellID"])
        stand_id = str(r["StandID"]); standname = r["StandName"]; pressname = r["Pressenname"]
        total_min = float(r["Dauer_min"])
        used = float(used_in_cell.get((p, a, cell_id), 0.0))
        rest = max(0.0, total_min - used)
        if rest <= 1e-6:
            continue

        # Rüstzeit bei Artikelwechsel
        t_cur = get_t(p)
        needs_setup = (prev_art.get(p) is not None and prev_art[p] != a and params.setup_minutes > 0)
        if needs_setup:
            left_cap = cap_total.get(p, 0.0) - press_sched_min.get(p, 0.0)
            if left_cap > 1e-6:
                setup_min = min(params.setup_minutes, left_cap)
                sch_rows.append({
                    "StandID": stand_id, "StandName": standname,
                    "Presse": p, "Pressenname": pressname,
                    "Artikel": "RÜSTEN", "CellID": "SETUP",
                    "Start": t_cur, "Ende": t_cur + timedelta(minutes=setup_min),
                    "Dauer_min": float(setup_min)
                })
                t_cur = t_cur + timedelta(minutes=setup_min)
                press_time[p] = t_cur
                add_minutes(p, float(setup_min))

        # verfügbare Restkapazität
        avail = max(0.0, cap_total.get(p, 0.0) - press_sched_min.get(p, 0.0))
        if avail <= 1e-6:
            continue
        run_min = min(rest, avail)
        sch_rows.append({
            "StandID": stand_id, "StandName": standname,
            "Presse": p, "Pressenname": pressname,
            "Artikel": a, "CellID": cell_id if cell_id and cell_id != "nan" else "Single",
            "Start": get_t(p), "Ende": get_t(p) + timedelta(minutes=run_min),
            "Dauer_min": float(run_min)
        })
        press_time[p] = get_t(p) + timedelta(minutes=run_min)
        add_minutes(p, float(run_min))
        prev_art[p] = a

    schedule_df = pd.DataFrame(sch_rows).sort_values(["StandID", "Presse", "Start"])
    pulled_df_local = pd.DataFrame(pulled_log_local,
                                   columns=["Artikel", "QuelleKW", "ZielKW", "Minuten", "StandID", "Presse", "CellID"])
    return schedule_df, pulled_df_local


# ---- Planung inkl. Vorziehen & neuem Scheduler ----
def plan_all(sheets: Dict[str, pd.DataFrame], params: PlanParams, bedarfe_override_kw: Optional[pd.DataFrame] = None):
    # Read
    pressen = sheets["Pressen"].copy()
    staende = sheets["Staende"].copy()
    freigaben = sheets["Freigaben_Werkzeug_Presse"].copy()
    zyklen = sheets["Zykluszeiten"].copy()
    bedarfe = sheets["Bedarfe_Woche"].copy()
    kap = sheets["Kapazitaet_Woche"].copy()

    # optionales Werkzeuge-Sheet
    tools_map: Dict[str, int] = defaultdict(lambda: 9999)
    w_sheet = None
    for cand in ["Werkzeuge", "Anzahl_Werkzeuge"]:
        if cand in sheets: w_sheet = sheets[cand].copy(); break
    if w_sheet is not None and params.limit_by_tools:
        w_sheet.columns = [c.strip() for c in w_sheet.columns]
        art_col = "Artikel"
        cnt_col = "Anzahl_Werkzeuge" if "Anzahl_Werkzeuge" in w_sheet.columns else [c for c in w_sheet.columns if "anzahl" in c.lower()][0]
        tools_map.update({str(r[art_col]): int(r[cnt_col]) for _, r in w_sheet[[art_col, cnt_col]].dropna().iterrows()})

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
        bedarfe = bedarfe[~bedarfe["Artikel"].isin(params.exclude)]

    bedarfe["KW_num"] = pd.to_numeric(bedarfe["KW"], errors="coerce")
    if params.kw not in set(bedarfe["KW_num"].dropna().astype(int)):
        raise ValueError(f"KW {params.kw} ist in Bedarfe_Woche nicht vorhanden.")

    # Bedarfe-Override (Editor)
    if bedarfe_override_kw is not None:
        bedarfe = bedarfe.copy()
        mask = bedarfe["KW_num"] == params.kw
        bed_map = dict(zip(bedarfe_override_kw["Artikel"], bedarfe_override_kw["Bedarf"]))
        bedarfe.loc[mask, "Bedarf"] = bedarfe.loc[mask, "Artikel"].map(bed_map).fillna(bedarfe.loc[mask, "Bedarf"])

    # Kapazität
    pressen_active = pressen[pressen["Aktiv_bool"]][["PressID", "Pressenname", "StandID"]]
    kap_active = kap.merge(pressen_active[["PressID"]], left_on="Presse_ID", right_on="PressID", how="inner")
    kap_active["Avail_min"] = kap_active["Verfuegbare_Minuten"].fillna(0) - kap_active["Wartung_Minuten"].fillna(0)
    capacity_left: Dict[str, float] = dict(zip(kap_active["Presse_ID"], kap_active["Avail_min"]))

    # Relevante Kombinationen
    rel = (
        bedarfe[bedarfe["KW_num"] == params.kw][["Artikel", "Bedarf"]]
        .merge(freigaben[["Artikel", "Pressenname", "Cavity"]], on="Artikel", how="inner")
        .merge(pressen_active, on="Pressenname", how="inner")
        .merge(zyklen_u[["Artikel", "Schließzeit", "r", "Gesamtzyklus"]], on="Artikel", how="inner")
    )
    if rel.empty:
        raise RuntimeError("Keine relevanten Datensätze (Bedarf + zulässige Pressen + Zeiten).")

    # Demand-Minuten (aktuelle KW)
    cavity_per_art = rel.groupby("Artikel")["Cavity"].max().to_dict()
    dem = demand_minutes(bedarfe[bedarfe["KW_num"] == params.kw][["Artikel", "Bedarf"]], zyklen_u, cavity_per_art)

    r_map = dem.set_index("Artikel")["r"].to_dict()
    s_map = dem.set_index("Artikel")["Schließzeit"].to_dict()
    remain_min: Dict[str, float] = dem.set_index("Artikel")["Bedarfsminuten"].to_dict()

    # Pull-ahead Pools (KW+1..+N)
    future_pool_by_kw: Dict[int, Dict[str, float]] = {}
    if params.allow_pull_ahead and params.pull_weeks > 0:
        future_kws = [params.kw + i for i in range(1, params.pull_weeks + 1)]
        bed_future = bedarfe[bedarfe["KW_num"].isin(future_kws)][["Artikel", "KW_num", "Bedarf"]].copy()
        if not bed_future.empty:
            cav_map = (
                freigaben.merge(pressen_active, on="Pressenname", how="inner")
                .groupby("Artikel")["Cavity"].max()
                .to_dict()
            )
            gz_map = zyklen_u.set_index("Artikel")["Gesamtzyklus"].to_dict()
            bed_future["Cavity"] = bed_future["Artikel"].map(cav_map)
            bed_future["GZ"] = bed_future["Artikel"].map(gz_map)
            bed_future = bed_future.dropna(subset=["Cavity", "GZ"]).copy()
            bed_future["Min_pro_Stk"] = (bed_future["GZ"] / bed_future["Cavity"]) / 60.0
            bed_future["Bedarfsminuten"] = bed_future["Bedarf"] * bed_future["Min_pro_Stk"]
            for kwv, g in bed_future.groupby("KW_num"):
                future_pool_by_kw[int(kwv)] = dict(zip(g["Artikel"], g["Bedarfsminuten"]))

    def pull_from_future(artikel: str, needed_min: float) -> List[Tuple[int, float]]:
        pulled = []
        if needed_min <= 0:
            return pulled
        for kwv in sorted(future_pool_by_kw.keys()):
            avail = future_pool_by_kw[kwv].get(artikel, 0.0)
            if avail <= 1e-6:
                continue
            take = min(needed_min, avail)
            if take > 0:
                future_pool_by_kw[kwv][artikel] = avail - take
                pulled.append((kwv, take))
                needed_min -= take
                if needed_min <= 1e-6:
                    break
        return pulled

    # allowed/presses/limits
    allowed_by_stand: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))
    for _, row in rel.iterrows():
        allowed_by_stand[row["StandID"]][row["Artikel"]].add(str(row["PressID"]))
    presses_by_stand = pressen_active.groupby("StandID")["PressID"].apply(lambda s: list(map(str, s))).to_dict()
    stand_limit_map = staende.set_index("StandID")["MaxMachinesPerWerker"].to_dict()

    articles = sorted(remain_min.keys())
    tools_limit = {a: int(defaultdict(lambda: 9999, tools_map).get(a, 9999)) for a in articles}
    presses_for_article: Dict[str, Set[str]] = {a: set() for a in articles}

    # Zellen
    cell_frames = []
    plan_rows_all: List[Dict] = []

    def run_cells(k: int):
        nonlocal cell_frames, plan_rows_all
        c, p = build_cells(
            k, articles, remain_min, r_map, s_map, allowed_by_stand, presses_by_stand, capacity_left,
            params.sum_r_limit, params.max_spread, params.respect_stand_limit, stand_limit_map,
            params.allow_multiple_cells_per_stand, tools_limit, presses_for_article
        )
        if not c.empty:
            cell_frames.append(c)
        plan_rows_all.extend(p)

    if "3er" in params.cell_strategy:
        run_cells(3)
    if ("2er" in params.cell_strategy) or (params.cell_strategy == "nur 2er → Einzel"):
        run_cells(2)

    # Restbedarf → Einzel
    def add_plan(stand_id: str, press: str, artikel: str, minutes: float, cell_id: str):
        if press not in presses_for_article[artikel] and len(presses_for_article[artikel]) >= tools_limit[artikel]:
            return 0.0
        take = min(minutes, capacity_left.get(press, 0.0), remain_min.get(artikel, 0.0))
        if take <= 0:
            return 0.0
        plan_rows_all.append({"StandID": stand_id, "Presse": press, "Artikel": artikel,
                              "Dauer_min": float(take), "CellID": cell_id, "QuelleKW": None})
        capacity_left[press] = capacity_left.get(press, 0.0) - take
        remain_min[artikel] = remain_min.get(artikel, 0.0) - take
        presses_for_article[artikel].add(press)
        return take

    for a in articles:
        need = remain_min.get(a, 0.0)
        if need <= 1e-6:
            continue
        poss = rel[rel["Artikel"] == a][["StandID", "PressID"]].drop_duplicates().copy()
        poss["Cap_left"] = poss["PressID"].astype(str).map(capacity_left).fillna(0.0)
        poss = poss.sort_values("Cap_left", ascending=False)
        for _, r in poss.iterrows():
            if need <= 1e-6:
                break
            take = add_plan(str(r["StandID"]), str(r["PressID"]), a, need, cell_id=f"Single-{a}")
            need -= take

    cells_df = pd.concat(cell_frames, ignore_index=True) if cell_frames else pd.DataFrame(columns=["Hinweis"], data=[["Keine Zellen gebildet"]])
    plan_df = pd.DataFrame(plan_rows_all, columns=["StandID", "Presse", "Artikel", "Dauer_min", "CellID", "QuelleKW"])

    # --- Vorziehen zur Verlängerung guter Zellen: wird im Scheduler final berücksichtigt ---
    # (Hier kein direkter Eingriff; Pull passiert in build_schedule_with_synchronized_cells())

    # Namen ergänzen
    name_map = pressen_active.set_index("PressID")["Pressenname"].to_dict()
    stand_name_map = staende.set_index("StandID")["StandName"].to_dict()
    plan_df["Pressenname"] = plan_df["Presse"].astype(str).map(name_map)
    plan_df["StandName"] = plan_df["StandID"].map(stand_name_map)
    plan_df = plan_df[["StandID", "StandName", "Presse", "Pressenname", "Artikel", "Dauer_min", "CellID", "QuelleKW"]] \
        .sort_values(["StandID", "Presse", "Artikel"]).reset_index(drop=True)

    # --- Neuer Schedule: Zellen synchron ---
    start_dt = datetime.combine(date.fromisocalendar(params.gantt_year, params.kw, 1), time(hour=params.shift_start_hour))
    schedule_df, pulled_df_extra = build_schedule_with_synchronized_cells(
        plan_df=plan_df,
        cells_df=cells_df if cells_df is not None else pd.DataFrame(),
        kap_active=kap_active,
        params=params,
        start_dt=start_dt,
        capacity_left=capacity_left,
        future_pool_by_kw=future_pool_by_kw
    )
    pulled_df = pulled_df_extra

    plan_df_final = plan_df.copy()  # Minutenbasis je Presse/Artikel (Schedule enthält echte Sequenz mit Rüst)

    # Coverage nach Schedule (RÜSTEN ausgeschlossen)
    prod = schedule_df[schedule_df["Artikel"] != "RÜSTEN"].copy()
    prod["Artikel_join"] = prod["Artikel"].str.replace(r"\s+\[vorgezogen\]$", "", regex=True)
    prod_minutes = prod.groupby("Artikel_join")["Dauer_min"].sum()
    dem2 = dem.copy()
    dem2["Geplante_Minuten"] = dem2["Artikel"].map(prod_minutes).fillna(0.0)
    dem2["Geplante_Stk"] = dem2["Geplante_Minuten"] / dem2["Min_pro_Stk"]
    dem2["Deckungsgrad_%"] = (dem2["Geplante_Minuten"] / dem2["Bedarfsminuten"]).clip(upper=1.0) * 100.0
    dem2["Restminuten"] = (dem2["Bedarfsminuten"] - dem2["Geplante_Minuten"]).clip(lower=0.0)
    dem2["Restmenge_Stk"] = dem2["Restminuten"] / dem2["Min_pro_Stk"]
    art_df = dem2[["Artikel", "Bedarf", "Min_pro_Stk", "Bedarfsminuten",
                   "Geplante_Minuten", "Geplante_Stk", "Deckungsgrad_%", "Restminuten", "Restmenge_Stk"]]

    # KPIs
    total_demand_min = float(art_df["Bedarfsminuten"].sum())
    total_planned_min = float(art_df["Geplante_Minuten"].sum())
    total_cap_min = float(kap_active["Avail_min"].sum())
    kpis = {
        "KW": params.kw,
        "Σ Bedarfsmin": total_demand_min,
        "Σ geplant (min)": total_planned_min,
        "Σ Kapazität (min)": total_cap_min,
        "Deckungsgrad gesamt": (total_planned_min / total_demand_min * 100.0) if total_demand_min > 0 else 0.0,
    }

    # Vorzugs-Report mit Namen
    if pulled_df is not None and not pulled_df.empty:
        pulled_df["Pressenname"] = pulled_df["Presse"].astype(str).map(name_map)
        pulled_df["StandName"] = pulled_df["StandID"].map(stand_name_map)

    return cells_df, plan_df_final, art_df, schedule_df, kpis, pulled_df


# -------- Ablauf / UI --------
if up is None:
    st.info("Bitte Excel laden – danach Bedarfs-Editor, Parameter & Ergebnisse.")
    st.stop()

sheets = read_workbook(up)
need = {"Pressen", "Staende", "Freigaben_Werkzeug_Presse", "Zykluszeiten", "Bedarfe_Woche", "Kapazitaet_Woche"}
missing = sorted(need - set(sheets))
if missing:
    st.error(f"Fehlende Sheets: {', '.join(missing)}")
    st.stop()

bedarfe_df = sheets["Bedarfe_Woche"].copy()
bedarfe_df["KW_num"] = pd.to_numeric(bedarfe_df["KW"], errors="coerce")
kw_options = sorted(set(bedarfe_df["KW_num"].dropna().astype(int)))
selected_kw = st.sidebar.selectbox("Kalenderwoche (KW)", kw_options, index=max(0, len(kw_options) - 1))

kw_articles = sorted(set(bedarfe_df[bedarfe_df["KW_num"] == selected_kw]["Artikel"]))
selected_exclude = st.sidebar.multiselect("Artikel ausschließen", options=kw_articles,
                                          default=[a for a in kw_articles if a in exclude_default])

st.subheader(f"Bedarfe editieren – KW {selected_kw}")
bedarfe_kw = bedarfe_df[bedarfe_df["KW_num"] == selected_kw][["Artikel", "Bedarf"]].copy().reset_index(drop=True)
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
    cells_df, plan_df, art_df, schedule_df, kpis, pulled_df = plan_all(sheets, params, bedarfe_override_kw=edited_bedarfe_kw)

# -------- Anzeigen --------
c1, c2, c3, c4 = st.columns(4)
c1.metric("KW", kpis["KW"])
c2.metric("Σ Bedarf (min)", f"{kpis['Σ Bedarfsmin']:.0f}")
c3.metric("Σ geplant (min)", f"{kpis['Σ geplant (min)']:.0f}")
c4.metric("Deckungsgrad gesamt", f"{kpis['Deckungsgrad gesamt']:.1f}%")

st.subheader("Gebildete Zellen")
st.dataframe(cells_df, use_container_width=True)

st.subheader("Plan je Presse / Artikel (Minuten)")
st.dataframe(plan_df, use_container_width=True)

st.subheader("Deckung je Artikel")
st.dataframe(art_df.sort_values(["Deckungsgrad_%", "Bedarfsminuten"], ascending=[True, False]), use_container_width=True)

if not schedule_df.empty:
    # Y-Achse sortieren: StandID ↑, innerhalb Stand nach Pressenname ↑
    schedule_df = schedule_df.copy()
    schedule_df["YLabel"] = schedule_df.apply(lambda r: f"S{r['StandID']} | {r['StandName']} | {r['Pressenname']}", axis=1)
    yorder = schedule_df.drop_duplicates(subset=["Presse", "YLabel"]).sort_values(["StandID", "Pressenname"])["YLabel"].tolist()

    # Farb-Logik: RÜSTEN immer grau; sonst Farbe pro Zelle (CellID)
    schedule_df["ColorKey"] = schedule_df["CellID"].where(schedule_df["Artikel"] != "RÜSTEN", other="SETUP")

    st.subheader("Ablaufplan je Presse (Start/Ende) inkl. Rüstzeiten")
    st.dataframe(schedule_df, use_container_width=True)

    # Gantt
    color_map = {"SETUP": "rgb(130,130,130)"}  # feste Farbe für Rüstwechsel
    fig = px.timeline(
        schedule_df,
        x_start="Start", x_end="Ende",
        y="YLabel",
        color="ColorKey",
        color_discrete_map=color_map,
        hover_data=["StandName", "Presse", "Artikel", "Dauer_min", "CellID"],
        title=f"Gantt – KW {selected_kw} (Start Mo {params.shift_start_hour:02d}:00)"
    )
    fig.update_yaxes(autorange="reversed", categoryorder="array", categoryarray=yorder)

    if params.shift_grid:
        # Schichtgitter: Mo ab shift_start_hour, 7 Tage × 3 Schichten
        anchor = datetime.combine(date.fromisocalendar(params.gantt_year, params.kw, 1), time(hour=params.shift_start_hour))
        shapes = []; labels = []; names = ["Früh", "Spät", "Nacht"]
        for d in range(7):
            for s in range(3):
                sh_start = anchor + timedelta(days=d, hours=8 * s)
                sh_end = sh_start + timedelta(hours=8)
                color = "rgba(120,120,120,0.06)" if s % 2 == 0 else "rgba(170,170,170,0.06)"
                shapes.append(dict(type="rect", xref="x", yref="paper",
                                   x0=sh_start, x1=sh_end, y0=0, y1=1,
                                   fillcolor=color, line=dict(width=0)))
                labels.append((sh_start + timedelta(hours=4), names[s]))
        fig.update_layout(shapes=shapes)
        for x, txt in labels:
            fig.add_annotation(x=x, y=1.02, xref="x", yref="paper", text=txt, showarrow=False, font=dict(size=10))

    st.plotly_chart(fig, use_container_width=True, theme="streamlit")

# Vorzüge anzeigen
if pulled_df is not None and not pulled_df.empty:
    st.subheader("Vorzeitig gedeckte Bedarfe (aus zukünftigen KWs vorgezogen)")
    st.dataframe(pulled_df.groupby(["Artikel", "QuelleKW"]).agg(Minuten=("Minuten", "sum")).reset_index(),
                 use_container_width=True)

# Downloads
st.subheader("Downloads")
def to_xlsx_bytes() -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        (cells_df if not cells_df.empty else pd.DataFrame({"Hinweis": ["Keine Zellen gebildet"]})).to_excel(writer, sheet_name="Zellen", index=False)
        plan_df.to_excel(writer, sheet_name="Plan_je_Presse", index=False)
        art_df.to_excel(writer, sheet_name="Coverage_je_Artikel", index=False)
        (schedule_df if not schedule_df.empty else pd.DataFrame({"Hinweis": ["Kein Schedule"]})).to_excel(writer, sheet_name="Schedule", index=False)
        if pulled_df is not None and not pulled_df.empty:
            pulled_df.to_excel(writer, sheet_name="Vorzuege", index=False)
    return output.getvalue()

st.download_button("📥 Excel-Export", data=to_xlsx_bytes(),
                   file_name=f"Plan_KW{selected_kw}.xlsx",
                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
st.download_button("📥 Plan (CSV)", data=plan_df.to_csv(index=False).encode("utf-8"),
                   file_name=f"Plan_KW{selected_kw}.csv", mime="text/csv")
