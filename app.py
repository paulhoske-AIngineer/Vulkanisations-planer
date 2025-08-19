# -*- coding: utf-8 -*-
"""
Vulkanisations-Planer – Streamlit App (mit Bedarfs-Editor, Werkzeuganzahl & Rüstzeiten)
---------------------------------------------------------------------------------------

NEU:
- Bedarfe für die ausgewählte KW in der App editierbar (Data Editor)
- Optionales Sheet "Werkzeuge" (oder "Anzahl_Werkzeuge"): Spalten  Artikel, Anzahl_Werkzeuge
  -> begrenzt die Anzahl gleichzeitiger Pressen je Artikel
- Rüstzeiten 4h (=240 min, parametrisierbar) zwischen verschiedenen Artikeln je Presse
  -> Rüstjobs werden im Ablauf/Schedule eingefügt; falls Kapazität nicht reicht, kürzt die App
     den letzten Auftrag auf der Presse
- Gantt: Pressen mit Stand sichtbar (Y-Achse: "S<StandID> | <StandName> | <Presse>"),
  Schichtgitter (Start Mo 06:00, 3x8h Schichten, einblendbar)

Start (im Ordner der app.py):
  python -m pip install --upgrade pip
  pip install streamlit pandas openpyxl xlsxwriter plotly
  streamlit run app.py
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
import plotly.graph_objects as go
import streamlit as st

# ----------------------------- UI -----------------------------
st.set_page_config(page_title="Vulkanisations-Planer", layout="wide")
st.title("🧰 Vulkanisations-Planer")
st.caption("Excel laden, Bedarfe & Parameter setzen → Planung mit Zellen, Rüstzeiten & Gantt")

with st.sidebar:
    st.header("1) Daten laden")
    up = st.file_uploader(
        "Excel-Vorlage (*.xlsx)",
        type=["xlsx"],
        help="Benötigte Sheets: Pressen, Staende, Freigaben_Werkzeug_Presse, Zykluszeiten, Bedarfe_Woche, Kapazitaet_Woche. Optional: Werkzeuge (Artikel, Anzahl_Werkzeuge).",
    )

    st.header("2) Parameter — Zellen & Regeln")
    sum_r_limit = st.slider("Σr-Limit pro Zelle", 0.80, 1.20, 1.00, 0.01)
    max_spread = st.slider("max. Schließzeit-Spread in Zelle", 0.0, 0.30, 0.15, 0.01, help="(Smax−Smin)/Smax")
    cell_strategy = st.selectbox("Zell-Strategie", ["3er → 2er → Einzel", "nur 2er → Einzel", "nur Einzel"])
    respect_stand_limit = st.checkbox("MaxMachinesPerWerker je Stand respektieren", True)
    allow_multiple_cells_per_stand = st.checkbox("Mehrere Zellen je Stand zulassen", True)
    enforce_freigefahren = st.checkbox("Nur freigefahrene Werkzeuge zulassen", True)

    st.header("3) Werkzeuge & Rüstzeiten")
    setup_minutes = st.number_input("Rüstzeit (Minuten) pro Werkzeugwechsel", min_value=0, max_value=24*60, value=240, step=10)
    limit_by_tools = st.checkbox("Anzahl gleichzeitig nutzbarer Werkzeuge je Artikel begrenzen (Sheet „Werkzeuge“)", True)

    st.header("4) Schicht & Gantt")
    gantt_year = st.number_input("Jahr für Gantt (ISO)", min_value=2020, max_value=2100, value=date.today().year, step=1)
    shift_grid = st.checkbox("Schichtgitter anzeigen (3×8h, Start Mo 06:00)", True)
    shift_start_hour = st.number_input("Schichtstart (Stunde, Mo)", min_value=0, max_value=23, value=6)
    exclude_default = {"UTK0035", "U0210160"}

# ----------------------------- Helpers -----------------------------
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
    z["Servicezeit"] = z["Bedienzeit"] + z["Nebenzeit"]
    z["Gesamtzyklus"] = z[["Bedienzeit","Nebenzeit","Schließzeit"]].sum(axis=1)
    z["r"] = z["Servicezeit"] / z["Schließzeit"]
    return z

def demand_minutes(bedarfe_kw: pd.DataFrame, zyklen: pd.DataFrame, cavity_per_art: Dict[str, float]) -> pd.DataFrame:
    dem = bedarfe_kw.merge(zyklen[["Artikel","Gesamtzyklus","Schließzeit","r"]], on="Artikel", how="left")
    dem = dem[dem["Artikel"].isin(cavity_per_art.keys())].copy()
    dem["Cavity"] = dem["Artikel"].map(cavity_per_art)
    dem["Min_pro_Stk"] = (dem["Gesamtzyklus"]/dem["Cavity"])/60.0
    dem["Bedarfsminuten"] = dem["Bedarf"] * dem["Min_pro_Stk"]
    return dem

def spread_from_s(s_list: List[float]) -> float:
    smin, smax = min(s_list), max(s_list)
    return (smax - smin)/smax if smax else 0.0

# ---- Zellen-Builder (wie zuvor), erweitert um Werkzeuglimit ----
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

    def add_plan(stand_id: str, press: str, artikel: str, minutes: float):
        # Werkzeuglimit: wie viele Pressen dürfen *gleichzeitig* für diesen Artikel laufen?
        if press not in presses_for_article[artikel] and len(presses_for_article[artikel]) >= tools_limit[artikel]:
            return 0.0
        take = min(minutes, capacity_left.get(press, 0.0), remain_min.get(artikel, 0.0))
        if take <= 0:
            return 0.0
        plan_rows_local.append({"StandID": stand_id, "Presse": press, "Artikel": artikel, "Dauer_min": float(take)})
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
                    # Werkzeuglimit: wenn wir bereits max Pressen für a belegt haben, nur noch vorhandene Pressen zulassen
                    if len(presses_for_article[a]) >= tools_limit[a]:
                        poss=[p for p in poss if p in presses_for_article[a]]  # nur auf bereits genutzten Pressen
                    if not poss: feas=False; break
                    poss.sort(key=lambda p: -capacity_left.get(p,0))
                    pick=poss[0]; mapping[a]=pick; used.add(pick)
                if feas and len(mapping)==k:
                    chosen=(comb,sum_r,sp,mapping); break
            if not chosen: continue

            comb,sum_r,sp,mapping = chosen
            cell_id=f"{k}er-{stand_id}-{cell_counter}"; cell_counter+=1
            cell_rows.append({
                "CellID": cell_id, "StandID": stand_id, "Typ": f"{k}er}",
                "Kombi_Artikel": " + ".join(comb), "Summe_r": sum_r, "Spread": sp,
                **{f"Art{i+1}": comb[i] for i in range(k)},
                **{f"Presse{i+1}": mapping[comb[i]] for i in range(k)},
            })
            for a in comb:
                add_plan(stand_id, mapping[a], a, remain_min.get(a,0.0))
            used_stands_in_round.add(stand_id); progress=True

    return pd.DataFrame(cell_rows), plan_rows_local

# ---- Planung + Rüstzeiten + Gantt ----
def plan_all(
    sheets: Dict[str, pd.DataFrame],
    params: PlanParams,
    bedarfe_override_kw: Optional[pd.DataFrame] = None,
):
    pressen = sheets["Pressen"].copy()
    staende = sheets["Staende"].copy()
    freigaben = sheets["Freigaben_Werkzeug_Presse"].copy()
    zyklen = sheets["Zykluszeiten"].copy()
    bedarfe = sheets["Bedarfe_Woche"].copy()
    kap = sheets["Kapazitaet_Woche"].copy()

    # optionales Werkzeuge-Sheet
    tools_map: Dict[str,int] = defaultdict(lambda: 9999)  # praktisch unlimitiert, wenn nicht angegeben
    tools_sheet = None
    for cand in ["Werkzeuge", "Anzahl_Werkzeuge"]:
        if cand in sheets:
            tools_sheet = sheets[cand].copy()
            break
    if tools_sheet is not None and params.limit_by_tools:
        tools_sheet.columns = [c.strip() for c in tools_sheet.columns]
        # erlaubte Spaltennamen toleranter interpretieren
        art_col = "Artikel"
        cnt_col = [c for c in tools_sheet.columns if "anzahl" in c.lower()][0] if not "Anzahl_Werkzeuge" in tools_sheet.columns else "Anzahl_Werkzeuge"
        tmp = tools_sheet[[art_col, cnt_col]].dropna()
        tools_map.update({str(r[art_col]): int(r[cnt_col]) for _, r in tmp.iterrows()})

    # Flags
    pressen, freigaben = normalize_flags(pressen, freigaben)
    zyklen = build_time_features(zyklen)

    # Filter
    if params.enforce_freigefahren:
        freigaben = freigaben[freigaben["Freigefahren_bool"]]
    if params.exclude:
        freigaben = freigaben[~freigaben["Artikel"].isin(params.exclude)]
        zyklen   = zyklen[~zyklen["Artikel"].isin(params.exclude)]
        bedarfe  = bedarfe[~bedarfe["Artikel"].isin(params.exclude)]

    bedarfe["KW_num"] = pd.to_numeric(bedarfe["KW"], errors="coerce")
    if params.kw not in set(bedarfe["KW_num"].dropna().astype(int)):
        raise ValueError(f"KW {params.kw} ist in Bedarfe_Woche nicht vorhanden.")

    # Wenn Bedarfe editiert wurden, diese verwenden
    if bedarfe_override_kw is not None:
        bedarfe = bedarfe.copy()
        bedarfe.loc[bedarfe["KW_num"]==params.kw, ["Artikel","Bedarf"]] = \
            bedarfe_override_kw[["Artikel","Bedarf"]].values

    # Kapazität (min)
    pressen_active = pressen[pressen["Aktiv_bool"]][["PressID","Pressenname","StandID"]]
    kap_active = kap.merge(pressen_active[["PressID"]], left_on="Presse_ID", right_on="PressID", how="inner")
    kap_active["Avail_min"] = kap_active["Verfuegbare_Minuten"].fillna(0) - kap_active["Wartung_Minuten"].fillna(0)
    capacity_left: Dict[str, float] = dict(zip(kap_active["Presse_ID"], kap_active["Avail_min"]))

    # Relevante Kombinationen
    rel = (
        bedarfe[bedarfe["KW_num"]==params.kw][["Artikel","Bedarf"]]
        .merge(freigaben[["Artikel","Pressenname","Cavity"]], on="Artikel", how="inner")
        .merge(pressen_active, on="Pressenname", how="inner")
        .merge(zyklen[["Artikel","Schließzeit","r","Gesamtzyklus"]], on="Artikel", how="inner")
    )
    if rel.empty:
        raise RuntimeError("Keine relevanten Datensätze (Bedarf + zulässige Pressen + Zeiten).")

    # Demand-Minuten
    cavity_per_art = rel.groupby("Artikel")["Cavity"].max().to_dict()
    dem = demand_minutes(bedarfe[bedarfe["KW_num"]==params.kw][["Artikel","Bedarf"]], zyklen, cavity_per_art)

    r_map = dem.set_index("Artikel")["r"].to_dict()
    s_map = dem.set_index("Artikel")["Schließzeit"].to_dict()
    remain_min: Dict[str, float] = dem.set_index("Artikel")["Bedarfsminuten"].to_dict()
    min_per_piece = dem.set_index("Artikel")["Min_pro_Stk"].to_dict()

    # allowed/presses/limits
    allowed_by_stand: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))
    for _, row in rel.iterrows():
        allowed_by_stand[row["StandID"]][row["Artikel"]].add(str(row["PressID"]))
    presses_by_stand = pressen_active.groupby("StandID")["PressID"].apply(lambda s: list(map(str, s))).to_dict()
    stand_limit_map = staende.set_index("StandID")["MaxMachinesPerWerker"].to_dict()
    articles = sorted(remain_min.keys())
    tools_limit = {a: int(tools_map.get(a, 9999)) for a in articles}
    presses_for_article: Dict[str, Set[str]] = {a: set() for a in articles}

    # Zellen
    cell_frames=[]; plan_rows_all: List[Dict] = []
    def run_cells(k:int):
        nonlocal cell_frames, plan_rows_all
        c,p = build_cells(
            k, articles, remain_min, r_map, s_map, allowed_by_stand, presses_by_stand, capacity_left,
            params.sum_r_limit, params.max_spread, params.respect_stand_limit, stand_limit_map,
            params.allow_multiple_cells_per_stand, tools_limit, presses_for_article
        )
        if not c.empty: cell_frames.append(c); plan_rows_all.extend(p)

    if "3er" in params.cell_strategy: run_cells(3)
    if ("2er" in params.cell_strategy) or (params.cell_strategy=="nur 2er → Einzel"): run_cells(2)

    # Restbedarf → Einzel
    def add_plan(stand_id: str, press: str, artikel: str, minutes: float):
        if press not in presses_for_article[artikel] and len(presses_for_article[artikel]) >= tools_limit[artikel]:
            return 0.0
        take = min(minutes, capacity_left.get(press,0.0), remain_min.get(artikel,0.0))
        if take<=0: return 0.0
        plan_rows_all.append({"StandID":stand_id,"Presse":press,"Artikel":artikel,"Dauer_min":float(take)})
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
            take = add_plan(str(r["StandID"]), str(r["PressID"]), a, need)
            need -= take

    cells_df = pd.concat(cell_frames, ignore_index=True) if cell_frames else pd.DataFrame(columns=["Hinweis"], data=[["Keine Zellen gebildet"]])
    plan_df = pd.DataFrame(plan_rows_all, columns=["StandID","Presse","Artikel","Dauer_min"])

    # Namen
    name_map = pressen_active.set_index("PressID")["Pressenname"].to_dict()
    stand_name_map = staende.set_index("StandID")["StandName"].to_dict()
    plan_df["Pressenname"] = plan_df["Presse"].astype(str).map(name_map)
    plan_df["StandName"]   = plan_df["StandID"].map(stand_name_map)
    plan_df = plan_df[["StandID","StandName","Presse","Pressenname","Artikel","Dauer_min"]] \
                 .sort_values(["StandID","Presse","Artikel"]).reset_index(drop=True)

    # --- Schedule mit Rüstzeiten ---
    start_dt = datetime.combine(date.fromisocalendar(params.gantt_year, params.kw, 1), time(hour=params.shift_start_hour))
    sch_rows = []; plan_adjusted = []

    # Gesamtkapazität je Presse (für eventuelle Kürzungen wegen Rüstzeiten)
    cap_total = kap_active.set_index("Presse_ID")["Avail_min"].to_dict()

    for (press, stand), g in plan_df.groupby(["Presse","StandID"], sort=False):
        # Reihenfolge: Artikel mit größter Dauer zuerst (minimiert Fragmentierung)
        g = g.sort_values("Dauer_min", ascending=False).copy()
        t0 = start_dt
        total_task_min = float(g["Dauer_min"].sum())
        distinct_articles = list(g["Artikel"].unique())
        ruest_needed = params.setup_minutes * max(0, len(distinct_articles)-1)
        over = max(0.0, total_task_min + ruest_needed - cap_total.get(press, total_task_min + ruest_needed))
        # Kürzung der letzten Aufgabe, falls Kapazität überzogen
        if over > 1e-6:
            # Kürze die kleinste Restaufgabe (hier: letzte Zeile nach Sortierung = kleinste)
            idx_last = g.index[-1]
            g.loc[idx_last, "Dauer_min"] = max(0.0, float(g.loc[idx_last, "Dauer_min"]) - over)

        # Jetzt sequenzieren und Rüstjobs einfügen
        prev_art = None
        for _, r in g.iterrows():
            dur = float(r["Dauer_min"])
            if dur <= 0: continue
            if prev_art is not None and r["Artikel"] != prev_art and params.setup_minutes>0:
                # Rüstzeitblock
                sch_rows.append({
                    "StandID": r["StandID"], "StandName": r["StandName"],
                    "Presse": press, "Pressenname": r["Pressenname"],
                    "Artikel": f"RÜSTEN ({prev_art}→{r['Artikel']})",
                    "Start": t0, "Ende": t0 + timedelta(minutes=params.setup_minutes),
                    "Dauer_min": float(params.setup_minutes)
                })
                t0 = t0 + timedelta(minutes=params.setup_minutes)
            # Produkt
            sch_rows.append({
                "StandID": r["StandID"], "StandName": r["StandName"],
                    "Presse": press, "Pressenname": r["Pressenname"],
                    "Artikel": r["Artikel"], "Start": t0, "Ende": t0 + timedelta(minutes=dur),
                    "Dauer_min": dur
            })
            plan_adjusted.append(r.to_dict())  # zum erneuten Plan-Export
            t0 = t0 + timedelta(minutes=dur)
            prev_art = r["Artikel"]

    schedule_df = pd.DataFrame(sch_rows).sort_values(["StandID","Presse","Start"])
    plan_df_final = plan_df.copy()  # (für Export behalten wir die Minuten je Presse/Artikel; Kürzung ist in Schedule berücksichtigt)

    # Coverage neu berechnen anhand schedule_df (ohne RÜSTEN)
    prod_minutes = schedule_df[~schedule_df["Artikel"].str.startswith("RÜSTEN")].groupby("Artikel")["Dauer_min"].sum()
    dem = dem.copy()
    dem["Geplante_Minuten"] = dem["Artikel"].map(prod_minutes).fillna(0.0)
    dem["Geplante_Stk"] = dem["Geplante_Minuten"] / dem["Min_pro_Stk"]
    dem["Deckungsgrad_%"] = (dem["Geplante_Minuten"] / dem["Bedarfsminuten"]).clip(upper=1.0) * 100.0
    dem["Restminuten"] = (dem["Bedarfsminuten"] - dem["Geplante_Minuten"]).clip(lower=0.0)
    dem["Restmenge_Stk"] = dem["Restminuten"] / dem["Min_pro_Stk"]
    art_df = dem[["Artikel","Bedarf","Min_pro_Stk","Bedarfsminuten","Geplante_Minuten","Geplante_Stk","Deckungsgrad_%","Restminuten","Restmenge_Stk"]]

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

    return cells_df, plan_df_final, art_df, schedule_df, kpis

# ----------------------------- Ablauf -----------------------------
if up is None:
    st.info("Bitte Excel laden – danach erscheinen Bedarfs-Editor, Parameter & Ergebnisse.")
    st.stop()

sheets = read_workbook(up)
needed = {"Pressen","Staende","Freigaben_Werkzeug_Presse","Zykluszeiten","Bedarfe_Woche","Kapazitaet_Woche"}
missing = sorted(needed - set(sheets))
if missing:
    st.error(f"Fehlende Sheets: {', '.join(missing)}")
    st.stop()

# KW wählen + Bedarfe editieren
bedarfe_df = sheets["Bedarfe_Woche"].copy()
bedarfe_df["KW_num"] = pd.to_numeric(bedarfe_df["KW"], errors="coerce")
kw_options = sorted(set(bedarfe_df["KW_num"].dropna().astype(int)))
selected_kw = st.sidebar.selectbox("Kalenderwoche (KW)", kw_options, index=max(0, len(kw_options)-1))

# Ausschlüsse (nur Artikel der KW)
kw_articles = sorted(set(bedarfe_df[bedarfe_df["KW_num"]==selected_kw]["Artikel"]))
selected_exclude = st.sidebar.multiselect("Artikel ausschließen", options=kw_articles, default=[a for a in kw_articles if a in exclude_default])

# Editor
st.subheader(f"Bedarfe editieren – KW {selected_kw}")
bedarfe_kw = bedarfe_df[bedarfe_df["KW_num"]==selected_kw][["Artikel","Bedarf"]].copy().reset_index(drop=True)
edited_bedarfe_kw = st.data_editor(bedarfe_kw, num_rows="fixed", use_container_width=True, key="bedarfs_editor")

# Plan-Button
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
        gantt_year=int(gantt_year),
        shift_grid=bool(shift_grid),
        shift_start_hour=int(shift_start_hour),
    )
    cells_df, plan_df, art_df, schedule_df, kpis = plan_all(sheets, params, bedarfe_override_kw=edited_bedarfe_kw)

# ----------------------------- Anzeigen -----------------------------
c1,c2,c3,c4 = st.columns(4)
c1.metric("KW", kpis["KW"])
c2.metric("Σ Bedarf (min)", f"{kpis['Σ Bedarfsmin']:.0f}")
c3.metric("Σ geplant (min)", f"{kpis['Σ geplant (min)']:.0f}")
c4.metric("Deckungsgrad gesamt", f"{kpis['Deckungsgrad gesamt']:.1f}%")

st.subheader("Gebildete Zellen")
st.dataframe(cells_df, use_container_width=True)

st.subheader("Plan je Presse / Artikel (Minuten)")
st.dataframe(plan_df, use_container_width=True)

st.subheader("Deckung je Artikel")
st.dataframe(art_df.sort_values(["Deckungsgrad_%","Bedarfsminuten"], ascending=[True, False]), use_container_width=True)

st.subheader("Ablaufplan je Presse (Start/Ende) inkl. Rüstzeiten")
# Y-Label mit Stand sichtbar
if not schedule_df.empty:
    schedule_df = schedule_df.copy()
    schedule_df["YLabel"] = schedule_df.apply(
        lambda r: f"S{r['StandID']} | {r['StandName']} | {r['Pressenname']}", axis=1
    )
    st.dataframe(schedule_df, use_container_width=True)

# ----------------------------- Gantt mit Schichtgitter -----------------------------
if not schedule_df.empty:
    fig = px.timeline(
        schedule_df,
        x_start="Start", x_end="Ende",
        y="YLabel",
        color="Artikel",
        hover_data=["StandName","Presse","Dauer_min"],
        title=f"Gantt – KW {selected_kw} (Start Mo {params.shift_start_hour:02d}:00)"
    )
    fig.update_yaxes(autorange="reversed")

    if params.shift_grid:
        # Schichten (3×8h) für 7 Tage ab Mo <shift_start>
        start = schedule_df["Start"].min()
        # Anker: Mo der KW + shift_start_hour
        anchor = datetime.combine(date.fromisocalendar(params.gantt_year, params.kw, 1), time(hour=params.shift_start_hour))
        # 21 Schichten (7 Tage * 3 Schichten)
        stripes=[]
        labels=[]
        sh_names=["Früh","Spät","Nacht"]
        for d in range(7):
            for s in range(3):
                sh_start = anchor + timedelta(days=d, hours=8*s)
                sh_end   = sh_start + timedelta(hours=8)
                color = "rgba(100,100,100,0.06)" if s%2==0 else "rgba(160,160,160,0.06)"
                stripes.append(dict(type="rect", xref="x", yref="paper", x0=sh_start, x1=sh_end, y0=0, y1=1, fillcolor=color, line=dict(width=0)))
                labels.append((sh_start, sh_names[s]))
        fig.update_layout(shapes=stripes)
        # kleine Schichtlabels oben
        for (x, txt) in labels:
            fig.add_annotation(x=x + timedelta(hours=4), y=1.02, xref="x", yref="paper",
                               text=txt, showarrow=False, font=dict(size=10), align="center")

    st.plotly_chart(fig, use_container_width=True, theme="streamlit")

# ----------------------------- Downloads -----------------------------
st.subheader("Downloads")

def to_xlsx_bytes() -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        (cells_df if not cells_df.empty else pd.DataFrame({"Hinweis":["Keine Zellen gebildet"]})).to_excel(writer, sheet_name="Zellen", index=False)
        plan_df.to_excel(writer, sheet_name="Plan_je_Presse", index=False)
        art_df.to_excel(writer, sheet_name="Coverage_je_Artikel", index=False)
        (schedule_df if not schedule_df.empty else pd.DataFrame({"Hinweis":["Kein Schedule"]})).to_excel(writer, sheet_name="Schedule", index=False)
    return output.getvalue()

st.download_button(
    label="📥 Excel-Export",
    data=to_xlsx_bytes(),
    file_name=f"Plan_KW{selected_kw}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)
st.download_button(
    label="📥 Plan (CSV)",
    data=plan_df.to_csv(index=False).encode("utf-8"),
    file_name=f"Plan_KW{selected_kw}.csv",
    mime="text/csv",
)
