"""
PSE API V2 Connector for Energy Prophet.
Architecture: Wide Tables (Integrated Data Warehouse)
"""

import logging
import os
import requests
import pandas as pd
import pyodbc
import datetime
from typing import Dict, Optional

PSE_API_BASE = "https://api.raporty.pse.pl/api"

# Zgrupowane endpointy - dane trafiają do TYCH SAMYCH tabel
ENDPOINT_CONFIG = {
    # TABELA: energy_prices
    "rce-pln": {"target_table": "energy_prices", "delay_days": 1},

    # TABELA: generation_mix (MERGED: actuals + forecasts + load)
    "his-wlk-cal": {"target_table": "generation_mix", "delay_days": 1, "role": "actuals"},
    "kse-load": {"target_table": "generation_mix", "delay_days": 1, "role": "load_forecast"},
    "pk5l-wp": {"target_table": "generation_mix", "delay_days": 1, "role": "oze_forecast"},

    # TABELA: power_balance (MERGED: reserves + daily plan)
    "his-bil-mocy": {"target_table": "power_balance", "delay_days": 1, "role": "reserves"},
    "pdgopkd": {"target_table": "power_balance", "delay_days": 1, "role": "daily_plan"},

    # TABELA: cross_border_flows
    "przeplywy-mocy": {"target_table": "cross_border_flows", "delay_days": 1},

    # TABELA: pse_alerts
    "pdgsz": {"target_table": "pse_alerts", "delay_days": 1},

    # TABELA: oze_curtailment
    "poze-redoze": {"target_table": "oze_curtailment", "delay_days": 1},

    # TABELA: co2_prices (enrichment)
    "rcco2": {"target_table": "co2_prices", "delay_days": 1},
    "unav-pk5l": {"target_table": "planned_outages", "delay_days": 1},

    # TABELA: balancing_settlement (D+7, walidacja)
    "crb-rozl": {"target_table": "balancing_settlement", "delay_days": 7},
    "eb-rozl": {"target_table": "balancing_settlement", "delay_days": 7},
    "en-rozl": {"target_table": "balancing_settlement", "delay_days": 7},
    "ro-rozl": {"target_table": "balancing_settlement", "delay_days": 7},
}


class PSEConnector:
    def __init__(self, base_url: str = PSE_API_BASE, timeout: int = 30):
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "EnergyProphet/1.0"
        })

    def _connect_with_retry(self, conn_str: str, max_retries: int = 5):
        """Połączenie SQL z retry logic i Connection Timeout."""
        import time
        for attempt in range(max_retries):
            try:
                cs = conn_str
                if 'Connection Timeout' not in cs:
                    cs += ';Connection Timeout=30'
                return pyodbc.connect(cs)
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 10
                    logging.warning(f"  ⚠️ SQL connection failed. Retrying in {wait_time}s... (Attempt {attempt + 1}/{max_retries}): {e}")
                    time.sleep(wait_time)
                else:
                    raise

    def _clean_float(self, value) -> Optional[float]:
        """Convert value to float, handling None, empty strings, NaN, and whitespace."""
        if value is None:
            return None
        if pd.isna(value):
            return None
        if isinstance(value, str):
            value = value.strip()
            if value == "":
                return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _fetch(self, endpoint: str, date_str: str, max_retries: int = 3) -> Optional[pd.DataFrame]:
        """Pobiera dane z endpointu PSE API z obsługą paginacji (nextLink) i retry."""
        import time as _time

        for attempt in range(max_retries):
            url = f"{self.base_url}/{endpoint}"
            params = {"$filter": f"business_date eq '{date_str}'"}

            all_records = []
            page = 1

            try:
                while url:
                    if page == 1:
                        resp = self.session.get(url, params=params, timeout=self.timeout)
                    else:
                        resp = self.session.get(url, timeout=self.timeout)

                    resp.raise_for_status()
                    data = resp.json()
                    records = data.get("value", []) if isinstance(data, dict) else data

                    if records:
                        all_records.extend(records)

                    next_link = data.get("nextLink") if isinstance(data, dict) else None
                    if next_link:
                        url = next_link
                        page += 1
                    else:
                        url = None

                if all_records:
                    logging.info(f"  ✓ {endpoint}: {len(all_records)} rows" + (f" ({page} pages)" if page > 1 else ""))
                    return pd.DataFrame(all_records)

                logging.warning(f"  ⚠ {endpoint}: empty")
                return pd.DataFrame()

            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5
                    logging.warning(f"  ⚠️ {endpoint}: fetch failed. Retrying in {wait_time}s... (Attempt {attempt + 1}/{max_retries}): {e}")
                    _time.sleep(wait_time)
                else:
                    logging.error(f"  ✗ {endpoint}: failed after {max_retries} attempts: {e}")
                    return None

    def run_etl(self, run_date: datetime.date):
        """Główna funkcja ETL."""
        logging.info(f"\n{'='*60}")
        logging.info(f"ETL RUN: {run_date}")
        logging.info(f"{'='*60}")

        conn_str = os.environ.get("SqlConnectionString")
        if not conn_str:
            logging.error("❌ Missing SqlConnectionString")
            return

        # Grupuj po dacie źródłowej
        date_tasks: Dict[str, list] = {}
        for ep, cfg in ENDPOINT_CONFIG.items():
            target_date = run_date - datetime.timedelta(days=cfg["delay_days"])
            date_key = target_date.strftime('%Y-%m-%d')
            date_tasks.setdefault(date_key, []).append(ep)

        # Przetwarzaj każdą datę
        for date_str, endpoints in sorted(date_tasks.items()):
            logging.info(f"\n📅 {date_str}")

            # Pobierz wszystkie dane dla tej daty
            raw: Dict[str, pd.DataFrame] = {}
            for ep in endpoints:
                df = self._fetch(ep, date_str)
                if df is not None and not df.empty:
                    raw[ep] = df

            if not raw:
                continue

            # Upload do SQL (z retry na cały blok connect+execute+commit)
            import time as _time
            max_sql_retries = 3
            for sql_attempt in range(max_sql_retries):
                try:
                    with self._connect_with_retry(conn_str) as conn:
                        cursor = conn.cursor()

                        # 1. ENERGY_PRICES
                        if 'rce-pln' in raw:
                            self._upsert_prices(cursor, raw['rce-pln'])

                        # 2. GENERATION_MIX (merged)
                        self._upsert_generation_mix(
                            cursor,
                            actuals=raw.get('his-wlk-cal'),
                            load_fcst=raw.get('kse-load'),
                            oze_fcst=raw.get('pk5l-wp')
                        )

                        # 3. POWER_BALANCE (merged)
                        self._upsert_power_balance(
                            cursor,
                            reserves=raw.get('his-bil-mocy'),
                            daily_plan=raw.get('pdgopkd')
                        )

                        # 4. CROSS_BORDER_FLOWS
                        if 'przeplywy-mocy' in raw:
                            self._upsert_flows(cursor, raw['przeplywy-mocy'])

                        # 5. PSE_ALERTS
                        if 'pdgsz' in raw:
                            self._upsert_alerts(cursor, raw['pdgsz'])

                        # 6. OZE_CURTAILMENT
                        if 'poze-redoze' in raw:
                            self._upsert_curtailment(cursor, raw['poze-redoze'])

                        # 7. CO2_PRICES
                        if 'rcco2' in raw:
                            self._upsert_co2(cursor, raw['rcco2'])

                        # 8. SETTLEMENT (D+7)
                        for ep in ['crb-rozl', 'eb-rozl', 'en-rozl', 'ro-rozl']:
                            if ep in raw:
                                self._upsert_settlement(cursor, raw[ep], ep)

                        # 9. PLANNED OUTAGES
                        if 'unav-pk5l' in raw:
                                self._upsert_outages(cursor, raw['unav-pk5l'])

                        conn.commit()
                        logging.info(f"  ✅ Committed")
                        break  # sukces

                except Exception as e:
                    if sql_attempt < max_sql_retries - 1:
                        wait_time = (sql_attempt + 1) * 15
                        logging.warning(f"  ⚠️ SQL batch failed. Retrying in {wait_time}s... (Attempt {sql_attempt + 1}/{max_sql_retries}): {e}")
                        _time.sleep(wait_time)
                    else:
                        logging.error(f"  ❌ SQL batch failed after {max_sql_retries} attempts: {e}")

    # =========================================================================
    # UPSERT FUNCTIONS
    # =========================================================================

    def _upsert_prices(self, cursor, df: pd.DataFrame):
        """energy_prices ← rce-pln"""
        logging.info("    → energy_prices")
        sql = """
        MERGE INTO energy_prices AS T
        USING (SELECT ? AS dtime, ? AS business_date, ? AS rce_pln) AS S
        ON T.dtime = S.dtime
        WHEN MATCHED THEN UPDATE SET rce_pln = S.rce_pln
        WHEN NOT MATCHED THEN INSERT (dtime, business_date, rce_pln) 
            VALUES (S.dtime, S.business_date, S.rce_pln);
        """
        for _, r in df.iterrows():
            cursor.execute(sql, r['dtime'], r['business_date'],
                self._clean_float(r.get('rce_pln')))

    def _upsert_generation_mix(self, cursor, actuals: pd.DataFrame,
                                load_fcst: pd.DataFrame, oze_fcst: pd.DataFrame):
        """
        generation_mix ← his-wlk-cal + kse-load + pk5l-wp (MERGED)

        Strategia:
        1. his-wlk-cal daje actuals (pv, wi, jg, demand, jnwrb, jgm, jgo) - 96 rows/day (15min)
        2. kse-load daje load_fcst vs load_actual - 96 rows/day (15min)
        3. pk5l-wp daje oze forecasts - 24 rows/day (1h) → upsample do 15min

        Kolumny z his-wlk-cal:
        - pv: fotowoltaika
        - wi: wiatr
        - jg: jednostki gazowe
        - jnwrb: jednostki niezgłoszone (unregistered units)
        - jgm: magazyny energii (storage)
        - jgo: inne OZE (other renewables)
        """
        logging.info("    → generation_mix")

        if actuals is None or actuals.empty:
            return

        # Prepare forecast lookup (hourly → 15min)
        fcst_lookup = {}
        if oze_fcst is not None and not oze_fcst.empty:
            for _, r in oze_fcst.iterrows():
                hour_key = pd.to_datetime(r['plan_dtime']).floor('h')
                fcst_lookup[hour_key] = {
                    'wi_fcst': r.get('fcst_wi_tot_gen'),
                    'pv_fcst': r.get('fcst_pv_tot_gen'),
                    'demand_fcst_pk5l': r.get('grid_demand_fcst')
                }

        # Prepare load forecast lookup
        load_lookup = {}
        if load_fcst is not None and not load_fcst.empty:
            for _, r in load_fcst.iterrows():
                load_lookup[r['dtime']] = {
                    'demand_fcst': r.get('load_fcst'),
                    'demand_actual': r.get('load_actual')
                }

        # Main upsert
        sql = """
        MERGE INTO generation_mix AS T
        USING (SELECT ? AS dtime, ? AS business_date,
                      ? AS pv_mw, ? AS wi_mw, ? AS jg_mw, ? AS demand_mw,
                      ? AS swm_total_mw, ? AS demand_fcst_mw,
                      ? AS wi_fcst_mw, ? AS pv_fcst_mw,
                      ? AS jnwrb_mw, ? AS jgm_mw, ? AS jgo_mw) AS S
        ON T.dtime = S.dtime
        WHEN MATCHED THEN UPDATE SET
            pv_mw = S.pv_mw, wi_mw = S.wi_mw, jg_mw = S.jg_mw,
            demand_mw = S.demand_mw, swm_total_mw = S.swm_total_mw,
            demand_fcst_mw = S.demand_fcst_mw,
            wi_fcst_mw = S.wi_fcst_mw, pv_fcst_mw = S.pv_fcst_mw,
            jnwrb_mw = S.jnwrb_mw, jgm_mw = S.jgm_mw, jgo_mw = S.jgo_mw
        WHEN NOT MATCHED THEN INSERT
            (dtime, business_date, pv_mw, wi_mw, jg_mw, demand_mw,
             swm_total_mw, demand_fcst_mw, wi_fcst_mw, pv_fcst_mw,
             jnwrb_mw, jgm_mw, jgo_mw)
            VALUES (S.dtime, S.business_date, S.pv_mw, S.wi_mw, S.jg_mw,
                    S.demand_mw, S.swm_total_mw, S.demand_fcst_mw,
                    S.wi_fcst_mw, S.pv_fcst_mw,
                    S.jnwrb_mw, S.jgm_mw, S.jgo_mw);
        """

        for _, r in actuals.iterrows():
            dtime = pd.to_datetime(r['dtime'])
            hour_key = dtime.floor('h')

            # Get forecasts (upsampled from hourly)
            fcst = fcst_lookup.get(hour_key, {})
            load = load_lookup.get(r['dtime'], {})

            # Prefer kse-load forecast, fallback to pk5l-wp
            demand_fcst = load.get('demand_fcst') or fcst.get('demand_fcst_pk5l')

            swm_total = float(r.get('swm_p') or 0) + float(r.get('swm_np') or 0)

            cursor.execute(sql,
                r['dtime'],
                r['business_date'],
                r.get('pv'),
                r.get('wi'),
                r.get('jg'),
                r.get('demand'),
                swm_total,
                demand_fcst,
                fcst.get('wi_fcst'),
                fcst.get('pv_fcst'),
                r.get('jnwrb'),  # jednostki niezgłoszone (unregistered units)
                r.get('jgm'),    # magazyny (storage)
                r.get('jgo'),    # inne OZE (other renewables)
            )

    def _upsert_power_balance(self, cursor, reserves: pd.DataFrame,
                               daily_plan: pd.DataFrame):
        """
        power_balance ← his-bil-mocy + pdgopkd (MERGED)

        Strategia:
        1. his-bil-mocy daje rezerwy (2 rows/day: SR, SW)
        2. pdgopkd daje plan dzienny (96 rows/day) → agregujemy do szczytów
        """
        logging.info("    → power_balance")

        if reserves is None or reserves.empty:
            return

        # Aggregate daily plan to peaks (SR=morning ~13:30, SW=evening ~18:00)
        plan_agg = {'SR': {}, 'SW': {}}
        if daily_plan is not None and not daily_plan.empty:
            for _, r in daily_plan.iterrows():
                hour = pd.to_datetime(r['dtime']).hour
                # SR peak: 12-15, SW peak: 17-20
                peak = 'SR' if 12 <= hour <= 15 else ('SW' if 17 <= hour <= 20 else None)
                if peak:
                    # Bierzemy max niedoboru i max ograniczeń w oknie szczytu
                    plan_agg[peak]['rez_under'] = max(
                        plan_agg[peak].get('rez_under', 0),
                        float(r.get('rez_under') or 0)
                    )
                    plan_agg[peak]['ogr_mwe'] = max(
                        plan_agg[peak].get('ogr_mwe', 0),
                        float(r.get('ogr_mwe') or 0)
                    )

        sql = """
        MERGE INTO power_balance AS T
        USING (SELECT ? AS business_date, ? AS peak_type, ? AS peak_hour,
                      ? AS reserves_total_mw, ? AS reserves_spinning_mw,
                      ? AS reserves_cold_mw, ? AS reserves_storage_mw,
                      ? AS demand_mw, ? AS swm_mw,
                      ? AS rez_under_mw, ? AS ogr_mwe_mw) AS S
        ON T.business_date = S.business_date AND T.peak_type = S.peak_type
        WHEN MATCHED THEN UPDATE SET 
            peak_hour = S.peak_hour,
            reserves_total_mw = S.reserves_total_mw,
            reserves_spinning_mw = S.reserves_spinning_mw,
            reserves_cold_mw = S.reserves_cold_mw,
            reserves_storage_mw = S.reserves_storage_mw,
            demand_mw = S.demand_mw, swm_mw = S.swm_mw,
            rez_under_mw = S.rez_under_mw, ogr_mwe_mw = S.ogr_mwe_mw
        WHEN NOT MATCHED THEN INSERT 
            (business_date, peak_type, peak_hour, reserves_total_mw,
             reserves_spinning_mw, reserves_cold_mw, reserves_storage_mw,
             demand_mw, swm_mw, rez_under_mw, ogr_mwe_mw) 
            VALUES (S.business_date, S.peak_type, S.peak_hour, S.reserves_total_mw,
                    S.reserves_spinning_mw, S.reserves_cold_mw, S.reserves_storage_mw,
                    S.demand_mw, S.swm_mw, S.rez_under_mw, S.ogr_mwe_mw);
        """

        for _, r in reserves.iterrows():
            peak_type = r.get('peak')
            plan = plan_agg.get(peak_type, {})

            cursor.execute(sql,
                r['business_date'],
                peak_type,
                r.get('peak_hour'),
                r.get('rez'),
                r.get('rez_jgw_wir'),
                r.get('rez_jgw_zim'),
                r.get('rez_jgm'),
                r.get('demand'),
                r.get('swm'),
                plan.get('rez_under'),
                plan.get('ogr_mwe')
            )

    def _upsert_flows(self, cursor, df: pd.DataFrame):
        """cross_border_flows ← przeplywy-mocy"""
        logging.info("    → cross_border_flows")
        sql = """
        MERGE INTO cross_border_flows AS T
        USING (SELECT ? AS dtime, ? AS business_date, ? AS section_code, ? AS value_mw) AS S
        ON T.dtime = S.dtime AND T.section_code = S.section_code
        WHEN MATCHED THEN UPDATE SET value_mw = S.value_mw
        WHEN NOT MATCHED THEN INSERT (dtime, business_date, section_code, value_mw) 
            VALUES (S.dtime, S.business_date, S.section_code, S.value_mw);
        """
        for _, r in df.iterrows():
            cursor.execute(sql, r['dtime'], r['business_date'], r['section_code'],
                self._clean_float(r.get('value')))

    def _upsert_alerts(self, cursor, df: pd.DataFrame):
        """pse_alerts ← pdgsz (deduplicated)"""
        logging.info("    → pse_alerts")

        # Deduplikacja - ostatni status per godzina
        df = df.sort_values('publication_ts').groupby('dtime').last().reset_index()

        sql = """
        MERGE INTO pse_alerts AS T
        USING (SELECT ? AS dtime, ? AS business_date, ? AS is_active, ? AS usage_level) AS S
        ON T.dtime = S.dtime
        WHEN MATCHED THEN UPDATE SET is_active = S.is_active, usage_level = S.usage_level
        WHEN NOT MATCHED THEN INSERT (dtime, business_date, is_active, usage_level) 
            VALUES (S.dtime, S.business_date, S.is_active, S.usage_level);
        """
        for _, r in df.iterrows():
            is_active = 1 if str(r.get('is_active')).lower() == 'true' else 0
            cursor.execute(sql, r['dtime'], r['business_date'], is_active, r.get('usage_fcst'))

    def _upsert_curtailment(self, cursor, df: pd.DataFrame):
        """oze_curtailment ← poze-redoze"""
        logging.info("    → oze_curtailment")
        sql = """
        MERGE INTO oze_curtailment AS T
        USING (SELECT ? AS dtime, ? AS business_date, 
                      ? AS pv_red_balance, ? AS pv_red_network,
                      ? AS wi_red_balance, ? AS wi_red_network) AS S
        ON T.dtime = S.dtime
        WHEN MATCHED THEN UPDATE SET 
            pv_red_balance_mwh = S.pv_red_balance, pv_red_network_mwh = S.pv_red_network,
            wi_red_balance_mwh = S.wi_red_balance, wi_red_network_mwh = S.wi_red_network
        WHEN NOT MATCHED THEN INSERT 
            (dtime, business_date, pv_red_balance_mwh, pv_red_network_mwh,
             wi_red_balance_mwh, wi_red_network_mwh) 
            VALUES (S.dtime, S.business_date, S.pv_red_balance, S.pv_red_network,
                    S.wi_red_balance, S.wi_red_network);
        """
        for _, r in df.iterrows():
            cursor.execute(sql, r['dtime'], r['business_date'],
                self._clean_float(r.get('pv_red_balance')),
                self._clean_float(r.get('pv_red_network')),
                self._clean_float(r.get('wi_red_balance')),
                self._clean_float(r.get('wi_red_network')))

    def _upsert_co2(self, cursor, df: pd.DataFrame):
        """co2_prices ← rcco2"""
        logging.info("    → co2_prices")
        sql = """
        MERGE INTO co2_prices AS T
        USING (SELECT ? AS business_date, ? AS rcco2_eur, ? AS rcco2_pln) AS S
        ON T.business_date = S.business_date
        WHEN MATCHED THEN UPDATE SET rcco2_eur = S.rcco2_eur, rcco2_pln = S.rcco2_pln
        WHEN NOT MATCHED THEN INSERT (business_date, rcco2_eur, rcco2_pln) 
            VALUES (S.business_date, S.rcco2_eur, S.rcco2_pln);
        """
        for _, r in df.iterrows():
            cursor.execute(sql, r['business_date'],
                self._clean_float(r.get('rcco2_eur')),
                self._clean_float(r.get('rcco2_pln')))

    def _upsert_settlement(self, cursor, df: pd.DataFrame, endpoint: str):
        """balancing_settlement ← crb-rozl / eb-rozl / en-rozl / ro-rozl (D+7)

        Każdy endpoint aktualizuje SWOJE kolumny w jednej wide table.
        Klucz: (business_date, dtime). Wspólne kolumny: period, dtime_utc, period_utc.
        """
        logging.info(f"    → balancing_settlement ({endpoint})")

        # DDL - tworzymy tabelę jeśli nie istnieje (commit osobno aby MERGE widział schemat)
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'balancing_settlement')
        CREATE TABLE balancing_settlement (
            business_date       DATE           NOT NULL,
            dtime               DATETIME2      NOT NULL,
            period              NVARCHAR(20)   NULL,
            dtime_utc           DATETIME2      NULL,
            period_utc          NVARCHAR(20)   NULL,
            cen_cost            DECIMAL(18,5)  NULL,
            ckoeb_cost          DECIMAL(18,5)  NULL,
            ceb_pp_cost         DECIMAL(18,5)  NULL,
            ceb_sr_cost         DECIMAL(18,5)  NULL,
            ceb_sr_afrrd_cost   DECIMAL(18,5)  NULL,
            ceb_sr_afrrg_cost   DECIMAL(18,5)  NULL,
            eb_d_pp             DECIMAL(18,5)  NULL,
            eb_w_pp             DECIMAL(18,5)  NULL,
            eb_afrrd            DECIMAL(18,5)  NULL,
            eb_afrrg            DECIMAL(18,5)  NULL,
            en_d                DECIMAL(18,5)  NULL,
            en_w                DECIMAL(18,5)  NULL,
            balance             DECIMAL(18,5)  NULL,
            ro_cost             DECIMAL(18,5)  NULL,
            created_at          DATETIME2      DEFAULT GETUTCDATE(),
            updated_at          DATETIME2      DEFAULT GETUTCDATE(),
            CONSTRAINT PK_balancing_settlement PRIMARY KEY (business_date, dtime)
        );
        """)
        cursor.connection.commit()

        # Mapowanie endpoint → kolumny do upsert
        column_map = {
            'crb-rozl': {
                'cols': ['cen_cost', 'ckoeb_cost', 'ceb_pp_cost', 'ceb_sr_cost',
                         'ceb_sr_afrrd_cost', 'ceb_sr_afrrg_cost'],
                'fields': ['cen_cost', 'ckoeb_cost', 'ceb_pp_cost', 'ceb_sr_cost',
                           'ceb_sr_afrrd_cost', 'ceb_sr_afrrg_cost'],
            },
            'eb-rozl': {
                'cols': ['eb_d_pp', 'eb_w_pp', 'eb_afrrd', 'eb_afrrg'],
                'fields': ['eb_d_pp', 'eb_w_pp', 'eb_afrrd', 'eb_afrrg'],
            },
            'en-rozl': {
                'cols': ['en_d', 'en_w', 'balance'],
                'fields': ['en_d', 'en_w', 'balance'],
            },
            'ro-rozl': {
                'cols': ['ro_cost'],
                'fields': ['ro_cost'],
            },
        }

        if endpoint not in column_map:
            logging.warning(f"    ⚠ Unknown settlement endpoint: {endpoint}")
            return

        ep_cfg = column_map[endpoint]
        cols = ep_cfg['cols']
        fields = ep_cfg['fields']

        # Budujemy dynamiczny MERGE SQL
        source_cols = ', '.join([f'? AS {c}' for c in cols])
        update_set = ', '.join([f'{c} = S.{c}' for c in cols])
        insert_cols = ', '.join(['business_date', 'dtime', 'period', 'dtime_utc', 'period_utc'] + cols)
        insert_vals = ', '.join(['S.business_date', 'S.dtime', 'S.period', 'S.dtime_utc', 'S.period_utc'] + [f'S.{c}' for c in cols])

        sql = f"""
        MERGE INTO balancing_settlement AS T
        USING (SELECT ? AS business_date, ? AS dtime, ? AS period, ? AS dtime_utc, ? AS period_utc,
                      {source_cols}) AS S
        ON T.business_date = S.business_date AND T.dtime = S.dtime
        WHEN MATCHED THEN UPDATE SET
            {update_set}, updated_at = GETUTCDATE()
        WHEN NOT MATCHED THEN INSERT
            ({insert_cols}, created_at, updated_at)
            VALUES ({insert_vals}, GETUTCDATE(), GETUTCDATE());
        """

        for _, r in df.iterrows():
            params = [
                r.get('business_date'),
                r.get('dtime'),
                r.get('period'),
                r.get('dtime_utc'),
                r.get('period_utc'),
            ]
            for f in fields:
                params.append(self._clean_float(r.get(f)))

            cursor.execute(sql, *params)

    def _upsert_outages(self, cursor, df: pd.DataFrame):
        """Tabela: planned_outages (unav-pk5l)"""
        logging.info("    → planned_outages")
        # To są zdarzenia, więc MERGE robimy po unikalnym kluczu (np. unit_code + start_time)
        # Dla uproszczenia w MVP: Usuwamy wpisy z danego dnia i ładujemy nowe (Snapshot)

        # 1. Delete existing for this business_date (to avoid duplicates on re-run)
        if not df.empty:
            bdate = df.iloc[0]['business_date']
            cursor.execute("DELETE FROM planned_outages WHERE business_date = ?", bdate)

        # 2. Insert fresh data
        sql = """
        INSERT INTO planned_outages (business_date, power_plant, unit_code, reason, start_time, end_time)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        for _, r in df.iterrows():
            # Parsowanie dat (formaty w API mogą być różne, warto użyć pd.to_datetime)
            start = pd.to_datetime(r.get('start_dtime') or r.get('start_dtime_utc'))
            end = pd.to_datetime(r.get('end_dtime') or r.get('end_dtime_utc'))

            cursor.execute(sql,
                           r['business_date'],
                           r.get('power_plant'),
                           r.get('unit_code'),
                           r.get('reason'),
                           start, end
                           )

# Entry point
def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')
    PSEConnector().run_etl(datetime.date.today())

if __name__ == "__main__":
    main()