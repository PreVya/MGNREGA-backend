from datetime import datetime
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from app import models
import logging

logger = logging.getLogger(__name__)

# Helpers: normalize keys and dedupe incoming payloads (keep last occurrence)
def _get_state_code(rec: dict) -> str:
    return (rec.get("state_code") or rec.get("State_Code") or rec.get("State") or rec.get("State_Name"))

def _get_district_code(rec: dict) -> str:
    return (rec.get("district_code") or rec.get("District_Code") or rec.get("District") or rec.get("district_name"))


def dedupe_records(records: list, key_fn) -> list:
    """Return deduplicated list of records keeping the last occurrence for each key derived from key_fn.
    Records without a key are preserved in order (but skipped when key_fn returns falsy).
    """
    if not records:
        return []
    dedup = {}
    orderless = []
    for rec in records:
        try:
            k = key_fn(rec)
        except Exception:
            k = None
        if k:
            dedup[k] = rec
        else:
            orderless.append(rec)
    # keep orderless first (if any), then dedup values in insertion order of last occurrences
    return orderless + list(dedup.values())


def upsert_states(db, records, batch_size: int = 500):
    """Batch insert/upsert states using ON CONFLICT on state_code.
    Deduplicate by state_code within each batch to avoid PG cardinality errors.
    Ensure unique index exists for ON CONFLICT target.
    """
    # clean incoming records: remove duplicates by state_code (keep last)
    records = dedupe_records(records, _get_state_code)

    # ensure unique index exists in DB (no-op if already present)
    try:
        db.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_states_state_code ON states (state_code);")
    except Exception:
        # ignore index creation errors (permissions, already exists with different name, etc.)
        pass

    total = len(records)
    inserted = 0
    for i in range(0, total, batch_size):
        batch = records[i : i + batch_size]
        dedup = {}
        for record in batch:
            state_code = record.get("state_code") or record.get("State_Code")
            state_name = record.get("state_name") or record.get("State") or record.get("State_Name")
            if not state_code or not state_name:
                continue
            # keep last occurrence for a given state_code
            dedup[state_code] = {"state_code": state_code, "state_name": state_name}
        values = list(dedup.values())
        if not values:
            continue
        stmt = insert(models.States).values(values)
        stmt = stmt.on_conflict_do_update(
            index_elements=["state_code"],
            set_={
                "state_name": stmt.excluded.state_name,
                "updated_at": func.now(),
            },
        )
        db.execute(stmt)
        inserted += len(values)
    db.commit()
    return {"message": f"{inserted} states upserted successfully"}


def upsert_districts(db, records, batch_size: int = 500):
    """Batch upsert districts. Prefetch states to avoid per-row queries and dedupe by district_code.
    Ensure unique index exists for district_code to allow ON CONFLICT.
    """
    # dedupe incoming records by district_code before any DB work
    records = dedupe_records(records, _get_district_code)

    try:
        db.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_districts_district_code ON districts (district_code);")
    except Exception:
        pass

    # Prefetch state_code -> id map
    state_rows = db.query(models.States.state_code, models.States.id).all()
    state_map = {r.state_code: r.id for r in state_rows}

    total = len(records)
    inserted_count = 0
    for i in range(0, total, batch_size):
        batch = records[i : i + batch_size]
        dedup = {}
        for record in batch:
            district_name = record.get("District") or record.get("district_name")
            district_code = record.get("District_Code") or record.get("district_code")
            state_code = record.get("State_Code") or record.get("state_code")
            if not district_name or not district_code or not state_code:
                continue
            state_id = state_map.get(state_code)
            if not state_id:
                continue
            # keep last occurrence per district_code
            dedup[district_code] = {
                "district_name": district_name,
                "district_code": district_code,
                "state_id": state_id,
            }
        values = list(dedup.values())
        if not values:
            continue
        insert_stmt = insert(models.Districts).values(values)
        stmt = insert_stmt.on_conflict_do_update(
            index_elements=["district_code"],
            set_={
                "district_name": insert_stmt.excluded.district_name,
                "state_id": insert_stmt.excluded.state_id,
                "updated_at": func.now(),
            },
        )
        db.execute(stmt)
        inserted_count += len(values)
        db.commit()
    return {"message": f"{inserted_count} districts upserted successfully"}


# Mapping from API/raw keys to our model column names
_FIELD_MAP = {
    "Approved_Labour_Budget": "approved_labour_budget",
    "Average_Wage_rate_per_day_per_person": "average_wage_rate_per_day_per_person",
    "Average_days_of_employment_provided_per_Household": "average_days_of_employment_per_household",
    "Differently_abled_persons_worked": "differently_abled_persons_worked",
    "Material_and_skilled_Wages": "material_and_skilled_wages",
    "Number_of_Completed_Works": "number_of_completed_works",
    "Number_of_GPs_with_NIL_exp": "number_of_gp_with_nil_exp",
    "Number_of_Ongoing_Works": "number_of_ongoing_works",
    "Persondays_of_Central_Liability_so_far": "persondays_of_central_liability_so_far",
    "SC_persondays": "sc_persondays",
    "SC_workers_against_active_workers": "sc_workers_against_Active_workers",
    "ST_persondays": "st_persondays",
    "ST_workers_against_active_workers": "st_workers_against_Active_workers",
    "Total_Adm_Expenditure": "total_adm_expenditure",
    "Total_Exp": "total_exp",
    "Total_Households_Worked": "total_households_worked",
    "Total_Individuals_Worked": "total_num_of_individuals_worked",
    "Total_No_of_Active_Job_Cards": "total_num_of_active_job_cards",
    "Total_No_of_Active_Workers": "total_num_of_active_workers",
    "Total_No_of_HHs_completed_100_Days_of_Wage_Employment": "total_num_of_hh_completed_100_day_wage_employment",
    "Total_No_of_JobCards_issued": "total_num_of_job_cards_issued",
    "Total_No_of_Workers": "total_num_of_workers",
    "Total_No_of_Works_Takenup": "total_num_of_works_takenup",
    "Wages": "wages",
    "Women_Persondays": "women_persondays",
    "percent_of_Category_B_Works": "percent_of_category_B_works",
    "percent_of_Expenditure_on_Agriculture_Allied_Works": "percentage_of_expenditure_on_agriculture_allied_works",
    "percent_of_NRM_Expenditure": "percent_of_NRM_expenditure",
    "percentage_payments_gererated_within_15_days": "percentage_payments_generated_within_15_days",
    # fallback keys often used in other codepaths
    "approved_labour_budget": "approved_labour_budget",
    "average_wage_rate_per_day_per_person": "average_wage_rate_per_day_per_person",
    "average_days_of_employment_provided_per_household": "average_days_of_employment_per_household",
    "differently_abled_persons_worked": "differently_abled_persons_worked",
    "material_and_skilled_wages": "material_and_skilled_wages",
    "number_of_completed_works": "number_of_completed_works",
    "number_of_gps_with_nil_exp": "number_of_gp_with_nil_exp",
}


def _coerce_value(v):
    if v is None:
        return None
    if isinstance(v, str):
        v = v.strip()
        if v == "":
            return None
        # try int
        try:
            if v.isdigit():
                return int(v)
            return float(v.replace(',', ''))
        except Exception:
            return v
    return v


def upsert_mgnrega_data(db, records, batch_size: int = 500):
    """Insert or update MGNREGA data without creating duplicates.

    Behavior:
    - Deduplicate incoming records by district_code (keep last occurrence).
    - Ensure referenced states/districts exist (create missing ones).
    - If mgnrega_data table is empty: perform simple bulk INSERTs of deduped rows (no ON CONFLICT).
    - If table has rows: perform batch INSERT ... ON CONFLICT DO UPDATE (create unique index if necessary).

    This prevents duplicate inserts when the DB table is empty and performs updates when data exists.
    """
    if not records:
        return {"message": "no records provided"}

    # dedupe incoming records by district_code first
    records = dedupe_records(records, _get_district_code)

    # --- ensure referenced states exist ---
    state_rows = db.query(models.States.state_code, models.States.id).all()
    state_map = {r.state_code: r.id for r in state_rows}

    state_codes = {}
    for r in records:
        sc = (r.get("state_code") or r.get("State_Code"))
        if sc:
            sn = r.get("state_name") or r.get("State") or r.get("State_Name") or ""
            state_codes[sc] = sn

    missing_states = [ {"state_code": sc, "state_name": state_codes.get(sc, "")} for sc in state_codes.keys() if sc not in state_map ]
    if missing_states:
        try:
            stmt = insert(models.States).values(missing_states)
            stmt = stmt.on_conflict_do_nothing(index_elements=["state_code"])
            db.execute(stmt)
            db.commit()
        except Exception:
            db.rollback()
    # refresh state_map
    state_rows = db.query(models.States.state_code, models.States.id).all()
    state_map = {r.state_code: r.id for r in state_rows}

    # --- ensure referenced districts exist ---
    district_rows = db.query(models.Districts.district_code, models.Districts.id).all()
    district_map = {r.district_code: r.id for r in district_rows}

    missing_districts = {}
    for r in records:
        dc = (r.get("district_code") or r.get("District_Code"))
        if not dc or dc in district_map:
            continue
        sc = (r.get("state_code") or r.get("State_Code"))
        state_id = state_map.get(sc) if sc else None
        dn = r.get("district_name") or r.get("District") or ""
        if state_id:
            missing_districts[dc] = {"district_code": dc, "district_name": dn, "state_id": state_id}

    if missing_districts:
        try:
            stmt = insert(models.Districts).values(list(missing_districts.values()))
            stmt = stmt.on_conflict_do_nothing(index_elements=["district_code"])
            db.execute(stmt)
            db.commit()
        except Exception:
            db.rollback()
    # refresh district_map
    district_rows = db.query(models.Districts.district_code, models.Districts.id).all()
    district_map = {r.district_code: r.id for r in district_rows}

    # --- prepare deduplicated rows keyed by district_id ---
    prepared = {}
    for r in records:
        dc = (r.get("district_code") or r.get("District_Code"))
        if not dc:
            continue
        did = district_map.get(dc)
        if not did:
            continue
        row = prepared.get(did, {"district_id": did})
        if "timestamp" in {c.name for c in models.MGNREGAData.__table__.columns}:
            row["timestamp"] = datetime.utcnow()
        for raw_k, model_k in _FIELD_MAP.items():
            if raw_k in r:
                row[model_k] = _coerce_value(r.get(raw_k))
        for k, v in r.items():
            if k in {c.name for c in models.MGNREGAData.__table__.columns}:
                row[k] = _coerce_value(v)
        prepared[did] = row

    values = list(prepared.values())
    if not values:
        return {"message": "no valid rows to insert/update"}

    # Split into batches
    batches = [values[i : i + batch_size] for i in range(0, len(values), batch_size)]

    # Decide strategy based on whether table already has data
    try:
        existing_count = db.query(models.MGNREGAData).count()
    except Exception:
        existing_count = None

    rows_processed = 0
    if existing_count == 0:
        # simple insert (no ON CONFLICT needed) — values already deduped by district_id
        for batch in batches:
            try:
                db.execute(insert(models.MGNREGAData).values(batch))
                db.commit()
                rows_processed += len(batch)
            except Exception as e:
                db.rollback()
                # fallback: try per-row insert to get more specific errors and avoid whole-batch failure
                for v in batch:
                    try:
                        db.execute(insert(models.MGNREGAData).values(v))
                        db.commit()
                        rows_processed += 1
                    except Exception:
                        db.rollback()
        return {"message": f"inserted {rows_processed} rows (table was empty)"}
    else:
        # attempt upsert using ON CONFLICT DO UPDATE (ensure unique index exists)
        try:
            db.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_mgnrega_district_id ON mgnrega_data (district_id);")
        except Exception:
            pass

        # build list of updateable columns
        excluded_names = {"id", "district_id", "data_fetched_on"}
        mcols = [c.name for c in models.MGNREGAData.__table__.columns if c.name not in excluded_names]

        for batch in batches:
            insert_stmt = insert(models.MGNREGAData).values(batch)
            set_dict = {col: getattr(insert_stmt.excluded, col) for col in mcols}
            set_dict["updated_at"] = func.now()
            stmt = insert_stmt.on_conflict_do_update(index_elements=["district_id"], set_=set_dict)
            try:
                db.execute(stmt)
                db.commit()
                rows_processed += len(batch)
            except Exception:
                db.rollback()
                # fallback to per-row update/insert
                for v in batch:
                    try:
                        did = v["district_id"]
                        upd = v.copy()
                        upd.pop("district_id", None)
                        upd["updated_at"] = datetime.utcnow()
                        updated = db.query(models.MGNREGAData).filter_by(district_id=did).update(upd)
                        if not updated:
                            db.execute(insert(models.MGNREGAData).values(v))
                        db.commit()
                        rows_processed += 1
                    except Exception:
                        db.rollback()
        return {"message": f"upserted {rows_processed} rows (table not empty)"}


def save_raw_api_cache(db: Session, api_url: str, response_json: dict):
    try:
        cache_entry = models.APICache(
            api_url=api_url,
            response_data=response_json,
            timestamp=datetime.utcnow(),
        )
        db.add(cache_entry)
        db.commit()
        return {"status": "saved"}
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to save raw API cache: {e}")
        return {"status": "error"}