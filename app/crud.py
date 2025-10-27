from datetime import datetime
from sqlalchemy.orm import Session
from app import models
import logging

logger = logging.getLogger(__name__)

def insert_states(db: Session, records: list):
    """Insert or update states from API records."""
    added, skipped = 0, 0
    seen =set()

    for record in records:
        state_name = record.get("State_Name") or record.get("state_name")
        state_code = record.get("State_Code") or record.get("state_code")
        if not (state_name and state_code):
            continue
        seen.add((state_code, state_name))

    for state_code, state_name in seen:
        existing = db.query(models.States).filter_by(state_code=state_code).first()
        if not existing:
            db.add(models.States(state_code=state_code, state_name=state_name))
            added += 1
        else:
            skipped += 1

    db.commit()
    logger.info(f"✅ States processed: {added} added, {skipped} skipped.")
    return {"added": added, "skipped": skipped}


def insert_districts(db: Session, records: list):
    """Insert unique districts linked to their states."""
    added, skipped = 0, 0
    seen = set()

    # Build a unique set of (district_code, district_name, state_code)
    for record in records:
        state_code = record.get("State_Code") or record.get("state_code")
        district_code = record.get("District_Code") or record.get("district_code")
        district_name = record.get("District_Name") or record.get("district_name")

        if not (state_code and district_code and district_name):
            continue
        seen.add((district_code, district_name, state_code))

    for district_code, district_name, state_code in seen:
        state = db.query(models.States).filter_by(state_code=state_code).first()
        if not state:
            logger.warning(f"⚠️ Missing state {state_code} — skipping district {district_name}")
            continue

        existing = db.query(models.Districts).filter_by(district_code=district_code).first()
        if not existing:
            db.add(models.Districts(
                district_code=district_code,
                district_name=district_name,
                state_id=state.id
            ))
            added += 1
        else:
            skipped += 1

    db.commit()
    logger.info(f"✅ Districts processed: {added} added, {skipped} skipped.")
    return {"added": added, "skipped": skipped}



def insert_mgnrega_data(db: Session, records: list):
    """Insert unique MGNREGA data linked to districts."""
    processed, errors = 0, 0
    seen = set()
    new_entries = []

    for record in records:
        try:
            district_code = record.get("District_Code") or record.get("district_code")
            if not district_code:
                continue

            # Create a simple unique key to avoid inserting duplicates
            unique_key = (
                district_code,
                record.get("Total_Num_of_Job_Cards_Issued"),
                record.get("Total_Exp"),
            )
            if unique_key in seen:
                continue
            seen.add(unique_key)

            district = db.query(models.Districts).filter_by(district_code=district_code).first()
            if not district:
                logger.warning(f"⚠️ Missing district {district_code}")
                continue

            entry = models.MGNREGAData(
                approved_labour_budget=int(record.get("Approved_Labour_Budget") or 0),
                average_wage_rate_per_day_per_person=float(record.get("Average_Wage_rate_per_day_per_person") or 0),
                differently_abled_persons_worked=int(record.get("Differently_abled_persons_worked") or 0),
                material_and_skilled_wages=float(record.get("Material_and_Skilled_Wages") or 0),
                number_of_complted_projects=int(record.get("Number_of_Completed_Projects") or 0),
                number_of_gp_with_nil_exp=int(record.get("Number_of_GP_with_Nil_Exp") or 0),
                number_of_ongoing_works=int(record.get("Number_of_Ongoing_Works") or 0),
                persondays_of_central_liability_so_far=int(record.get("Persondays_of_Central_Liability_so_far") or 0),
                sc_persondays=int(record.get("SC_Persondays") or 0),
                sc_workers_against_Active_workers=int(record.get("SC_workers_against_Active_workers") or 0),
                st_persondays=int(record.get("ST_Persondays") or 0),
                st_workers_against_Active_workers=int(record.get("ST_workers_against_Active_workers") or 0),
                total_adm_expenditure=float(record.get("Total_Adm_Expenditure") or 0),
                total_exp=float(record.get("Total_Exp") or 0),
                total_households_worked=int(record.get("Total_Households_Worked") or 0),
                total_individuals_worked=int(record.get("Total_Individuals_Worked") or 0),
                total_num_of_active_job_cards=int(record.get("Total_Num_of_Active_Job_Cards") or 0),
                total_num_of_active_workers=int(record.get("Total_Num_of_Active_Workers") or 0),
                total_num_of_hh_completed_100_day_wage_employment=int(record.get("Total_Num_of_HH_Completed_100_Day_Wage_Employment") or 0),
                total_num_of_job_cards_issued=int(record.get("Total_Num_of_Job_Cards_Issued") or 0),
                total_num_of_workers=int(record.get("Total_Num_of_Workers") or 0),
                total_num_of_works_takenup=int(record.get("Total_No_of_Works_Takenup") or 0),
                wages=float(record.get("Wages") or 0),
                women_persondays=int(record.get("Women_Persondays") or 0),
                percent_of_category_B_works=float(record.get("Percent_of_Category_B_Works") or 0),
                percentage_of_expenditure_on_agriculture_allied_works=float(record.get("Percentage_of_Expenditure_on_Agriculture_Allied_Works") or 0),
                percent_of_NRM_expenditure=float(record.get("Percent_of_NRM_Expenditure") or 0),
                percentage_payments_generated_within_15_days=float(record.get("Percentage_of_Payments_Generated_within_15_days") or 0),
                remarks=record.get("Remarks") or "NA",
                timestamp=datetime.utcnow(),
                district_id=district.id
            )

            new_entries.append(entry)
            processed += 1

        except Exception as e:
            errors += 1
            logger.error(f"🔥 Error preparing MGNREGA record for {record.get('District_Name')}: {e}")

    try:
        db.bulk_save_objects(new_entries)
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"🔥 Bulk insert failed: {e}")

    logger.info(f"✅ MGNREGA data inserted: {processed} successful, {errors} errors.")
    return {"processed": processed, "errors": errors}



def save_raw_api_cache(db: Session, api_url: str, response_json: dict):
    """Store raw API response for debugging or auditing."""
    try:
        cache_entry = models.APICache(
            api_url=api_url,
            response_json=response_json,
            timestamp=datetime.utcnow()
        )
        db.add(cache_entry)
        db.commit()
        return {"status": "saved"}
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to save raw API cache: {e}")
        return {"status": "error"}






























# from datetime import datetime
# from redis import Redis
# from sqlalchemy.orm import Session
# from app import config
# import json
# from typing import Dict, Any, Optional, List
# from app.models import States, Districts, MGNREGAData, RawAPICache

# _redis_client = None
# def get_redis() -> Redis:
#     global _redis_client
#     if _redis_client is None:
#         _redis_client = Redis.from_url(config.REDIS_URL, decode_responses=True)
#     return _redis_client


# def _normalize_api_key(key: str) -> str:
#     """
#     Normalize API keys like "Approved_Labour_Budget" or "Average Wage rate per day per person"
#     to snake_case matching our SQLAlchemy fields (best-effort).
#     """
#     k = key.strip()
#     # replace spaces and non-alnum with underscore
#     k = "".join(ch if (ch.isalnum() or ch == '_') else '_' for ch in k)
#     # collapse multiple underscores
#     while "__" in k:
#         k = k.replace("__", "_")
#     return k.lower()


# _MANUAL_FIELD_MAP = {
#     "approved_labour_budget": "approved_labour_budget",
#     "average_wage_rate_per_day_per_person": "average_wage_rate_per_day_per_person",
#     "average_days_of_employment_provided_per_household": "average_days_of_employment_provided_per_household",
#     "differently_abled_persons_worked": "differently_abled_persons_worked",
#     "material_and_skilled_wages": "material_and_skilled_wages",
#     "number_of_completed_works": "number_of_completed_works",
#     "number_of_gps_with_nil_exp": "number_of_gps_with_nil_exp",
#     "number_of_ongoing_works": "number_of_ongoing_works",
#     "persondays_of_central_liability_so_far": "persondays_of_central_liability_so_far",
#     "sc_persondays": "sc_persondays",
#     "sc_workers_against_active_workers": "sc_workers_against_active_workers",
#     "st_persondays": "st_persondays",
#     "st_workers_against_active_workers": "st_workers_against_active_workers",
#     "total_adm_expenditure": "total_adm_expenditure",
#     "total_exp": "total_exp",
#     "total_households_worked": "total_households_worked",
#     "total_individuals_worked": "total_individuals_worked",
#     "total_no_of_active_job_cards": "total_no_of_active_job_cards",
#     "total_no_of_active_workers": "total_no_of_active_workers",
#     "total_no_of_hhs_completed_100_days_of_wage_employment": "total_no_of_hhs_completed_100_days_of_wage_employment",
#     "total_no_of_jobcards_issued": "total_no_of_jobcards_issued",
#     "total_no_of_workers": "total_no_of_workers",
#     "total_no_of_works_takenup": "total_no_of_works_takenup",
#     "wages": "wages",
#     "women_persondays": "women_persondays",
#     "percent_of_category_b_works": "percent_of_category_b_works",
#     "percent_of_expenditure_on_agriculture_allied_works": "percent_of_expenditure_on_agriculture_allied_works",
#     "percent_of_nrm_expenditure": "percent_of_nrm_expenditure",
#     "percentage_payments_generated_within_15_days": "percentage_payments_generated_within_15_days",
#     "remarks": "remarks",
#     "fin_year": "fin_year",
#     "month": "month",
#     "state_code": "state_code",
#     "state_name": "state_name",
#     "district_code": "district_code",
#     "district_name": "district_name",
# }

# def _map_record_keys(record: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     Convert a single API record keys to our model-friendly snake_case keys.
#     Does a best-effort mapping: normalize keys and use manual map when possible.
#     """
#     mapped = {}
#     for k, v in record.items():
#         norm = _normalize_api_key(k)
#         # prefer manual map if present
#         if norm in _MANUAL_FIELD_MAP:
#             mapped_field = _MANUAL_FIELD_MAP[norm]
#         else:
#             mapped_field = norm
#         mapped[mapped_field] = v
#     return mapped


# def get_or_create_state(db: Session, state_name: str, state_code: Optional[str] = None) -> States:
#     state = db.query(States).filter(States.state_name == state_name).first()
#     if state:
#         return state
#     state = States(state_name=state_name, state_code=state_code or "")
#     db.add(state)
#     db.commit()
#     db.refresh(state)
#     return state

# def get_or_create_district(db: Session, district_name: str, district_code: Optional[str], state_id: int) -> Districts:
#     """Return existing district row or create it."""
#     district = db.query(Districts).filter(Districts.district_code == district_code).first() \
#                or db.query(Districts).filter(Districts.district_name == district_name, Districts.state_id == state_id).first()
#     if district:
#         return district
#     district = Districts(district_name=district_name, district_code=district_code or "", state_id=state_id)
#     db.add(district)
#     db.commit()
#     db.refresh(district)
#     return district

# def get_district_stats(db: Session, district_code: str, fin_year: Optional[str] = None, month: Optional[str] = None, ttl_seconds: int = 43200) -> Dict[str, Any]:
#     r = get_redis()
#     key = _cache_key_for_district(district_code, fin_year, month)
#     cached = r.get(key)
#     if cached:
#         try:
#             return json.loads(cached)
#         except Exception:
#             r.delete(key)

#     # Cache miss → get from DB
#     query = db.query(MGNREGAData).filter(MGNREGAData.district_code == district_code)
#     if fin_year:
#         query = query.filter(MGNREGAData.fin_year == fin_year)
#     if month:
#         query = query.filter(MGNREGAData.month == month)

#     row = query.order_by(MGNREGAData.data_fetched_on.desc()).first()
#     if not row:
#         return {}

#     result = {c.name: getattr(row, c.name) for c in MGNREGAData.__table__.columns}
#     try:
#         r.setex(key, ttl_seconds, json.dumps(result, default=str))
#     except Exception:
#         pass

#     return result

# def save_raw_api_cache(db: Session, api_url: str, response_json: Dict[str, Any]) -> RawAPICache:
#     """Store raw API response in DB raw_api_cache table (for auditing / debugging)."""
#     try:
#         rec = RawAPICache(api_url=api_url, response_data=response_json, timestamp=datetime.utcnow())
#         db.add(rec)
#         db.commit()
#         db.refresh(rec)
#         return rec
#     except Exception:
#         db.rollback()
#         raise

# def create_or_update_mgnrega_record(db: Session, record: Dict[str, Any]) -> MGNREGAData:
#     mapped = _map_record_keys(record)

#     # --- ensure we have state + district inserted first ---
#     state_name = mapped.get("state_name")
#     state_code = mapped.get("state_code")
#     district_name = mapped.get("district_name")
#     district_code = mapped.get("district_code")

#     if not state_name or not district_name:
#         raise ValueError("record must contain state_name and district_name")

#     state = get_or_create_state(db, state_name=state_name, state_code=state_code)
#     district = get_or_create_district(db, district_name=district_name, district_code=district_code, state_id=state.id)


#     existing = None
#     try:
        
#         if hasattr(MGNREGAData, "fin_year") and hasattr(MGNREGAData, "month"):
#             fy = mapped.get("fin_year")
#             month = mapped.get("month")
#             if fy and month:
#                 existing = db.query(MGNREGAData).filter(
#                     getattr(MGNREGAData, "district_code") == district_code,
#                     getattr(MGNREGAData, "fin_year") == fy,
#                     getattr(MGNREGAData, "month") == month
#                 ).first()
#         # fallback: attempt to find by district id and exact persondays/total_exp + same timestamp (weak heuristic)
#         if existing is None:
#             existing = db.query(MGNREGAData).filter(
#                 getattr(MGNREGAData, "district_code", MGNREGAData).is_(district_code)
#             ).order_by(MGNREGAData.data_fetched_on.desc()).first()
#     except Exception:
#         # If filtering by attributes that don't exist errors, ignore and create new
#         existing = None

#     # --- create object payload by only writing existing columns ---
#     obj_kwargs = {}
#     model_cols = {c.name for c in MGNREGAData.__table__.columns}

#     # Always include link to district and state codes if columns exist
#     if "district_code" in model_cols:
#         obj_kwargs["district_code"] = district_code or ""
#     if "state_code" in model_cols:
#         obj_kwargs["state_code"] = state_code or ""
#     if "district_id" in model_cols:
#         obj_kwargs["district_id"] = district.id

#     # map values from API into obj_kwargs only for columns that exist
#     for k, v in mapped.items():
#         if k in model_cols:
#             # convert numeric-like strings to appropriate Python types where possible
#             try:
#                 col_type = next((col.type for col in MGNREGAData.__table__.columns if col.name == k), None)
#             except StopIteration:
#                 col_type = None

#             # basic conversion attempts
#             if v is None or (isinstance(v, str) and v.strip() == ""):
#                 obj_kwargs[k] = None
#             else:
#                 # try integer
#                 if isinstance(v, str):
#                     v_stripped = v.strip()
#                     # try int
#                     if v_stripped.isdigit():
#                         obj_kwargs[k] = int(v_stripped)
#                     else:
#                         try:
#                             # float conversion
#                             obj_kwargs[k] = float(v_stripped)
#                         except Exception:
#                             obj_kwargs[k] = v
#                 else:
#                     obj_kwargs[k] = v

#     # set timestamp if field exists
#     if "timestamp" in model_cols and "timestamp" not in obj_kwargs:
#         try:
#             obj_kwargs["timestamp"] = datetime.utcnow()
#         except Exception:
#             pass

#     # --- insert or update ---
#     if existing:
#         for key, value in obj_kwargs.items():
#             setattr(existing, key, value)
#         try:
#             db.add(existing)
#             db.commit()
#             db.refresh(existing)
#             return existing
#         except Exception:
#             db.rollback()
#             raise
#     else:
#         try:
#             new_obj = MGNREGAData(**obj_kwargs)
#             db.add(new_obj)
#             db.commit()
#             db.refresh(new_obj)
#             return new_obj
#         except Exception as e:
#             db.rollback()
#             raise


# def bulk_ingest_records(db: Session, api_url: str, records: List[Dict[str, Any]], save_raw: bool = True) -> Dict[str, int]:
#     """
#     Ingest a list of API records (the "records" array).
#     Optionally save the raw API response to DB (raw_api_cache).
#     Returns a summary dict: {'created': int, 'updated': int, 'errors': int}
#     """
#     created = 0
#     updated = 0
#     errors = 0

#     if save_raw:
#         try:
#             save_raw_api_cache(db, api_url=api_url, response_json={"records_count": len(records), "sample_first": records[0] if records else {}})
#         except Exception:
#             # don't fail ingestion if raw cache saving fails
#             pass

#     for rec in records:
#         try:
#             mapped = _map_record_keys(rec)
#             # only process records for target state (e.g., MAHARASHTRA) - you said filter state-wise
#             target_state_upper = (config.TARGET_STATE.upper() if getattr(config, "TARGET_STATE", None) else "MAHARASHTRA")
#             record_state_name = (mapped.get("state_name") or "").upper()
#             if record_state_name != target_state_upper:
#                 continue  # skip non-target-state rows

#             # create_or_update returns the row object
#             row = create_or_update_mgnrega_record(db, rec)
#             # increment created/updated counters heuristically
#             # If id existed before updating, we cannot easily detect updated vs created reliably here,
#             # so we use the presence of 'created' in SQLAlchemy new state — but keep it simple:
#             created += 1  # conservative: count attempts as created (or you can compute more precisely)
#         except Exception:
#             errors += 1

#     return {"processed": created, "errors": errors}

# def _cache_key_for_district(district_code: str, fin_year: Optional[str] = None, month: Optional[str] = None) -> str:
#     if fin_year and month:
#         return f"mgnrega:district:{district_code}:fy:{fin_year}:month:{month}"
#     elif fin_year:
#         return f"mgnrega:district:{district_code}:fy:{fin_year}"
#     else:
#         return f"mgnrega:district:{district_code}:latest"
    

# def list_districts_for_state(db: Session, state_name: str) -> List[Dict[str, Any]]:
#     state = db.query(States).filter(States.state_name == state_name).first()
#     if not state:
#         return []
#     rows = db.query(Districts).filter(Districts.state_id == state.id).order_by(Districts.district_name).all()
#     out = []
#     for d in rows:
#         out.append({"district_id": d.id, "district_code": d.district_code, "district_name": d.district_name})
#     return out