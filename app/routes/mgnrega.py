from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from app.database import SessionLocal
from app import models
import time
import logging

logger = logging.getLogger("mgnrega")
logger.setLevel(logging.INFO)

router = APIRouter(prefix="/mgnrega", tags=["mgnrega"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _serialize(obj):
    return {c.name: getattr(obj, c.name) for c in obj.__table__.columns}


@router.get("/all")
def get_all(db: Session = Depends(get_db), limit: int | None = Query(1000, ge=0), debug: bool = False):
    """Return all data (or a limited sample) and KPIs.

    - By default this endpoint returns at most `limit` rows from large tables to avoid loading huge result sets.
    - Pass `limit=0` to disable limiting (use with caution).
    - Set `debug=true` to include per-step timings in the response.
    """
    timings = {}
    total_start = time.time()

    # load base tables with timing
    t = time.time()
    states = db.query(models.States).all()
    timings["states_query_s"] = time.time() - t
    logger.info("/mgnrega/all - loaded states (%d rows) in %.3fs", len(states), timings["states_query_s"])

    t = time.time()
    districts = db.query(models.Districts).all()
    timings["districts_query_s"] = time.time() - t
    logger.info("/mgnrega/all - loaded districts (%d rows) in %.3fs", len(districts), timings["districts_query_s"])

    # respect limit to avoid loading massive data; limit==0 => no limit
    t = time.time()
    q = db.query(models.MGNREGAData)
    if limit and limit > 0:
        q = q.limit(limit)
    mgnrega_rows = q.all()
    timings["mgnrega_query_s"] = time.time() - t
    logger.info("/mgnrega/all - loaded mgnrega_rows (%d rows, limit=%s) in %.3fs", len(mgnrega_rows), str(limit), timings["mgnrega_query_s"])

    t = time.time()
    q = db.query(models.APICache)
    if limit and limit > 0:
        q = q.limit(limit)
    raw_cache = q.all()
    timings["raw_cache_query_s"] = time.time() - t
    logger.info("/mgnrega/all - loaded raw_cache (%d rows, limit=%s) in %.3fs", len(raw_cache), str(limit), timings["raw_cache_query_s"]) 

    # build lookup maps
    state_by_id = {s.id: s for s in states}
    district_by_id = {d.id: d for d in districts}

    states_out = [_serialize(s) for s in states]
    districts_out = [_serialize(d) for d in districts]

    mgnrega_out = []
    for r in mgnrega_rows:
        rec = _serialize(r)
        # attach district and state details when available
        d = district_by_id.get(r.district_id)
        if d:
            rec["district_name"] = d.district_name
            rec["district_code"] = d.district_code
            s = state_by_id.get(d.state_id)
            if s:
                rec["state_name"] = s.state_name
                rec["state_code"] = s.state_code
        mgnrega_out.append(rec)

    raw_cache_out = [_serialize(c) for c in raw_cache]

    # -------------------------
    # KPI calculations (backend)
    # -------------------------
    t = time.time()
    overall = {}
    overall["total_states"] = db.query(func.count(models.States.id)).scalar() or 0
    overall["total_districts"] = db.query(func.count(models.Districts.id)).scalar() or 0
    overall["mgnrega_records"] = db.query(func.count(models.MGNREGAData.id)).scalar() or 0

    overall["total_approved_labour_budget"] = (
        db.query(func.coalesce(func.sum(models.MGNREGAData.approved_labour_budget), 0)).scalar() or 0
    )
    overall["total_expenditure"] = (
        db.query(func.coalesce(func.sum(models.MGNREGAData.total_exp), 0)).scalar() or 0
    )
    overall["average_wage_rate"] = (
        db.query(func.coalesce(func.avg(models.MGNREGAData.average_wage_rate_per_day_per_person), 0)).scalar() or 0
    )
    overall["average_percentage_payments_within_15_days"] = (
        db.query(func.coalesce(func.avg(models.MGNREGAData.percentage_payments_generated_within_15_days), 0)).scalar() or 0
    )
    overall["total_persondays"] = (
        db.query(func.coalesce(func.sum(models.MGNREGAData.persondays_of_central_liability_so_far), 0)).scalar() or 0
    )

    try:
        if overall["total_approved_labour_budget"]:
            overall["percent_utilization"] = (
                float(overall["total_expenditure"]) / float(overall["total_approved_labour_budget"]) * 100
            )
        else:
            overall["percent_utilization"] = None
    except Exception:
        overall["percent_utilization"] = None

    timings["kpis_query_s"] = time.time() - t
    logger.info("/mgnrega/all - computed overall KPIs in %.3fs", timings["kpis_query_s"]) 

    # Per-state aggregates (group by state)
    t = time.time()
    per_state = []
    q = (
        db.query(
            models.States.id.label("state_id"),
            models.States.state_name.label("state_name"),
            models.States.state_code.label("state_code"),
            func.count(models.Districts.id).label("district_count"),
            func.coalesce(func.sum(models.MGNREGAData.approved_labour_budget), 0).label("approved_labour_budget"),
            func.coalesce(func.sum(models.MGNREGAData.total_exp), 0).label("total_expenditure"),
            func.coalesce(func.avg(models.MGNREGAData.average_wage_rate_per_day_per_person), 0).label("avg_wage_rate"),
            func.coalesce(func.avg(models.MGNREGAData.percentage_payments_generated_within_15_days), 0).label("avg_pct_payments_15_days"),
            func.coalesce(func.sum(models.MGNREGAData.persondays_of_central_liability_so_far), 0).label("total_persondays"),
        )
        .join(models.Districts, models.Districts.state_id == models.States.id, isouter=True)
        .join(models.MGNREGAData, models.MGNREGAData.district_id == models.Districts.id, isouter=True)
        .group_by(models.States.id)
    )

    for row in q.all():
        state_rec = {
            "state_id": row.state_id,
            "state_name": row.state_name,
            "state_code": row.state_code,
            "district_count": int(row.district_count or 0),
            "approved_labour_budget": int(row.approved_labour_budget or 0),
            "total_expenditure": float(row.total_expenditure or 0),
            "avg_wage_rate": float(row.avg_wage_rate or 0),
            "avg_percentage_payments_within_15_days": float(row.avg_pct_payments_15_days or 0),
            "total_persondays": int(row.total_persondays or 0),
        }
        try:
            if state_rec["approved_labour_budget"]:
                state_rec["percent_utilization"] = (
                    float(state_rec["total_expenditure"]) / float(state_rec["approved_labour_budget"]) * 100
                )
            else:
                state_rec["percent_utilization"] = None
        except Exception:
            state_rec["percent_utilization"] = None
        per_state.append(state_rec)

    timings["per_state_query_s"] = time.time() - t
    logger.info("/mgnrega/all - computed per-state KPIs in %.3fs", timings["per_state_query_s"]) 

    total_elapsed = time.time() - total_start
    timings["total_request_s"] = total_elapsed
    logger.info("/mgnrega/all - total request time: %.3fs", total_elapsed)

    response = {
        "states": states_out,
        "districts": districts_out,
        "mgnrega_data": mgnrega_out,
        "raw_api_cache": raw_cache_out,
        "kpis": {"overall": overall, "by_state": per_state},
    }

    if debug:
        response["_timings"] = timings

    return response


@router.get("/health")
def health_check():
    """Lightweight health endpoint for quick liveness checks."""
    return {"status": "ok"}
