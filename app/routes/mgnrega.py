from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app import models

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
def get_all(db: Session = Depends(get_db)):
    """Return all data from states, districts, mgnrega_data and raw_api_cache as JSON."""
    # load base tables
    states = db.query(models.States).all()
    districts = db.query(models.Districts).all()
    mgnrega_rows = db.query(models.MGNREGAData).all()
    raw_cache = db.query(models.APICache).all()

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

    return {
        "states": states_out,
        "districts": districts_out,
        "mgnrega_data": mgnrega_out,
        "raw_api_cache": raw_cache_out,
    }
