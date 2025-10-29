import traceback
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app import crud
from app.config import TARGET_STATE, FIN_YEAR, MGNREGA_API_URL
import requests
import json


def fetch_mgnrega_data():
    print("Running scheduled MGNREGA data fetch job...")

    db: Session = SessionLocal()
    try:
        params = {
            "filters[state_name]": TARGET_STATE,
            "filters[fin_year]": FIN_YEAR,
            "limit": 1000
        }

        response = requests.get(MGNREGA_API_URL, params=params, timeout=120)
        response.raise_for_status()

        try:
            data = response.json()
        except json.JSONDecodeError:
            print("API did not return valid JSON data, skipping...")
            return

        records = data.get("records") if isinstance(data, dict) else data
        if not records:
            print(f"No records returned for {TARGET_STATE}")
            return

        print(f"{len(records)} records fetched for {TARGET_STATE} ({FIN_YEAR})")


        state_summary = crud.upsert_states(db, records)
        print(f"States Summary: {state_summary}")

        district_summary = crud.upsert_districts(db, records)
        print(f"Districts Summary: {district_summary}")

        mgnrega_summary = crud.upsert_mgnrega_data(db, records)
        print(f"MGNREGA Summary: {mgnrega_summary}")

        crud.save_raw_api_cache(db, api_url=MGNREGA_API_URL, response_json=data)

        print(f"Data pipeline completed successfully for {TARGET_STATE} ({FIN_YEAR})")

    except requests.RequestException as e:
        print(f"Network error fetching MGNREGA data: {e}")
    except Exception as e:
        print(f"Error processing MGNREGA data: {e}")
        traceback.print_exc()
    finally:
        db.close()

def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(fetch_mgnrega_data, "interval", hours=24)
    scheduler.start()
    fetch_mgnrega_data()
    print("Scheduler started — fetching MGNREGA data every 24 hours.")
