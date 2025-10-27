from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL")
MGNREGA_API_URL = os.getenv("MGNREGA_API_URL")
TARGET_STATE = "MAHARASHTRA"
FIN_YEAR = "2024-2025"