from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from app.config import DATABASE_URL

# Enable pool_pre_ping to detect and recycle closed/stale connections.
# Add TCP keepalive and pool tuning to reduce chance of remote SSL connection closures.
# Adjust pool_size, max_overflow and pool_recycle if your DB has specific idle timeouts.
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    pool_recycle=1800,  # recycle connections after 30 minutes
    connect_args={
        # enable TCP keepalives to prevent idle SSL connections being closed by network devices
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    },
)
SessionLocal = sessionmaker(autoflush=False,bind=engine,autocommit=False)
Base = declarative_base()