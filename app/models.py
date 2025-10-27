from sqlalchemy import JSON, Column, Integer, String, ForeignKey, TIMESTAMP, BigInteger, Float, func
from app.database import Base

class States(Base):
    __tablename__ = 'states'
    id = Column(Integer, primary_key=True, index=True)
    state_name = Column(String, unique=True, index=True, nullable=False)
    state_code = Column(String, unique=True, index=True, nullable=False)

class Districts(Base):
    __tablename__ = 'districts'
    id = Column(Integer, primary_key=True, index=True)
    district_name = Column(String, unique=True, index=True, nullable=False)
    district_code = Column(String, unique=True, index=True, nullable=False)
    state_id = Column(Integer, ForeignKey('states.id',ondelete="CASCADE"), nullable=False)

class MGNREGAData(Base):
    __tablename__ = 'mgnrega_data'
    id = Column(Integer, primary_key=True, index=True)
    district_id = Column(Integer, ForeignKey('districts.id',ondelete="CASCADE"), nullable=False)
    approved_labour_budget = Column(BigInteger, nullable=False)
    average_wage_rate_per_day_per_person = Column(Float, nullable=False)
    differently_abled_persons_worked = Column(Integer, nullable=False)
    material_and_skilled_wages = Column(Float, nullable=False)
    number_of_complted_projects = Column(Integer, nullable=False)
    number_of_gp_with_nil_exp = Column(Integer, nullable=False)
    number_of_ongoing_works = Column(Integer, nullable=False)
    persondays_of_central_liability_so_far = Column(BigInteger, nullable=False)
    sc_persondays = Column(BigInteger, nullable=False)
    sc_workers_against_Active_workers = Column(BigInteger, nullable=False)
    st_persondays = Column(BigInteger, nullable=False)
    st_workers_against_Active_workers = Column(BigInteger, nullable=False)
    total_adm_expenditure = Column(Float, nullable=False)
    total_exp = Column(Float, nullable=False)
    total_households_worked = Column(BigInteger, nullable=False)
    total_individuals_worked = Column(BigInteger, nullable=False)
    total_num_of_active_job_cards = Column(BigInteger, nullable=False)
    total_num_of_active_workers = Column(BigInteger, nullable=False)
    total_num_of_hh_completed_100_day_wage_employment = Column(BigInteger, nullable=False)
    total_num_of_job_cards_issued = Column(BigInteger, nullable=False)
    total_num_of_workers = Column(BigInteger, nullable=False)
    total_num_of_works_takenup = Column(BigInteger, nullable=False)
    wages = Column(Float, nullable=False)
    women_persondays = Column(BigInteger, nullable=False)
    percent_of_category_B_works = Column(Float, nullable=False)
    percentage_of_expenditure_on_agriculture_allied_works = Column(Float, nullable=False)
    percent_of_NRM_expenditure = Column(Float, nullable=False)
    percentage_payments_generated_within_15_days = Column(Float, nullable=False)
    remarks = Column(String, nullable=True)
    timestamp = Column(TIMESTAMP, nullable=False)
    data_fetched_on = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), onupdate=func.now())

class RawAPICache(Base):
    __tablename__ = 'raw_api_cache'
    id = Column(Integer, primary_key=True, index=True)
    api_url = Column(String, nullable=False)
    response_data= Column(JSON, nullable=False)
    timestamp = Column(TIMESTAMP, nullable=False)