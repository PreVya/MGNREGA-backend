from pydantic import BaseModel
from typing import Optional
from datetime import datetime, date

class StateBase(BaseModel):
    state_name: str
    state_code: str

class StateCreate(StateBase):
    pass

class State(StateBase):
    id: int

    class Config:
        orm_mode = True

class DistrictBase(BaseModel):
    district_name: str
    district_code: str
    state_id: int

class DistrictCreate(DistrictBase):
    pass    

class District(DistrictBase):
    id: int

    class Config:
        orm_mode = True

class MGNREGADataBase(BaseModel):
    approved_labour_budget: int
    average_wage_rate_per_day_per_person: float
    differently_abled_persons_worked: int
    material_and_skilled_wages: float
    number_of_complted_projects: int
    number_of_gp_with_nil_exp: int
    number_of_ongoing_works: int
    persondays_of_central_liability_so_far: int
    sc_persondays: int
    sc_workers_against_Active_workers: int
    st_persondays: int
    st_workers_against_Active_workers: int
    total_adm_expenditure: float
    total_exp: float
    total_households_worked: int
    total_individuals_worked: int
    total_num_of_active_job_cards: int
    total_num_of_active_workers: int
    total_num_of_hh_completed_100_day_wage_employment: int
    total_num_of_job_cards_issued: int
    total_num_of_workers: int
    total_num_of_works_takenup: int
    wages: float
    women_persondays: int
    percent_of_category_B_works: float
    percentage_of_expenditure_on_agriculture_allied_works: float
    percent_of_NRM_expenditure: float
    percentage_payments_generated_within_15_days: float
    remarks: Optional[str] = None
    timestamp: date

class MGNREGADataCreate(MGNREGADataBase):
    pass

class MGNREGAData(MGNREGADataBase):
    id: int
    data_fetched_on: datetime
    updated_at: Optional[datetime] = None

    class Config:
        orm_mode = True


class RawAPICacheBase(BaseModel):
    api_url: str
    response_data: dict
    timestamp: datetime


class RawAPICacheCreate(RawAPICacheBase):
    pass


class RawAPICache(RawAPICacheBase):
    id: int

    class Config:
        orm_mode = True
