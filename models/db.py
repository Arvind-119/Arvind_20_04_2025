from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

# Create SQLite engine
engine = create_engine('sqlite:///store_monitoring.db')
Session = sessionmaker(bind=engine)

class StoreStatus(Base):
    __tablename__ = 'store_status'
    
    id = Column(Integer, primary_key=True)
    store_id = Column(String, index=True)
    timestamp_utc = Column(DateTime, index=True)
    status = Column(String)  # 'active' or 'inactive'

class BusinessHours(Base):
    __tablename__ = 'business_hours'
    
    id = Column(Integer, primary_key=True)
    store_id = Column(String, index=True)
    day_of_week = Column(Integer)  # 0 = Monday, 6 = Sunday
    start_time_local = Column(String)
    end_time_local = Column(String)

class StoreTimezone(Base):
    __tablename__ = 'store_timezones'
    
    id = Column(Integer, primary_key=True)
    store_id = Column(String, unique=True, index=True)
    timezone_str = Column(String)

def init_db():
    Base.metadata.create_all(engine) 