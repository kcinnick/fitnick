from datetime import datetime

from sqlalchemy import MetaData, Table, Column, VARCHAR, UniqueConstraint, Numeric, Date, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

meta = MetaData()
Base = declarative_base()


class SleepSummary(Base):
    __tablename__ = 'sleep_summary'
    date = Column(Date, nullable=False, primary_key=True)
    deep = Column(VARCHAR)
    light = Column(VARCHAR)
    rem = Column(VARCHAR)
    wake = Column(VARCHAR)
    total_minutes_asleep = Column(VARCHAR)
    total_time_in_bed = Column(VARCHAR)
    UniqueConstraint('date', name='date')
    schema = 'sleep'


sleep_summary_table = Table(
    'summary',
    meta,
    Column('date', Date, nullable=False),
    Column('deep', VARCHAR),
    Column('light', VARCHAR),
    Column('rem', VARCHAR),
    Column('wake', VARCHAR),
    Column('total_minutes_asleep', VARCHAR),
    Column('total_time_in_bed', VARCHAR),
    UniqueConstraint('date', name='date'),
    schema='sleep',
)
