from datetime import datetime

from sqlalchemy import MetaData, Table, Column, VARCHAR, UniqueConstraint, Numeric, Date, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

meta = MetaData()
Base = declarative_base()


class HeartDaily(Base):
    __tablename__ = 'daily'
    type = Column(VARCHAR, primary_key=True)
    minutes = Column(Numeric(10, 5))
    date = Column(Date, nullable=False)
    calories = Column(Numeric(10, 5))
    resting_heart_rate = Column(Integer)
    UniqueConstraint('type', 'minutes', 'date', 'calories', name='daily_type_minutes_date_calories')
    schema='heart'


heart_daily_table = Table(
    'daily',
    meta,
    Column('type', VARCHAR, primary_key=True),
    Column('minutes', Numeric(10, 5)),
    Column('date', Date, nullable=False),
    Column('calories', Numeric(10, 5)),
    Column('resting_heart_rate', Integer()),
    UniqueConstraint('type', 'minutes', 'date', 'calories', name='daily_type_minutes_date_calories'),
    schema='heart',
)
