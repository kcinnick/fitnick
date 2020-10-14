from sqlalchemy import MetaData, Table, Column, UniqueConstraint, Numeric, Date, Integer, String
from sqlalchemy.ext.declarative import declarative_base

meta = MetaData()
Base = declarative_base()


class Calories(Base):
    __tablename__ = 'daily'
    date = Column('date', Date, nullable=False, primary_key=True)
    value = Column('value', Integer())
    UniqueConstraint('date', name='date')
    schema = 'calories'


class CaloriesIntraday(Base):
    __tablename__ = 'intraday'
    date = Column('date', Date, nullable=False, primary_key=True)
    level = Column('level', Integer())
    mets = Column('mets', Integer())
    value = Column('value', Numeric())
    schema = 'calories'
