from sqlalchemy import MetaData, Table, Column, UniqueConstraint, Numeric, Date
from sqlalchemy.ext.declarative import declarative_base

meta = MetaData()
Base = declarative_base()


class WeightRecord(Base):
    __tablename__ = 'daily'
    date = Column(Date, nullable=False, primary_key=True)
    pounds = Column(Numeric(4, 1))
    UniqueConstraint('date', name='date')
    schema = 'weight'


weight_table = Table(
    'daily',
    meta,
    Column('date', Date, nullable=False, primary_key=True),
    Column('pounds', Numeric(4, 1)),
    UniqueConstraint('date', name='date'),
    schema='weight'
)
