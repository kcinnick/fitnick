from sqlalchemy import MetaData, Table, Column, UniqueConstraint, Numeric, Date
from sqlalchemy.ext.declarative import declarative_base

meta = MetaData()
Base = declarative_base()


class WeightRecord(Base):
    __tablename__ = 'weight'
    date = Column(Date, nullable=False, primary_key=True)
    pounds = Column(Numeric(3, 1))
    UniqueConstraint('date', name='date')
    schema = 'body'


weight_table = Table(
    'weight',
    meta,
    Column('date', Date, nullable=False, primary_key=True),
    Column('pounds', Numeric(3, 1)),
    UniqueConstraint('date', name='date'),
    schema='body'
)
