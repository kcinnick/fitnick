from sqlalchemy import MetaData, Table, Column, UniqueConstraint, Numeric, Date, Integer, String
from sqlalchemy.ext.declarative import declarative_base

meta = MetaData()
Base = declarative_base()


class BodyFatRecord(Base):
    __tablename__ = 'daily'
    date = Column('date', Date, nullable=False, primary_key=True)
    fat = Column('fat', Integer())
    logId = Column('logId', String())
    source = Column('source', String())
    time = Column('time', String())
    UniqueConstraint('date', name='date')
    schema = 'bodyfat'

    def __eq__(self, other):
        return self.date, self.fat, self.source == other.date, other.fat, other.source

    def __str__(self):
        return f"{self.date}, {self.fat}, {self.source}, {self.time}"


bodyfat_table = Table(
    'daily',
    meta,
    Column('date', Date, nullable=False, primary_key=True),
    Column('fat', Integer()),
    Column('logId', String()),
    Column('source', String()),
    Column('time', String()),
    UniqueConstraint('date', name='date'),
    schema='bodyfat'
)
