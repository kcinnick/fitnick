from sqlalchemy import MetaData, Table, Column, UniqueConstraint, Numeric, Date, Integer, String
from sqlalchemy.ext.declarative import declarative_base

meta = MetaData()
Base = declarative_base()


class Calories(Base):
    __tablename__ = 'calories'
    date = Column('date', Date, nullable=False, primary_key=True)
    total = Column('total', Integer())
    calories_bmr = Column('calories_bmr', Integer())
    activity_calories = Column('activity_calories', Integer())
    UniqueConstraint('date', name='date')
    schema = 'activity'

    def __eq__(self, other):
        return self.date, self.total, self.calories_bmr == other.date, other.total, other.calories_bmr

    def __str__(self):
        return f"{self.date}, {self.total}, {self.calories_bmr}, {self.activity_calories}"


calories_table = Table(
    'calories',
    meta,
    Column('date', Date),
    Column('total', Integer()),
    Column('calories_bmr', Numeric(10, 5)),
    Column('activity_calories', Numeric(10, 5)),
    UniqueConstraint('date', name='date'),
    schema='activity'
)


class CaloriesIntraday(Base):
    __tablename__ = 'intraday'
    date = Column('date', Date, nullable=False, primary_key=True)
    level = Column('level', Integer())
    mets = Column('mets', Integer())
    value = Column('value', Numeric())
    schema = 'calories'
