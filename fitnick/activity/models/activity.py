from sqlalchemy import MetaData, Table, Column, VARCHAR, UniqueConstraint, Numeric, Date, Integer
from sqlalchemy.ext.declarative import declarative_base

meta = MetaData()
Base = declarative_base()


class ActivityLogRecord(Base):
    __tablename__ = 'log'
    activity_id = Column(Integer)
    activity_name = Column(VARCHAR)
    log_id = Column(VARCHAR, primary_key=True)
    calories = Column(Numeric(10, 5))
    distance = Column(Numeric(10, 5))
    duration = Column(Integer)  # in milliseconds - needs to be converted
    duration_minutes = Column(Numeric(10, 5))
    start_date = Column(Date)
    start_time = Column(VARCHAR)
    steps = Column(Integer)

    UniqueConstraint('log_id', name='log_id')
    schema = 'activity'

    def __eq__(self, other):
        return self.log_id == other.log_id

    def __str__(self):
        return f"{self.log_id}, {self.activity_name}, {self.calories}," \
               f" {self.start_date} {self.start_time}, {self.steps}"


activity_log_table = Table(
    'log',
    meta,
    Column('activity_id', Integer()),
    Column('activity_name', VARCHAR),
    Column('log_id', VARCHAR),
    Column('calories', Numeric(10, 5)),
    Column('distance', Numeric(10, 5)),
    Column('duration', Integer()),
    Column('duration_minutes', Numeric(10, 5)),
    Column('start_date', Date),
    Column('start_time', VARCHAR),
    Column('steps', Integer()),
    UniqueConstraint('log_id', name='log_id'),
    schema='activity'
)


steps_intraday_table = Table(
    'steps_intraday',
    meta,
    Column('date', VARCHAR, nullable=False, primary_key=True),
    Column('time', VARCHAR, primary_key=True),
    Column('steps', Integer()),
    UniqueConstraint('date', 'time', name='date_time'),
    schema='activity',
)
