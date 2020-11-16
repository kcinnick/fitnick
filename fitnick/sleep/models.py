from sqlalchemy import MetaData, Table, Column, VARCHAR, UniqueConstraint, Date
from sqlalchemy.ext.declarative import declarative_base

meta = MetaData()
Base = declarative_base()


class SleepSummary(Base):
    __tablename__ = 'summary'
    date = Column(Date, nullable=False, primary_key=True)
    duration = Column(VARCHAR)
    efficiency = Column(VARCHAR)
    end_time = Column(VARCHAR)
    minutes_asleep = Column(VARCHAR)
    minutes_awake = Column(VARCHAR)
    start_time = Column(VARCHAR)
    time_in_bed = Column(VARCHAR)
    log_id = Column(VARCHAR)
    UniqueConstraint('date', name='date')
    schema = 'sleep'

    def __eq__(self, other):
        return self.date, self.duration == other.date, other.duration

    def __str__(self):
        return f"{self.date}, Duration: {self.duration}\nScore:{self.efficiency}\n"


sleep_summary_table = Table(
    'summary',
    meta,
    Column('date', Date, nullable=False),
    Column('duration', VARCHAR),
    Column('efficiency', VARCHAR),
    Column('end_time', VARCHAR),
    Column('minutes_asleep', VARCHAR),
    Column('minutes_awake', VARCHAR),
    Column('start_time', VARCHAR),
    Column('time_in_bed', VARCHAR),
    Column('log_id', VARCHAR),
    UniqueConstraint('date', name='date'),
    schema='sleep'
)


class SleepLevel(Base):
    __tablename__ = 'level'
    date = Column(Date, nullable=False, primary_key=True)
    type_ = Column(VARCHAR)
    count_ = Column(VARCHAR)
    minutes = Column(VARCHAR)
    thirty_day_avg_minutes = Column(VARCHAR)
    UniqueConstraint('date', 'type', name='date_type')
    schema = 'sleep'

    def __str__(self):
        return f"{self.date}, {self.type_}, {self.count_}, {self.minutes}, {self.thirty_day_avg_minutes}"


sleep_level_table = Table(
    'level',
    meta,
    Column('date', Date, nullable=False),
    Column('type_', VARCHAR),
    Column('count_', VARCHAR),
    Column('minutes', VARCHAR),
    Column('thirty_day_avg_minutes', VARCHAR),
    UniqueConstraint('date', 'type_', name='date_type_'),
    schema='sleep'
)
