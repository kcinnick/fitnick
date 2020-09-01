from sqlalchemy import MetaData, Table, Column, VARCHAR, UniqueConstraint, Numeric, Date


meta = MetaData()
heart_daily_table = Table(
    'daily',
    meta,
    Column('type', VARCHAR, primary_key=True),
    Column('minutes', Numeric(10, 5)),
    Column('date', Date, nullable=False),
    Column('calories', Numeric(10, 5)),
    UniqueConstraint('type', 'minutes', 'date', 'calories', name='daily_type_minutes_date_calories'),
    schema='heart'
)

heart_daterange_table = Table(
    'daterange',
    meta,
    Column('type', VARCHAR, primary_key=True),
    Column('base_date', Date, nullable=False),
    Column('end_date', Date, nullable=False),
    Column('minutes', Numeric(10, 5)),
    Column('calories', Numeric(10, 5)),
    UniqueConstraint('base_date', 'end_date', 'type', name='daterange_base_date_end_date_type_key'),
    schema='heart'
)

