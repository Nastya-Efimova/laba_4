from sqlalchemy import *
from sqlalchemy.orm import *
from fastapi import FastAPI

app = FastAPI()

engine = create_engine("mysql://isp_p_Efimova:12345@77.91.86.135/isp_p_Efimova")

class Base(DeclarativeBase): pass

class Taker(Base):
    __tablename__ = 'taker'

    id = Column(Integer, primary_key=True, index=True)
    full_name = Column(String(100), nullable=False)
    address = Column(String(255), nullable=False)

    subscribes = relationship('Subscribe', back_populates='taker')

class Subscribe(Base):
    __tablename__ = 'subscribe'

    id = Column(Integer, primary_key=True, index=True)
    taker_id = Column(Integer, ForeignKey('taker.id'), nullable=False)
    producer_id = Column(Integer, ForeignKey('producer.id'), nullable=False)
    period = Column(Integer, nullable=False)
    start_date = Column(DateTime, nullable=False, server_default=func.now())

    taker = relationship('Taker', back_populates='subscribes')
    producer = relationship('Producer', back_populates='subscribes')

class Producer(Base):
    __tablename__ = 'producer'

    id = Column(Integer, primary_key=True, index=True)
    type_produce = Column(String(20), nullable=False)
    name = Column(String(50), nullable=False)
    cost = Column(Float(2), nullable=False)

    subscribes = relationship('Subscribe', back_populates='producer')

Base.metadata.create_all(engine)

@app.get('/subscriptions_count')
def get_subscriptions_count():
    with Session(autoflush=False, bind=engine) as db:
        result = db.query(Producer.id, Producer.name, func.count(Subscribe.id)).join(Subscribe).group_by(Producer.id).all()
        response = []
        for id, name, count in result:
            response.append({'producer_id': id, 'producer_name': name, 'subscriptions_count': count})
    return {'subscriptions_count': response}

@app.get('/producers')
def get_producers():
    with Session(autoflush=False, bind=engine) as db:
        producers = db.query(Producer).all()
        response = []
        for producer in producers:
            response.append({'id': producer.id, 'type_produce': producer.type_produce, 'name': producer.name, 'cost': producer.cost})
    return {'producers': response}

@app.get('/takers')
def get_takers():
    with Session(autoflush=False, bind=engine) as db:
        takers = db.query(Taker).all()
        response = []
        for taker in takers:
            response.append({'id': taker.id, 'full_name': taker.full_name, 'address': taker.address})
    return {'takers': response}

@app.get('/subscribes')
def get_subscribes():
    with Session(autoflush=False, bind=engine) as db:
        subscribes = db.query(Subscribe).all()
        response = []
        for subscribe in subscribes:
            response.append({'taker_id': subscribe.taker_id, 'producer_id': subscribe.producer_id, 'period': subscribe.period, 'date_start': subscribe.start_date})
    return {'subscribes': response}

@app.get('/takers_by_producer/{producer_id}')
def get_takers_by_producer(producer_id: int):
    with Session(autoflush=False, bind=engine) as db:
        takers = db.query(Taker).join(Subscribe).filter(Subscribe.producer_id == producer_id).all()
        response = []
        for taker in takers:
            response.append(taker)
    return {'takers': response}

@app.get('/producers_by_taker/{taker_id}')
def get_producers_by_taker(taker_id: int):
    with Session(autoflush=False, bind=engine) as db:
        producers = db.query(Producer).join(Subscribe).filter(Subscribe.taker_id == taker_id).all()
        response = []
        for producer in producers:
            response.append(producer)
    return {'producers': response}

@app.get('/subscription_counts')
def get_subscription_counts():
    with Session(autoflush=False, bind=engine) as db:
        counts = db.query(Producer.id, Producer.name, func.count(Subscribe.id).label('count')).join(Subscribe).group_by(Producer.id).all()
        response = []
        for count in counts:
            response.append({'producer_id': count.id, 'producer_name': count.name, 'count': count.count})
    return {'subscription_counts': response}

@app.get('/revenue_by_period/{start_date}/{end_date}')
def get_revenue_by_period(start_date: str, end_date: str):
    with Session(autoflush=False, bind=engine) as db:
        revenues = db.query(Producer.id, Producer.name, func.sum((Subscribe.period * Producer.cost)).label('revenue')).join(Subscribe).filter(Subscribe.start_date.between(start_date, end_date)).group_by(Producer.id).all()
        response = []
        for revenue in revenues:
            response.append({'producer_id': revenue.id, 'producer_name': revenue.name, 'revenue': revenue.revenue})
    return {'revenues': response}

@app.get('/takers_subscribed_to_magazines')
def get_takers_subscribed_to_magazines():
    with Session(autoflush=False, bind=engine) as db:
        takers = db.query(Taker).join(Subscribe).join(Producer).filter(Producer.type_produce == 'журнал').all()
        response = []
        for taker in takers:
            response.append(taker)
    return {'takers': response}

@app.get('/producers_without_subscribers')
def get_producers_without_subscribers():
    with Session(autoflush=False, bind=engine) as db:
        producers = db.query(Producer).filter(~Producer.subscribes.any()).all()
        response = []
        for producer in producers:
            response.append(producer)
    return {'producers': response}