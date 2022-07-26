from sqlalchemy import create_engine, Integer, String, Column
from .config import SQLALCHEMY_DATABASE_URL
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

cnx = create_engine(SQLALCHEMY_DATABASE_URL)
db_session = sessionmaker()
db_session.configure(bind=cnx)
session = db_session()
Base = declarative_base(db_session)

class Follow_Up(Base):
    __tablename__ = "follow_up"
    key = Column(Integer(), primary_key=True, autoincrement=True)
    varClientKey = Column(Integer())
    varEnteredBy = Column(String(100))
    dateEntered = Column(String(50))
    txtDetails = Column(String(5000))
    varLoanNo = Column(String(25))
    delq_days = Column(String(20))