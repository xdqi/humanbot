from sqlalchemy import engine_from_config
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Text, BigInteger, String
from sqlalchemy.orm import sessionmaker, scoped_session

import config

Base = declarative_base()


class Chat(Base):
    __tablename__ = 'chat'
    id = Column('id', Integer(), primary_key=True, autoincrement=True)
    chat_id = Column('chatid', BigInteger(), index=True)
    user_id = Column('userid', Integer(), index=True)
    text = Column('text', Text())
    date = Column('date', Integer(), index=True)


class User(Base):
    __tablename__ = 'users'
    uid = Column('uid', Integer(), primary_key=True, nullable=False)
    name = Column('name', String(60))
    firstname = Column('firstname', String(16))
    lastname = Column('lastname', String(16))


class UsernameHistory(Base):
    __tablename__ = 'user_history'
    id = Column('id', Integer(), primary_key=True, autoincrement=True)
    uid = Column('uid', Integer(), index=True, nullable=False)
    name = Column('name', String(60))
    firstname = Column('firstname', String(16))
    lastname = Column('lastname', String(16))
    date = Column('date', Integer(), index=True)


class Group(Base):
    __tablename__ = 'groups'
    gid = Column('id', BigInteger(), primary_key=True)
    name = Column('name', String(100))
    link = Column('link', String(50))


class GroupHistory(Base):
    __tablename__ = 'group_history'
    id = Column('id', Integer(), primary_key=True, autoincrement=True)
    gid = Column('gid', BigInteger(), index=True)
    name = Column('name', String(100))
    link = Column('link', String(50))
    date = Column('date', Integer, index=True)


engine = engine_from_config(config.DB_CONFIG, echo=True)

session_factory = sessionmaker(bind=engine)

Session = scoped_session(session_factory)

if __name__ == '__main__':
    Base.metadata.create_all(engine)
