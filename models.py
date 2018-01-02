import traceback
from datetime import datetime, timezone
from logging import getLogger

from sqlalchemy import engine_from_config
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Text, BigInteger, String
from sqlalchemy.orm import sessionmaker, scoped_session

import config
from humanbot import send_message_to_administrators, find_link_to_join
from utils import get_now_timestamp

Base = declarative_base()
logger = getLogger(__name__)


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
    username = Column('name', String(60))
    first_name = Column('firstname', String(50))
    last_name = Column('lastname', String(50))
    lang_code = Column('lang', String(10))


class UsernameHistory(Base):
    __tablename__ = 'user_history'
    id = Column('id', Integer(), primary_key=True, autoincrement=True)
    uid = Column('uid', Integer(), index=True, nullable=False)
    username = Column('name', String(60))
    first_name = Column('firstname', String(50))
    last_name = Column('lastname', String(50))
    lang_code = Column('lang', String(10))
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


engine = engine_from_config(config.DB_CONFIG, echo=not config.PRODUCTION)

session_factory = sessionmaker(bind=engine)

Session = scoped_session(session_factory)

if __name__ == '__main__':
    Base.metadata.create_all(engine)


def insert_message(chat_id: int, user_id: int, msg: str, date: datetime):
    if not msg:  # Not text message
        return
    utc_timestamp = int(date.timestamp())

    for i in range(10):
        try:
            session = Session()
            chat = Chat(chat_id=chat_id, user_id=user_id, text=msg, date=utc_timestamp)
            session.add(chat)
            session.commit()
            break
        except:
            session.rollback()
            send_message_to_administrators('DB write {} failed:\n{}'.format(i, traceback.format_exc()))
    find_link_to_join(session, msg)
    session.close()
    Session.remove()


def insert_message_local_timezone(chat_id, user_id, msg, date: datetime):
    utc_date = date.replace(tzinfo=timezone.utc)
    insert_message(chat_id, user_id, msg, utc_date)


def update_user_real(user_id, first_name, last_name, username, lang_code):
    """
    Update user information to database

    :param user_id:
    :param first_name:
    :param last_name:
    :param username:
    :param lang_code: Optional
    :return:
    """
    print(user_id, first_name, last_name, username, lang_code)

    session = Session()
    user = session.query(User).filter(User.uid == user_id).one_or_none()
    if not user:  # new user
        user = User(uid=user_id,
                    first_name=first_name,
                    last_name=last_name,
                    username=username,
                    lang_code=lang_code)

        session.add(user)
    else:  # existing user
        same = user.first_name == first_name and user.last_name == last_name and user.username == username
        if not same:  # information changed
            user.first_name = first_name
            user.last_name = last_name
            user.username = username
            user.lang_code = lang_code
            change = UsernameHistory(uid=user_id,
                                     username=username,
                                     first_name=first_name,
                                     last_name=last_name,
                                     lang_code=lang_code,
                                     date=get_now_timestamp()
                                     )
            session.add(change)
    try:
        session.commit()
    except:  # PRIMARY KEY CONSTRAINT
        session.rollback()
    session.close()
    Session.remove()


def update_group_real(chat_id, name, link):
    """
    Update group information to database

    :param chat_id: Group ID (bot marked format)
    :param name: Group Name
    :param link: Group Public Username (supergroup only)
    :return:
    """
    print(chat_id, name, link)

    session = Session()
    group = session.query(Group).filter(Group.gid == chat_id).one_or_none()
    if not group:  # new group
        group = Group(gid=chat_id, name=name, link=link)
        session.add(group)
    else:  # existing group
        same = group.name == name and group.link == link
        if not same:  # information changed
            group.name = name
            group.link = link
            change = GroupHistory(gid=chat_id,
                                  name=name,
                                  link=link,
                                  date=get_now_timestamp()
                                  )
            session.add(change)
    try:
        session.commit()
    except:  # PRIMARY KEY CONSTRAINT
        session.rollback()
    session.close()
    Session.remove()
