from logging import getLogger
from sqlalchemy import engine_from_config
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Index, \
    Integer, BigInteger, SmallInteger, String, Text
from sqlalchemy.orm import sessionmaker, scoped_session

import config
import utils

Base = declarative_base()
logger = getLogger(__name__)


class Chat(Base):
    __tablename__ = 'chat'
    id = Column('id', Integer(), primary_key=True, autoincrement=True)
    chat_id = Column('chatid', BigInteger(), index=True)
    user_id = Column('userid', Integer(), index=True)
    text = Column('text', Text())
    date = Column('date', Integer(), index=True)


class ChatFlag:
    new = 0
    edited = 1
    deleted = 2


class ChatNew(Base):
    __tablename__ = 'chat_new'
    id = Column('id', Integer(), primary_key=True, autoincrement=True)
    chat_id = Column('chatid', BigInteger(), index=True)
    message_id = Column('messageid', Integer())
    user_id = Column('userid', Integer(), index=True)
    text = Column('text', Text())
    date = Column('time', Integer(), index=True)
    flag = Column('flag', SmallInteger(), index=True)
    __table_args__ = (
        Index('ix_chat_new_chatid_messageid', chat_id, message_id),
        Index('ix_chat_new_chatid_userid', chat_id, user_id),
        Index('ix_chat_new_chatid_flag', chat_id, flag),
        Index('ix_chat_new_userid_flag', user_id, flag),
    )


class User(Base):
    __tablename__ = 'users'
    uid = Column('uid', Integer(), primary_key=True, nullable=False)
    username = Column('name', String(32))
    first_name = Column('firstname', String(255))
    last_name = Column('lastname', String(255))
    lang_code = Column('lang', String(10))


class UsernameHistory(Base):
    __tablename__ = 'user_history'
    id = Column('id', Integer(), primary_key=True, autoincrement=True)
    uid = Column('uid', Integer(), index=True, nullable=False)
    username = Column('name', String(32))
    first_name = Column('firstname', String(255))
    last_name = Column('lastname', String(255))
    lang_code = Column('lang', String(10))
    date = Column('date', Integer(), index=True)


class Group(Base):
    __tablename__ = 'groups'
    gid = Column('id', BigInteger(), primary_key=True)
    name = Column('name', String(100))
    link = Column('link', String(50))
    master = Column('master', Integer(), index=True)


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
    logger.debug('User %s %s %s %s %s', user_id, first_name, last_name, username, lang_code)

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
            changed_before = session.query(UsernameHistory).filter(UsernameHistory.uid == user_id).count()
            if not changed_before:
                original = UsernameHistory(uid=user_id,
                                           username=user.username,
                                           first_name=user.first_name,
                                           last_name=user.last_name,
                                           lang_code=user.lang_code,
                                           date=0)
                session.add(original)
            user.first_name = first_name
            user.last_name = last_name
            user.username = username
            user.lang_code = lang_code
            change = UsernameHistory(uid=user_id,
                                     username=username,
                                     first_name=first_name,
                                     last_name=last_name,
                                     lang_code=lang_code,
                                     date=utils.get_now_timestamp()
                                     )
            session.add(change)
    try:
        session.commit()
    except:  # PRIMARY KEY CONSTRAINT
        session.rollback()
    session.close()
    Session.remove()


def update_group_real(master_uid, chat_id, name, link):
    """
    Update group information to database

    :param master_uid: Client/Bot User ID (bot marked format)
    :param chat_id: Group ID (bot marked format)
    :param name: Group Name
    :param link: Group Public Username (supergroup only)
    :return:
    """
    logger.debug('group %s %s %s', chat_id, name, link)

    session = Session()
    group = session.query(Group).filter(Group.gid == chat_id).one_or_none()
    if not group:  # new group
        group = Group(gid=chat_id, name=name, link=link, master=master_uid)
        session.add(group)
    else:  # existing group
        same = group.name == name and group.link == link
        if group.master is None:
            group.master = master_uid
        if not same:  # information changed
            changed_before = session.query(GroupHistory).filter(GroupHistory.gid == chat_id).count()
            if not changed_before:
                original = GroupHistory(gid=chat_id,
                                        name=group.name,
                                        link=group.link,
                                        date=0)
                session.add(original)
            group.name = name
            group.link = link
            change = GroupHistory(gid=chat_id,
                                  name=name,
                                  link=link,
                                  date=utils.get_now_timestamp()
                                  )
            session.add(change)
    try:
        session.commit()
    except:  # PRIMARY KEY CONSTRAINT
        session.rollback()
    session.close()
    Session.remove()


if __name__ == '__main__':
    Base.metadata.create_all(engine)
