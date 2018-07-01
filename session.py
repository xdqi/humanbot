import mysql.connector
import sqlite3

from logging import getLogger

import config

logger = getLogger(__name__)
sqlite_version = sqlite3.sqlite_version
sqlite_version_info = sqlite3.sqlite_version_info


class FakeLock:
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class FakeConnection:
    def __init__(self, conn):
        self.conn = conn

        conn.cursor().execute("SET NAMES 'utf8mb4'")
        conn.commit()

    def cursor(self):
        c = self.conn.cursor(buffered=True)
        return FakeCursor(c)

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()


class FakeCursor:
    def __init__(self, cursor):
        self.cursor = cursor

    def process_query(self, query):
        if query.lower().startswith('insert or replace into'):
            query = query.replace('insert or replace into', 'replace into')

        elif query.startswith("select name from sqlite_master "
                              "where type='table' and name="):
            query = query.replace("select name from sqlite_master "
                                  "where type='table' and name=",
                                  'show tables like ')
        elif query.startswith('create table'):
            if 'integer' in query:  # sqlite integer has a dynamic length, we use bigint
                query = query.replace('integer', 'bigint')
            if 'primary key(md5_digest' in query:
                query = query.replace('primary key(md5_digest', 'primary key(md5_digest(16)')
            logger.error(query)
            if 'without rowid' in query:
                query = query.replace('without rowid', '')

        if '?' in query:
            query = query.replace('?', '%s')

        return query

    def execute(self, query: str, *args, **kwargs):
        query = self.process_query(query)
        self.cursor.execute(query, *args, **kwargs)
        return self

    def executemany(self, query: str, *args, **kwargs):
        query = self.process_query(query)
        self.cursor.executemany(query, *args, **kwargs)
        return self

    def fetchone(self):
        return self.cursor.fetchone()

    def close(self):
        self.cursor.close()


def connect(filename, check_same_thread):
    if filename == ':memory:':
        return sqlite3.connect(filename, check_same_thread=check_same_thread)
    conn = mysql.connector.connect(**config.MYSQL_CONFIG, database=config.MYSQL_SESSION_DB_PREFIX + filename[:-8])
    return FakeConnection(conn)


def monkey_patch_sqlite_session():
    import telethon.sessions.sqlite as sqlite
    import sys
    sqlite.Lock = FakeLock
    sqlite.RLock = FakeLock
    sqlite.sqlite3 = sys.modules[FakeLock.__module__]
