import sqlite3
import mysql.connector
import sys

import config


def main():
    filename = sys.argv[1]
    mconn = mysql.connector.connect(**config.MYSQL_CONFIG, database=config.MYSQL_SESSION_DB_PREFIX + filename)
    lconn = sqlite3.connect(database=filename + '.session')
    # mconn.set_charset_collation('utf8mb4', 'utf8mb4_general_ci')

    lcur = lconn.cursor()
    mcur = mconn.cursor()
    mcur.execute("SET NAMES 'utf8mb4'")
    mconn.commit()

    processed = 0
    processing = []

    for row in lcur.execute('SELECT * FROM entities'):
        processing.append(row)
        processed += 1
        if processed % 1000 == 0:
            mcur.executemany('INSERT INTO entities VALUES (%s,%s,%s,%s,%s)', processing)
            mconn.commit()
            print('processed', processed, 'rows')
            processing = []

    if processing:
        mcur.executemany('INSERT INTO entities VALUES (%s,%s,%s,%s,%s)', processing)
        mconn.commit()
        print('processed', processed, 'rows')
        processing = []

    lcur.close()
    mcur.close()


if __name__ == '__main__':
    main()
