# data_sqlite.py
import sqlite3
import pandas as pd
from datetime import datetime

DB_FILE = "pnl.db"  # arquivo que você vai criar e commitar

def dbConn():
    conn = sqlite3.connect(DB_FILE, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    # permite uso de nomes de coluna em pandas read_sql
    conn.row_factory = sqlite3.Row
    return conn

def dbClose(conn):
    if conn:
        conn.close()

def dbCreateTable():
    conn = dbConn(); cur = conn.cursor()
    tradeTb = """
        CREATE TABLE IF NOT EXISTS tradeTb(
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            prod    TEXT NOT NULL,
            cat     TEXT NOT NULL,
            ship    TEXT NOT NULL,
            year    INTEGER NOT NULL,
            op      TEXT NOT NULL,
            ton     INTEGER NOT NULL,
            lvl     REAL NOT NULL,
            notion  REAL NOT NULL,
            date    DATE DEFAULT (date('now')),
            reg     TIMESTAMP DEFAULT (datetime('now'))
        );
    """
    mtmtb = """
        CREATE TABLE IF NOT EXISTS mtmtb(
            idPnl   INTEGER PRIMARY KEY AUTOINCREMENT,
            idTrade INTEGER NOT NULL,
            prod    TEXT NOT NULL,
            cat     TEXT NOT NULL,
            ship    TEXT NOT NULL,
            year    INTEGER NOT NULL,
            mtm     REAL NOT NULL,
            pnl     REAL NOT NULL,
            date    DATE DEFAULT (date('now')),
            reg     TIMESTAMP DEFAULT (datetime('now'))
        );
    """
    posTb = """
        CREATE TABLE IF NOT EXISTS posTb(
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            prod    TEXT NOT NULL,
            cat     TEXT NOT NULL,
            ship    TEXT NOT NULL,
            year    INTEGER NOT NULL,
            pos     INTEGER NOT NULL,
            date    DATE DEFAULT (date('now')),
            reg     TIMESTAMP DEFAULT (datetime('now'))
        );
    """
    pnltb = """
        CREATE TABLE IF NOT EXISTS pnltb(
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            prod    TEXT NOT NULL,
            cat     TEXT NOT NULL,
            ship    TEXT NOT NULL,
            year    INTEGER NOT NULL,
            pnl     REAL NOT NULL,
            date    DATE DEFAULT (date('now')),
            reg     TIMESTAMP DEFAULT (datetime('now'))
        );
    """
    cur.execute(tradeTb)
    cur.execute(mtmtb)
    cur.execute(posTb)
    cur.execute(pnltb)
    conn.commit()
    dbClose(conn)

# Inserts
def dbInsertTrade(product, category, shipment, year, operation, ton, lvl, notion):
    conn = dbConn(); cur = conn.cursor()
    query = """
        INSERT INTO tradeTb(prod, cat, ship, year, op, ton, lvl, notion)
        VALUES (?,?,?,?,?,?,?,?);
    """
    cur.execute(query, (product, category, shipment, year, operation, int(ton), float(lvl), float(notion)))
    conn.commit()
    dbClose(conn)

def dbInsertPnl(id_trade, product, category, shipment, year, mtm, pnl):
    conn = dbConn(); cur = conn.cursor()
    query = """
        INSERT INTO mtmtb(idTrade, prod, cat, ship, year, mtm, pnl)
        VALUES (?,?,?,?,?,?,?);
    """
    cur.execute(query, (id_trade, product, category, shipment, year, float(mtm), float(pnl)))
    conn.commit()
    dbClose(conn)

def dbInsertPos(product, category, shipment, year, position):
    conn = dbConn(); cur = conn.cursor()
    query = """
        INSERT INTO posTb(prod, cat, ship, year, pos)
        VALUES (?,?,?,?,?);
    """
    cur.execute(query, (product, category, shipment, year, int(position)))
    conn.commit()
    dbClose(conn)

# Fetch
def dbFetchMtM(id_trade):
    conn = dbConn(); cur = conn.cursor()
    query = """
        SELECT mtm, reg FROM mtmtb
        WHERE idTrade=? ORDER BY reg DESC LIMIT 1;
    """
    cur.execute(query, (id_trade,))
    row = cur.fetchone()
    dbClose(conn)
    return None if row is None else row[0]

def dbFetchPnl(prod, cat, ship, year):
    conn = dbConn(); cur = conn.cursor()
    query = """
        SELECT mtm FROM mtmtb
        WHERE prod=? AND cat=? AND ship=? AND year=?
        ORDER BY reg DESC LIMIT 1;
    """
    cur.execute(query, (prod, cat, ship, year))
    row = cur.fetchone()
    dbClose(conn)
    return None if row is None else row[0]

def dbFetchPos(prod, cat, ship, year):
    conn = dbConn(); cur = conn.cursor()
    query = """
        SELECT pos FROM posTb
        WHERE prod=? AND cat=? AND ship=? AND year=?
        ORDER BY reg DESC LIMIT 1;
    """
    cur.execute(query, (prod, cat, ship, year))
    row = cur.fetchone()
    dbClose(conn)
    return row[0] if row else 0

def dbFetchTrade(prod, cat, ship, year):
    conn = dbConn(); cur = conn.cursor()
    query = """
        SELECT id, op, ton, lvl
        FROM tradeTb
        WHERE prod=? AND cat=? AND ship=? AND year=?
    """
    cur.execute(query, (prod, cat, ship, year))
    rows = cur.fetchall()
    dbClose(conn)
    # sqlite returns tuples; keep same shape as original
    return rows

# DataFrame loaders (using pandas.read_sql)
def dbLoadPnl(prod, year):
    conn = dbConn()
    query = """
        SELECT *
        FROM mtmtb
        WHERE prod LIKE ? AND year = ?
        ORDER BY reg DESC;
    """
    df = pd.read_sql_query(query, conn, params=(prod, year))
    dbClose(conn)

    orderCol = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    orderRow = ['FOB Vessel','FOB Paper','C&F Vessel']

    df['date'] = pd.to_datetime(df['date'])
    df['month'] = df['date'].dt.month_name().str[:3]

    grouped = df.groupby(['cat','month'])['pnl'].sum().reset_index()

    table = grouped.pivot(index='cat', columns='month', values='pnl') \
            .reindex(columns=orderCol, fill_value=0) \
            .reindex(orderRow, fill_value=0) \
            .fillna(0)

    table['Year'] = table.sum(axis=1)
    total_row = table.sum(axis=0).to_frame().T
    total_row.index = ['Total']
    return pd.concat([table, total_row])

def dbLoadPos(prod, year):
    conn = dbConn()
    query = """
        SELECT pos, cat, ship, year, reg
        FROM posTb
        WHERE prod LIKE ? AND year = ?
        ORDER BY reg DESC;
    """
    df = pd.read_sql_query(query, conn, params=(prod, year))
    dbClose(conn)

    orderCol = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    orderRow = ['FOB Vessel','FOB Paper','C&F Vessel']

    df_unique = df.sort_values('reg', ascending=False).drop_duplicates(subset=['cat','ship'], keep='first')

    table = df_unique.pivot(index='cat', columns='ship', values='pos') \
            .reindex(columns=orderCol, fill_value=0) \
            .reindex(orderRow, fill_value=0) \
            .fillna(0)

    table['Year'] = table.sum(axis=1)
    total_row = table.sum(axis=0).to_frame().T
    total_row.index = ['Total']
    return pd.concat([table, total_row])

def dbLoadMtm(prod, year):
    conn = dbConn()
    query = """
        SELECT mtm, cat, ship, year, reg
        FROM mtmtb
        WHERE prod LIKE ? AND year = ?
        ORDER BY reg DESC;
    """
    df = pd.read_sql_query(query, conn, params=(prod, year))
    dbClose(conn)

    orderCol = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    orderRow = ['FOB Vessel','FOB Paper','C&F Vessel']

    df_unique = df.sort_values('reg', ascending=False).drop_duplicates(subset=['cat','ship'], keep='first')

    table = df_unique.pivot(index='cat', columns='ship', values='mtm') \
            .reindex(columns=orderCol, fill_value=0) \
            .reindex(orderRow, fill_value=0)

    return table

def dbLoadTrade():
    conn = dbConn()
    query = "SELECT * FROM tradeTb"
    df = pd.read_sql_query(query, conn)
    dbClose(conn)
    return df

def dbLoadGraphPnl(prod, table='mtmtb'):
    conn = dbConn()
    query = f"""
        WITH latest AS (
          SELECT idPnl, pnl, cat, date, reg
          FROM {table}
          WHERE prod LIKE ?
          ORDER BY cat, date, reg DESC
        )
        SELECT pnl, cat, date
        FROM {table}
        WHERE prod LIKE ?
        ORDER BY date;
    """
    # simpler: fallback to pulling all rows and sorting in pandas
    df = pd.read_sql_query(f"SELECT pnl, cat, date FROM {table} WHERE prod LIKE ?", conn, params=(prod,))
    dbClose(conn)
    if df.empty:
        return df
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    return df
