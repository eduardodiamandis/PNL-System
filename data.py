import psycopg2
import pandas as pd

# Connect and Close

def dbConn():
    try:
        # Tenta usar as credenciais do Streamlit Secrets (produção)
        conn = psycopg2.connect(
            host=st.secrets["postgres"]["host"],
            database=st.secrets["postgres"]["database"],
            user=st.secrets["postgres"]["user"],
            password=st.secrets["postgres"]["password"],
            port=st.secrets["postgres"]["port"],
            sslmode='require'  # IMPORTANTE para Supabase
        )
        return conn
    except Exception as e:
        print(f'Erro conexão com Supabase: {e}')
        # Fallback para desenvolvimento local
        try:
            return psycopg2.connect(
                host='localhost', 
                database='PNL',
                user='ZenNohDev', 
                password='Zgbr@2025'
            )
        except Exception as e2:
            print(f'Erro conexão local: {e2}')
            return None

        # validação mínima
        if not all([host, database, user, password]):
            raise ConnectionError(
                "Parâmetros de conexão incompletos: host/database/user/password. "
                "Verifique .streamlit/secrets.toml (local) ou Secrets no Streamlit Cloud."
            )

        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port,
            connect_timeout=10
        )
        return conn

    except Exception as e:
        # Lança exceção clara para o chamador (não retornar None)
        raise ConnectionError(f"Erro conexão ao banco de dados: {e}")
    
def dbClose(conn):
    if conn:
        conn.close()

# Create Tables
def dbCreateTable():
    conn = dbConn()
    cursor  = conn.cursor()
    tradeTb = """
        CREATE TABLE IF NOT EXISTS tradeTb(
            id      SERIAL PRIMARY KEY,
            prod    VARCHAR(7) NOT NULL,
            cat     VARCHAR(10) NOT NULL,
            ship    VARCHAR(3) NOT NULL,
            year    INTEGER NOT NULL,
            op      VARCHAR(8) NOT NULL,
            ton     INTEGER NOT NULL,
            lvl     NUMERIC(4,2) NOT NULL,
            notion  NUMERIC(11,2) NOT NULL,
            date    DATE DEFAULT CURRENT_DATE,
            reg     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """
    mtmtb = """
        CREATE TABLE IF NOT EXISTS mtmtb(
            idPnl   SERIAL PRIMARY KEY,
            idTrade INTEGER NOT NULL,
            prod    VARCHAR(7) NOT NULL,
            cat     VARCHAR(10) NOT NULL,
            ship    VARCHAR(3) NOT NULL,
            year    INTEGER NOT NULL,
            mtm  NUMERIC(4,2) NOT NULL,
            pnl  NUMERIC(11,2) NOT NULL,
            date    DATE DEFAULT CURRENT_DATE,
            reg     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """
    posTb = """
        CREATE TABLE IF NOT EXISTS posTb(
            id      SERIAL PRIMARY KEY,
            prod    VARCHAR(7) NOT NULL,
            cat     VARCHAR(10) NOT NULL,
            ship    VARCHAR(3) NOT NULL,
            year    INTEGER NOT NULL,
            pos     INTEGER NOT NULL,
            date    DATE DEFAULT CURRENT_DATE,
            reg     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """
    pnltb = """
        CREATE TABLE IF NOT EXISTS pnltb(
            id      SERIAL PRIMARY KEY,
            prod    VARCHAR(7) NOT NULL,
            cat     VARCHAR(10) NOT NULL,
            ship    VARCHAR(3) NOT NULL,
            year    INTEGER NOT NULL,
            pnl     NUMERIC(11,2) NOT NULL,
            date    DATE DEFAULT CURRENT_DATE,
            reg     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );"""
    
    try:
        cursor.execute(tradeTb)
        cursor.execute(mtmtb)
        cursor.execute(posTb)
        conn.commit()
    except Exception as e:
        print(f'Erro criar tabelas: {e}')
        conn.rollback()
    finally:
        cursor.close()
        dbClose(conn)

# Inserts
def dbInsertTrade(product, category, shipment, year, operation, ton, lvl, notion):
    conn = dbConn(); cursor = conn.cursor()
    query = """
        INSERT INTO tradeTb(prod, cat, ship, year, op, ton, lvl, notion)
        VALUES (%s,%s,%s,%s,%s,%s,%s, %s);
    """
    try:
        cursor.execute(query, (product, category, shipment, year, operation, ton, lvl, notion))
        conn.commit()
    except Exception as e:
        print(f'Erro insertTrade: {e}')
        conn.rollback()
    finally:
        cursor.close(); dbClose(conn)

def dbInsertPnl(id, product, category, shipment, year, mtm, pnl):
    conn = dbConn(); cursor = conn.cursor()
    query = """
        INSERT INTO mtmtb(idTrade, prod, cat, ship, year, mtm, pnl)
        VALUES (%s,%s,%s,%s,%s,%s,%s);
    """
    try:
        cursor.execute(query, (id, product, category, shipment, year, mtm, pnl))
        conn.commit()
    except Exception as e:
        print(f'Erro insertPnl: {e}')
        conn.rollback()
    finally:
        cursor.close(); dbClose(conn)

def dbInsertPos (product, category, shipment, year, posisiton):
    conn = dbConn(); cursor = conn.cursor()
    query = """
        INSERT INTO posTb(prod, cat, ship, year, pos)
        VALUES (%s,%s,%s,%s,%s);
    """
    try:
        cursor.execute(query, (product, category, shipment, year, posisiton))
        conn.commit()
    except Exception as e:
        print(f'Erro insertPnl: {e}')
        conn.rollback()
    finally:
        cursor.close(); dbClose(conn)


# Fetch
def dbFetchMtM(id):
    conn = dbConn(); cursor = conn.cursor()
    query = """
        SELECT mtm, reg FROM mtmtb
        WHERE idTrade=%s
        ORDER BY reg DESC LIMIT 1;
    """
    cursor.execute(query, (id,))
    row = cursor.fetchone()
    cursor.close(); dbClose(conn)
    if row is None:
        return None
    else:
        return row[0]

def dbFetchPnl(prod, cat, ship, year):
    conn = dbConn(); cursor = conn.cursor()
    query = """
        SELECT mtm 
        FROM mtmtb
        WHERE prod=%s AND cat=%s AND ship=%s AND year=%s
        ORDER BY reg DESC LIMIT 1;
    """
    cursor.execute(query, (prod, cat, ship, year))
    row = cursor.fetchone()
    cursor.close(); dbClose(conn)
    return row[0]
    
def dbFetchPos(prod, cat, ship, year):
    conn = dbConn(); cursor = conn.cursor()
    query = """
        SELECT pos FROM posTb
        WHERE prod=%s AND cat=%s AND ship=%s AND year=%s
        ORDER BY reg DESC LIMIT 1;
    """
    cursor.execute(query, (prod, cat, ship, year))
    row = cursor.fetchone()
    cursor.close(); dbClose(conn)
    return row[0] if row else 0

def dbFetchTrade(prod, cat, ship, year):
    conn = dbConn(); cursor = conn.cursor()
    query = """
        SELECT id, op, ton, lvl
        FROM tradetb
        WHERE prod=%s AND cat=%s AND ship=%s AND year=%s
    """

    cursor.execute(query, (prod, cat, ship, year))
    rows = cursor.fetchall()
    cursor.close(); dbClose(conn)
    return rows

# DataFrame loaders
def dbLoadPnl(prod, year):
    conn = dbConn()
    query = """
        SELECT *
        FROM mtmtb
        WHERE prod ILIKE %s AND year = %s
        ORDER BY reg DESC;
    """
    df = pd.read_sql(query, conn, params=(prod, year))
    dbClose(conn)

    orderCol = ['Jan','Feb','Mar','Apr','May','Jun',
                'Jul','Aug','Sep','Oct','Nov','Dec']
    orderRow = ['FOB Vessel','FOB Paper','C&F Vessel']

    df['date'] = pd.to_datetime(df['date'])
    # 1) Extrair mês da coluna 'reg' e agrupar por mês
    df['month'] = df['date'].dt.month_name().str[:3]  # Converte para nome do mês (3 primeiras letras)
    
    grouped = (
        df.groupby(['cat', 'month'])
        ['pnl'].sum()
        .reset_index()
    )

    # 3) Pivot + fillna
    table = (
        grouped
        .pivot(index='cat', columns='month', values='pnl')
        .reindex(columns=orderCol, fill_value=0)
        .reindex(orderRow, fill_value=0)
        .fillna(0)
    )

    # 4) Year e totais
    table['Year'] = table.sum(axis=1)
    total_row = table.sum(axis=0).to_frame().T
    total_row.index = ['Total']

    return pd.concat([table, total_row])


def dbLoadPos(prod, year):
    conn = dbConn()
    query = """
        SELECT pos, cat, ship, year, reg
        FROM posTb
        WHERE prod ILIKE %s AND year = %s
        ORDER BY reg DESC;
    """
    df = pd.read_sql(query, conn, params=(prod, year))
    dbClose(conn)

    orderCol = ['Jan','Feb','Mar','Apr','May','Jun',
                'Jul','Aug','Sep','Oct','Nov','Dec']
    orderRow = ['FOB Vessel','FOB Paper','C&F Vessel']

    df_unique = (
        df.sort_values('reg', ascending=False)
          .drop_duplicates(subset=['cat','ship'], keep='first')
    )

    table = (
        df_unique
        .pivot(index='cat', columns='ship', values='pos')
        .reindex(columns=orderCol, fill_value=0)
        .reindex(orderRow, fill_value=0)
        .fillna(0)
    )

    table['Year'] = table.sum(axis=1)
    total_row = table.sum(axis=0).to_frame().T
    total_row.index = ['Total']

    return pd.concat([table, total_row])


def dbLoadMtm(prod, year):
    conn = dbConn()
    query = """
        SELECT mtm, cat, ship, year
        FROM mtmtb
        WHERE prod ILIKE %s AND year = %s
        ORDER BY reg DESC;
    """
    df = pd.read_sql(query, conn, params=(prod, year))
    dbClose(conn)

    orderCol = ['Jan','Feb','Mar','Apr','May','Jun',
                'Jul','Aug','Sep','Oct','Nov','Dec']
    orderRow = ['FOB Vessel','FOB Paper','C&F Vessel']

    df_unique = (
        df.sort_values('reg', ascending=False)
          .drop_duplicates(subset=['cat','ship'], keep='first')
    )

    table = (
        df_unique
        .pivot(index='cat', columns='ship', values='mtm')
        .reindex(columns=orderCol, fill_value=0)
        .reindex(orderRow, fill_value=0)
        .fillna(0)
    )

    table['Year'] = table.sum(axis=1)
    total_row = table.sum(axis=0).to_frame().T
    total_row.index = ['Total']

    return pd.concat([table, total_row])


def dbLoadMtm(prod, year):
    conn = dbConn()
    query = """
        SELECT mtm, cat, ship, year, reg
        FROM mtmtb
        WHERE prod ILIKE %s AND year = %s
        ORDER BY reg DESC;
    """
    df = pd.read_sql(query, conn, params=(prod, year))
    dbClose(conn)

    orderCol = ['Jan','Feb','Mar','Apr','May','Jun',
                'Jul','Aug','Sep','Oct','Nov','Dec']
    orderRow = ['FOB Vessel','FOB Paper','C&F Vessel']

    df_unique = df.sort_values('reg', ascending=False) \
                  .drop_duplicates(subset=['cat','ship'], keep='first')

    table = (
        df_unique
        .pivot(index='cat', columns='ship', values='mtm')
        .reindex(columns=orderCol, fill_value=0)
        .reindex(orderRow, fill_value=0)
    )
    return table



def dbLoadTrade():
    conn = dbConn()
    query = """SELECT * FROM tradeTb"""
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


# Graph loader
def dbLoadGraphPnl(prod, table='mtmtb'):
    conn = dbConn()

    query = f"""
        WITH latest AS (
          SELECT DISTINCT ON (cat, date)
            pnl, cat, date
          FROM {table}
          WHERE prod ILIKE %s
          ORDER BY cat, date, reg DESC
        )
        SELECT pnl, cat, date
        FROM latest
        ORDER BY date;
    """

    df = pd.read_sql(query, conn, params=[prod])
    dbClose(conn)
    return df

if __name__ == '__main__':
    dbCreateTable()
