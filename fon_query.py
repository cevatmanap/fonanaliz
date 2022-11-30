import yaml
import datetime
import sqlite3
import pandas as pd
import seaborn
import matplotlib.pyplot as plt
import os

dir_path = os.path.dirname(os.path.abspath(__file__))
conf_path = os.path.join(dir_path, "config.yaml")
conf = yaml.safe_load(open(conf_path, "r"))
DB_FILE = conf["DB_FILE"]
OLDEST_DATE = datetime.date.fromisoformat(conf["OLDEST"])

def connect_db():
    """
    Connect to Database

    Returns
    -------
    conn: SQL Connection Object
    """
    sql_connection = sqlite3.connect(DB_FILE)
    return sql_connection

def close_db(conn):
    """
    Close the given SQL connection.

    Parameters
    ----------
    conn: SQL Connection Object
    """
    conn.close()

def get_prices(sql_connection, foncode):
    """
    Get prices for the given Fund code.

    Parameters
    ----------
    sql_connection: SQL Connection Object
    foncode: str
        Fund code
    Returns
    -------
    row: list
        Fund prices as a list of floats
    """    
    cur = sql_connection.cursor()
    sql_command = "select price from t_fon_data where code = \"%s\"" % foncode
    cur.execute(sql_command)
    row = cur.fetchall()
    row = [float(x[0]) for x in row]
    return row


def get_prices_df(sql_connection, foncode):
    """
    Get prices for the given Fund code.

    Parameters
    ----------
    sql_connection: SQL Connection Object
    foncode: str
        Fund code
    Returns
    -------
    df: pandas.DataFrame
        Fund prices as a Pandas DataFrame
    """    
    df = pd.read_sql_query("SELECT code, date, price from t_fon_data where code = \"%s\"" %
                           foncode, sql_connection, parse_dates="date", dtype={"code": "str", "price": "float"})
    return df


def normalize_df(frame):
    """
    Rescale the "price" column of a DataFrame so that the earliest entry is scaled to 1.0

    Parameters
    ----------
    frame: pandas.DataFrame
        A Pandas DataFrame with columns "date" and "price"
    
    Returns
    -------
    frame: pandas.DataFrame
        Rescaled  DataFrame
    """
    min_idx = frame["date"].idxmin()
    factor = 1.0 / frame["price"][min_idx]
    frame["price"] *= factor
    return frame


def plot_prices(sql_connection, fon_codes, normalize=False):
    for c in fon_codes:
        d = get_prices_df(sql_connection, c)
        if normalize:
            d = normalize_df(d)
        seaborn.lineplot(data=d, x="date", y="price", label=d["code"][0])
    plt.show()


def get_fon_codes_with_min_attribute(sql_connection, attrib, min_val):
    """
    Get list of Fund codes with a minimum asset ratio

    Parameters
    ----------
    sql_connection: SQL Connection Object
    attrib: str
        Asset type to check
    min_val: float
        Minimum ratio for the given asset type

    Returns
    -------
    res: list(str)
        List of Fund Codes    
    """
    cur = sql_connection.cursor()
    sql_command = "select code from t_fon_data where %s > %s group by code" % (
        attrib, min_val)
    cur.execute(sql_command)
    row = cur.fetchall()
    res = []
    for r in row:
        res.append(r[0])
    return res


def get_fon_codes_with_max_attribute(sql_connection, attrib, max_val):
    """
    Get list of Fund codes with a maximum asset ratio

    Parameters
    ----------
    sql_connection: SQL Connection Object
    attrib: str
        Asset type to check
    max_val: float
        Maximum ratio for the given asset type

    Returns
    -------
    res: list(str)
        List of Fund Codes    
    """
    cur = sql_connection.cursor()
    sql_command = "select code from t_fon_data where %s < %s group by code" % (
        attrib, max_val)
    cur.execute(sql_command)
    row = cur.fetchall()
    res = []
    for r in row:
        res.append(r[0])
    return res


def get_change(sql_connection, fon_codes, prev_days):
    """
    Get relative changes for given fund codes for past days

    Parameters
    ----------
    sql_connection: SQL Connection Object
    fon_codes: list(str)
        List of Fund Codes
    prev_days: int
        Number of days to look back

    Returns
    -------
    result: pandas.DataFrame
        Pandas DataFrame with columns "code", "change"
    """
    end_time = datetime.date.today()
    start_time = end_time - datetime.timedelta(days=prev_days)
    cur = sql_connection.cursor()

    res = pd.DataFrame(columns=["code", "change"])

    for c in fon_codes:
        sql_command = """select date, price from t_fon_data \
            where code = \"%s\" and date > \"%s\" and date < \"%s\"""" % (
            c, start_time, end_time)
        cur.execute(sql_command)
        row = cur.fetchall()
        if len(row) > 0:
            start_price = row[-1][1]
            end_price = row[0][1]
            n = pd.DataFrame(
                [[c, (end_price - start_price) / start_price]], columns=["code", "change"])
            res = pd.concat([res, n], ignore_index=True)
    return res


def get_prices_between_dates(sql_connection, fon_code, start, end, normalize=False):
    end_time = datetime.date.fromisoformat(end)
    start_time = datetime.date.fromisoformat(start)
    cur = sql_connection.cursor()

    res = pd.DataFrame(columns=["code", "date", "price"])

    d = pd.read_sql_query("SELECT code, date, price from t_fon_data where code = \"%s\" and date > \"%s\" and date < \"%s\"" %
                        (fon_code, start_time, end_time), sql_connection, parse_dates="date", dtype={"code": "str", "price": "float"})
    if normalize:
        d = normalize_df(d)
    
    return d


def get_foncodes_with_keyword_in_fontitle(sql_connection, keyword):
    cur = sql_connection.cursor()
    sql_command = "select code from t_fon_title where title like \"%%%s%%\"" % (keyword.upper())
    #print(sql_command)
    cur.execute(sql_command)
    row = cur.fetchall()
    res = []
    for r in row:
        res.append(r[0])
    return res

