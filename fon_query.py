import yaml
import datetime
import sqlite3
import pandas as pd
import seaborn
import matplotlib.pyplot as plt

conf = yaml.safe_load(open("config.yaml", "r"))
DB_FILE = conf["DB_FILE"]
OLDEST_DATE = datetime.date.fromisoformat(conf["OLDEST"])

def connect_db():
    sql_connection = sqlite3.connect(DB_FILE)
    return sql_connection

def close_db(conn):
    conn.close()
    
def get_prices(sql_connection, foncode):
    cur = sql_connection.cursor()
    sql_command = "select price from fondata where code = \"%s\"" % foncode
    cur.execute(sql_command)
    row = cur.fetchall()
    row = [float(x[0]) for x in row]
    return row


def get_prices_df(sql_connection, foncode):
    df = pd.read_sql_query("SELECT code, date, price from fondata where code = \"%s\"" %
                           foncode, sql_connection, parse_dates="date", dtype={"code": "str", "price": "float"})
    return df


def normalize_df(frame):
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
    cur = sql_connection.cursor()
    sql_command = "select code from fondata where %s > %s group by code" % (
        attrib, min_val)
    cur.execute(sql_command)
    row = cur.fetchall()
    res = []
    for r in row:
        res.append(r[0])
    return res


def get_fon_codes_with_max_attribute(sql_connection, attrib, max_val):
    cur = sql_connection.cursor()
    sql_command = "select code from fondata where %s < %s group by code" % (
        attrib, max_val)
    cur.execute(sql_command)
    row = cur.fetchall()
    res = []
    for r in row:
        res.append(r[0])
    return res


def get_change(sql_connection, fon_codes, prev_days):
    end_time = datetime.date.today()
    start_time = end_time - datetime.timedelta(days=prev_days)
    cur = sql_connection.cursor()

    res = pd.DataFrame(columns=["code", "change"])

    for c in fon_codes:
        sql_command = """select date, price from fondata \
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

    d = pd.read_sql_query("SELECT code, date, price from fondata where code = \"%s\" and date > \"%s\" and date < \"%s\"" %
                        (fon_code, start_time, end_time), sql_connection, parse_dates="date", dtype={"code": "str", "price": "float"})
    if normalize:
        d = normalize_df(d)
    
    return d


def get_foncodes_with_keyword_in_fontitle(sql_connection, keyword):
    cur = sql_connection.cursor()
    sql_command = "select code from fontitle where title like \"%%%s%%\"" % (keyword.upper())
    print(sql_command)
    cur.execute(sql_command)
    row = cur.fetchall()
    res = []
    for r in row:
        res.append(r[0])
    return res

