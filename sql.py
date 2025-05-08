"""
-------------------------------------------------------
sql.py - Baseline SQL Query Executor
Author: Sairithik Komuravelly (Team: NoJoinZone)
Description:
    This program executes a standard SQL query directly on the 'sales' table
    using a single cur.execute() call. It is intended to provide a baseline
    output for comparison against the enhanced SQL logic implemented in
    the auto-generated '_generated.py'.

    Use this program to verify the correctness of your ESQL transformation by
    matching its output against that produced by the generated MF query program.
-------------------------------------------------------
"""
import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv


def query():
    """
    Used for testing standard queries in SQL.
    """
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password, cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    #cur.execute("SELECT cust,prod,avg(quant) avg_quant,max(quant) max_quant FROM sales WHERE year=2019 group by cust,prod")
    cur.execute("WITH g1 AS (SELECT cust, COUNT(quant) AS count_1_quant FROM sales WHERE state = 'NY' GROUP BY cust), g2 AS (SELECT cust, SUM(quant) AS sum_2_quant FROM sales WHERE state = 'NJ' GROUP BY cust), g3 AS (SELECT cust, MAX(quant) AS max_3_quant FROM sales WHERE state = 'CT' GROUP BY cust) SELECT distinct sales.cust, g1.count_1_quant AS count_1_quant, g2.sum_2_quant AS sum_2_quant, g3.max_3_quant AS max_3_quant FROM sales JOIN g1 ON sales.cust = g1.cust JOIN g2 ON sales.cust = g2.cust JOIN g3 ON sales.cust = g3.cust order by sales.cust")
    
    return tabulate.tabulate(cur.fetchall(),headers="keys", tablefmt="psql")


def main():
    print(query())


if "__main__" == __name__:
    main()
