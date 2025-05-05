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
    cur.execute("WITH base_group AS (SELECT cust, prod, SUM(quant) AS sum_quant, COUNT(quant) AS count_quant, AVG(quant) AS avg_quant FROM sales GROUP BY cust, prod), g1 AS (SELECT cust, prod, SUM(quant) AS sum_1_quant, COUNT(quant) AS count_1_quant, AVG(quant) AS avg_1_quant FROM sales WHERE state = 'NY' GROUP BY cust, prod), g2 AS (SELECT cust, prod, SUM(quant) AS sum_2_quant, AVG(quant) AS avg_2_quant FROM sales WHERE state = 'NJ' GROUP BY cust, prod), g3 AS (SELECT cust, prod, AVG(quant) AS avg_3_quant FROM sales WHERE state = 'CT' GROUP BY cust, prod) SELECT base_group.cust, base_group.prod, base_group.avg_quant, base_group.sum_quant, COALESCE(g1.sum_1_quant, 0) AS sum_1_quant, COALESCE(g1.count_1_quant, 0) AS count_1_quant, COALESCE(g1.avg_1_quant, 0) AS avg_1_quant, COALESCE(g2.avg_2_quant, 0) AS avg_2_quant, COALESCE(g3.avg_3_quant, 0) AS avg_3_quant FROM base_group LEFT JOIN g1 ON base_group.cust = g1.cust AND base_group.prod = g1.prod LEFT JOIN g2 ON base_group.cust = g2.cust AND base_group.prod = g2.prod LEFT JOIN g3 ON base_group.cust = g3.cust AND base_group.prod = g3.prod WHERE COALESCE(g1.sum_1_quant, 0) > 2 * COALESCE(g2.sum_2_quant, 0) OR COALESCE(g1.avg_1_quant, 0) > COALESCE(g3.avg_3_quant, 0)"
)

    return tabulate.tabulate(cur.fetchall(),headers="keys", tablefmt="psql")


def main():
    print(query())


if "__main__" == __name__:
    main()
