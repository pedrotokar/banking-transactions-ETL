import sqlite3
con = sqlite3.connect("data/teste_sql.db")
cur = con.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS sql_table(string, int, float)") 

for i in range(100):
    cur.execute("""
        INSERT INTO sql_table VALUES
            ('A',1,4.2),
            ('B',2,3.7),
            ('C',3,9.7)
    """)

con.commit()