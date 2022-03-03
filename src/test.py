import config
import pandas as pd
import sqlite3 as sq

conn = sq.connect('{}.sqlite'.format(config.SQLITE_TABLE_NAME))
df = pd.read_sql('select * from {}'.format(config.SQLITE_TABLE_NAME), conn)
print(df.shape)
conn.close()