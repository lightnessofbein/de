import pandas as pd
import ast
import config
import sqlite3 as sq


if __name__ == '__main__':
    #TODO: add marker for unknown/incorrect states
    debug_path = '/Users/sfeda/Projects/test_task_de/recources/sample_us_users.csv'
    data = pd.read_csv(debug_path, chunksize=config.CHUNKSIZE)
    conn = sq.connect('{}.sqlite'.format(config.SQLITE_TABLE_NAME))
    i = 0
    for chunk in data:
        chunk.reset_index(drop=True, inplace=True)
        chunk = chunk.join(pd.json_normalize(chunk['address'].apply(ast.literal_eval)))
        chunk.drop(columns=['address'], inplace=True)
        # repairing states
        chunk['state'] = chunk['state'].str.lower()
        chunk['state'] = chunk['state'].str.replace('us-', '')
        chunk['state'] = chunk['state'].str.replace('[^\w\s]','').str.strip()
        chunk['state'] = chunk['state'].replace(config.US_states_mapper)
        chunk['state'].fillna('')

        chunk.to_sql(config.SQLITE_TABLE_NAME, conn, if_exists='append', index=False) # writes to file
    conn.close()
# %%
