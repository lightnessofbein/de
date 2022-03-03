from typing import final
import config
import smtplib
import utils
import pandas as pd
import sqlite3 as sq


if __name__ == '__main__':
    try:
        smtp_client = smtplib.SMTP('localhost', config.SMTP_PORT)

        data = pd.read_csv(config.CSV_PATH, chunksize=config.CHUNKSIZE)
        conn = sq.connect('{}.sqlite'.format(config.SQLITE_TABLE_NAME))

        for chunk_index, chunk in enumerate(data):
            chunk = utils.preproc_chunk(chunk)
            chunk.to_sql(config.SQLITE_TABLE_NAME, conn, if_exists='append', index=False)
            utils.send_email(smtp_client, utils.generate_report_message(chunk_index, chunk))
    except Exception as e:
        error_message = f'ETL job failed on {chunk_index}th chunk with message {e}'
        utils.send_email(smtp_client, error_message)
    finally:
        conn.close()
        smtp_client.quit()
