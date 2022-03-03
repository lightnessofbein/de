
import ast
import config

import pandas as pd

from email.mime.text import MIMEText


def send_email(smtp_client, message):
    msg = MIMEText(message)
    msg['Subject'] = 'ETL job'
    msg['From'] = config.EMAIL_SENDER
    msg['To'] = ",".join(config.EMAIL_RECEPIENTS)

    smtp_client.sendmail(config.EMAIL_SENDER, config.EMAIL_RECEPIENTS, msg.as_string())


def preproc_chunk(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.reset_index(drop=True, inplace=True)
    # apply is bottleneck for sure, could be parallelized using swifter at least.
    df = df.join(pd.json_normalize(df['address'].apply(ast.literal_eval)))
    df.drop(columns=['address'], inplace=True)
    # repairing states
    df['state'] = df['state'].str.lower()
    df['state'] = df['state'].str.replace('us-', '')
    df['state'] = df['state'].str.replace('[^\w\s]', '').str.strip()
    df['state'] = df['state'].replace(config.US_states_mapper)
    # for states which dont match mapper from config we may create some indicator/smth according to the business task.
    df['state'].fillna('unknown_state', inplace=True)
    return df


def generate_report_message(chunk_index, df: pd.DataFrame) -> str:
    na_stats = df.isna().sum()
    msg = f'{chunk_index}th chunk was successfully with following stats of NA \n' + \
          f'{na_stats}'
    return msg
