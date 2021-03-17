import pandas as pd
from sqlalchemy import create_engine
import sys
import re

credentials_path = 'aws_mariadb_credentials.csv'

i = 1
while i < len(sys.argv):
    if sys.argv[i] == '-tse_file' and i < len(sys.argv)-1:
        tse_file = sys.argv[i+1]
        i += 2
    elif sys.argv[i] == '-credentials_path' and i < len(sys.argv)-1:
        credentials_path = sys.argv[i+1]
        i += 2
    else:
        print('Unknown argument' + str(sys.argv[i]))
        break


class sql_query:
    def __init__(self, credentials_path):
        self.db_credentials = pd.read_csv(credentials_path, index_col="Field")

    def __call__(self, tse_file):

        engine = create_engine(
            "mysql+pymysql://{user}:{pw}@localhost/{db}".format(
                user=self.db_credentials.loc["user"][0],
                pw=self.db_credentials.loc["password"][0],
                db="grafana"))

        # Preparing df to send to sql database
        df_to_send = pd.read_sql('select * from tse_import;', engine)
        df_to_send.drop(df_to_send.index, inplace=True)

        # Loading tse file
        df_tse = pd.read_csv(tse_file, skiprows=3, sep=' ')
        df_tse.columns = ['start', 'end', 'annot', 'probability']
        df_tse = df_tse[df_tse['annot'] != 'bckg']
        df_tse['annot'] = df_tse['annot'].apply(lambda x: '["' + x + '"]')

        if df_tse.shape[0] == 0:
            return

        # Creating need variables
        # name_file = tse_file[re.search('annotation_*', tse_file).start():]

        patient_start = re.search('PAT_.*_', tse_file).start()
        patient_end = re.search('PAT_.*_', tse_file).end()
        patient = tse_file[patient_start:patient_end-1]

        record = int(tse_file[patient_end:][:-4])

        header_annot = pd.read_csv(tse_file,
                                   delimiter='\t',
                                   header=None,
                                   nrows=3)[0][1]

        annotator_start = re.search('= *', header_annot).start() + 2
        annotator_end = re.search('= *', header_annot).end() + 4
        annotator = header_annot[annotator_start:annotator_end]

        header_date = pd.read_csv(tse_file,
                                  delimiter='\t',
                                  header=None,
                                  nrows=3)[0][2]

        try:
            annotation_date_start = re.search('....-..-..',
                                              header_date).start()
            annotation_date_end = re.search('....-..-..',
                                            header_date).end()
            annotation_date = header_date[annotation_date_start:
                                          annotation_date_end]
        except Exception:
            annotation_date = ''
        text = annotator + ' ' + annotation_date

        # updating data to send
        df_to_send['epoch'] = df_tse['start']
        df_to_send['text'] = text
        df_to_send['tags'] = df_tse['annot']
        df_to_send['epoch_end'] = df_tse['end']
        df_to_send['patient'] = patient
        df_to_send['record'] = record
        df_to_send['annotator'] = annotator

        df_to_send['epoch'] = df_to_send['epoch'].apply(
            lambda x: pd.Timestamp(x) + pd.Timedelta(minutes=60))
        df_to_send['epoch_end'] = df_to_send['epoch_end'].apply(
            lambda x: pd.Timestamp(x) + pd.Timedelta(minutes=60))

        # Sending line by line: duplicate will
        for i in range(df_to_send.shape[0]):
            try:
                df_to_send.iloc[i:i+1].to_sql(con=engine,
                                              name='tse_import',
                                              if_exists='append',
                                              index=False)
            except Exception:
                pass


if __name__ == '__main__':
    engine = sql_query(credentials_path)
    df_to_send = engine(tse_file)
