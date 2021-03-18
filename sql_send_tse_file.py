import pandas as pd
from sqlalchemy import create_engine
from optparse import OptionParser   
import sys
import re
from pyedflib import highlevel


class sql_query:
    def __init__(self, credentials_path, skiprows, tse_file):
        self.db_credentials = pd.read_csv(credentials_path, index_col="Field")
        self.skiprows = skiprows
        self.tse_file = tse_file
        self.headers = highlevel.read_edf_header(self.tse_file[:-6] + 'edf')
        self.startdate = pd.to_datetime(self.headers['startdate'])
        self.enddate = self.startdate + pd.Timedelta(
            seconds=self.headers['Duration'])


    def __call__(self):

        engine = create_engine(
            "mysql+pymysql://{user}:{pw}@localhost/{db}".format(
                user=self.db_credentials.loc["user"][0],
                pw=self.db_credentials.loc["password"][0],
                db="grafana"))

        # Preparing df to send to sql database
        df_to_send = pd.read_sql('select * from tse_import;', engine)
        df_to_send.drop(df_to_send.index, inplace=True)

        # Loading tse file
        df_tse = pd.read_csv(self.tse_file, skiprows=self.skiprows, sep=' ')
        df_tse.columns = ['start', 'end', 'annot', 'probability']
        df_tse = df_tse[df_tse['annot'] != 'bckg']
        df_tse['annot'] = df_tse['annot'].apply(lambda x: '["' + x + '"]')

        if df_tse.shape[0] == 0:
            return

        # Creating need variables
        # name_file = tse_file[re.search('annotation_*', tse_file).start():]


        # modify
        patient_path = self.tse_file[re.search('data/', self.tse_file).end():]
        patient = patient_path[:re.search('/', patient_path).end()-1]

        record_path = patient_path[len(patient)+1:]
        record = record_path[:re.search('/', record_path).end()-1]

        annotator = 'tuh'

        try:
            annotation_date = self.startdate
        except Exception:
            annotation_date = ''
        text = annotator + ' ' + str(annotation_date)

        # updating data to send
        df_to_send['epoch'] = df_tse['start'].apply(lambda x: self.startdate + 
        pd.Timedelta(seconds=x))
        df_to_send['text'] = text
        df_to_send['tags'] = df_tse['annot']
        df_to_send['epoch_end'] = df_tse['end'].apply(lambda x: self.startdate + 
        pd.Timedelta(seconds=x))
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

    parser = OptionParser()
    parser.add_option("-t", "--tse_file", dest="tse_file",
                      help="input tse file", metavar="FILE")
    parser.add_option("-c", "--cred_path", dest="cred_path",
                      help="input credential path", metavar="FILE",
                      default='aws_mariadb_credentials.csv')
    parser.add_option("-r", "--skiprows", dest="skiprows",
                      help="rows to skip in tse file", metavar="FILE",
                      default=1)

    (options, args) = parser.parse_args()


    engine = sql_query(options.cred_path, options.skiprows, options.tse_file)
    engine()
