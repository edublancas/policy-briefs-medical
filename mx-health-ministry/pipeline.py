import argparse
from pathlib import Path

from ploomber import DAG, SourceLoader
from ploomber.products import File, GenericProduct
from ploomber.tasks import PythonCallable, ShellScript, UploadToS3
from ploomber.clients import SQLAlchemyClient

parser = argparse.ArgumentParser(description='Run pipeline')
parser.add_argument('--upload', action='store_true')
args = parser.parse_args()

ROOT = Path('data/')
ROOT.mkdir(exist_ok=True)

dag = DAG()

client = SQLAlchemyClient('sqlite:///metadata.db')
dag.clients[GenericProduct] = client

loader = SourceLoader(path='.')

confirmed = ShellScript(loader['get_confirmed.sh'],
                        File('data/confirmed.csv'), dag)
suspected = ShellScript(loader['get_suspected.sh'],
                        File('data/suspected.csv'), dag)

if args.upload:
    upload_confirmed = UploadToS3('{{upstream["get_confirmed.sh"]}}',
                                  GenericProduct('mx_confirmed_s3'), dag,
                                  bucket='mx-covid-data',
                                  name='upload_mx_confirmed')
    upload_suspected = UploadToS3('{{upstream["get_suspected.sh"]}}',
                                  GenericProduct('mx_suspected_s3'), dag,
                                  bucket='mx-covid-data',
                                  name='upload_mx_suspected')

    confirmed >> upload_confirmed
    suspected >> upload_suspected


dag.build()
