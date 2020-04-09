from datetime import date, timedelta
from io import StringIO
import numpy as np
import re
import argparse
from pathlib import Path

import pandas as pd

from ploomber import DAG, SourceLoader
from ploomber.products import File, GenericProduct
from ploomber.tasks import PythonCallable, ShellScript, UploadToS3
from ploomber.clients import SQLAlchemyClient

parser = argparse.ArgumentParser(description='Run pipeline')
parser.add_argument('--upload', action='store_true')
args = parser.parse_args()


def parse_bad_line(line, regex):
    elements = re.findall(regex, line)
    return ','.join(elements[0])


def _clean(upstream, product, regex):
    lines = np.array(Path(str(upstream.first['csv'])).read_text().split('\n'))

    idxs = np.zeros(len(lines)).astype(int)

    regex_good = re.compile(r'^\d+,')
    regex_first_page = re.compile(r'^\d+')

    for i, line in enumerate(lines):
        if re.match(regex_good, line):
            idxs[i] = 1
        elif re.match(regex_first_page, line):
            idxs[i] = 2

    lines_good = lines[idxs == 1]
    lines_bad = lines[idxs == 2]
    lines_fixed = [parse_bad_line(l, regex) for l in lines_bad]

    fixed = lines_fixed + list(lines_good)
    content = '\n'.join(fixed)

    names = ['N Caso', 'Estado', 'Sexo', 'Edad',
             'Fecha de inicio de síntomas',
             'Identificación de COVID-19 por RT-PCR en tiempo real']
    df = pd.read_csv(StringIO(content), names=names)
    df.to_csv(str(product), index=False)


def make(date_):

    date_str = date_.strftime('%Y.%m.%d')
    ROOT = Path('data', date_str)
    ROOT.mkdir(exist_ok=True, parents=True)

    dag = DAG()

    client = SQLAlchemyClient('sqlite:///metadata.db')
    dag.clients[GenericProduct] = client

    loader = SourceLoader(path='.')

    confirmed = ShellScript(loader['get_confirmed.sh'],
                            {'pdf': File(ROOT / 'confirmed.pdf'),
                             'csv': File(ROOT / 'confirmed.csv')},
                            dag,
                            params={'date_str': date_str})
    suspected = ShellScript(loader['get_suspected.sh'],
                            {'pdf': File(ROOT / 'suspected.pdf'),
                             'csv': File(ROOT / 'suspected.csv')},
                            dag,
                            params={'date_str': date_str})

    confirmed_regex = re.compile(
        r'^(\d+)\s{1}([\w\s]+)\s{1}(FEMENINO|MASCULINO)\s{1}(\d+),(.+),(Confirmado)')
    suspected_regex = re.compile(
        r'^(\d+)\s{1}([\w\s]+)\s{1}(FEMENINO|MASCULINO)\s{1}(\d+)\s{1}(.+)\s{1}(Sospechoso)')

    clean_confirmed = PythonCallable(_clean,
                                     File(ROOT / 'confirmed_clean.csv'),
                                     dag,
                                     name='clean_confirmed',
                                     params={'regex': confirmed_regex})

    clean_suspected = PythonCallable(_clean,
                                     File(ROOT / 'suspected_clean.csv'),
                                     dag,
                                     name='clean_suspected',
                                     params={'regex': suspected_regex})

    confirmed >> clean_confirmed
    suspected >> clean_suspected

    if args.upload:
        upload_confirmed = UploadToS3('{{upstream["clean_confirmed"]}}',
                                      GenericProduct(
                                          'mx-health-ministry/{}/confirmed.csv'.format(date_str)), dag,
                                      bucket='mx-covid-data',
                                      name='upload_mx_confirmed')
        upload_suspected = UploadToS3('{{upstream["clean_suspected"]}}',
                                      GenericProduct(
                                          'mx-health-ministry/{}/suspected.csv'.format(date_str)),
                                      dag,
                                      bucket='mx-covid-data',
                                      name='upload_mx_suspected')

        clean_confirmed >> upload_confirmed
        clean_suspected >> upload_suspected

    return dag


yesterday = date.today() - timedelta(days=1)

dag = make(yesterday)

dag.build()

table = dag.status()

print(table)
