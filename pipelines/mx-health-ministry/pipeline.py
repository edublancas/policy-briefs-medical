import zipfile
from datetime import date, timedelta
from io import StringIO
import numpy as np
import re
import argparse
from pathlib import Path

import pandas as pd

from ploomber import DAG, SourceLoader
from ploomber.products import File, GenericProduct
from ploomber.tasks import (PythonCallable, ShellScript, UploadToS3,
                            DownloadFromURL)
from ploomber.clients import SQLAlchemyClient

parser = argparse.ArgumentParser(description='Run pipeline')
parser.add_argument('--upload', action='store_true')
args = parser.parse_args()


def parse_bad_line(line, regex):
    elements = re.findall(regex, line)

    if not elements:
        raise ValueError('Error parsing line: ', line)

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
    lines_fixed = [parse_bad_line(l.replace(',', ''), regex)
                   for l in lines_bad]
    fixed = lines_fixed + list(lines_good)

    filtered = []

    for l in fixed:
        if len(l.split(',')) != 6:
            print('Ignoring row: ', l)
        else:
            filtered.append(l)

    content = '\n'.join(filtered)

    names = ['N Caso', 'Estado', 'Sexo', 'Edad',
             'Fecha de inicio de síntomas',
             'Identificación de COVID-19 por RT-PCR en tiempo real']
    df = pd.read_csv(StringIO(content), names=names)
    df.to_csv(str(product), index=False)


def _uncompress(upstream, product):
    with zipfile.ZipFile(str(upstream.first), 'r') as zip_ref:
        zip_ref.extractall(str(product))


def _pop_by_state(upstream, product):
    path = Path(str(upstream.first), 'iter_00_cpv2010', 'conjunto_de_datos',
                'iter_00_cpv2010.csv')
    df = pd.read_csv(str(path))
    df = df[(df.mun == 0) & (df['loc'] == 0) & (df.entidad > 0)]
    df[['nom_ent', 'pobtot']].to_csv(str(product), index=False)


def _agg(upstream, product):
    pop = pd.read_csv(str(upstream['pop_by_state']))
    confirmed = pd.read_csv(str(upstream['clean_confirmed']))
    suspected = pd.read_csv(str(upstream['clean_suspected']))

    pop['nom_ent'] = pop.nom_ent.str.upper()
    pop['nom_ent'] = pop.nom_ent.replace({'COAHUILA DE ZARAGOZA': 'COAHUILA',
                                          'DISTRITO FEDERAL': 'CIUDAD DE MÉXICO',
                                          'MICHOACÁN DE OCAMPO': 'MICHOACÁN',
                                          'QUERÉTARO': 'QUERETARO',
                                          'VERACRUZ DE IGNACIO DE LA LLAVE':
                                          'VERACRUZ'})
    pop.columns = ['Estado', 'Poblacion']

    conf_by_state = pd.DataFrame(confirmed.groupby('Estado').size())
    conf_by_state.columns = ['Confirmados']

    susp_by_state = pd.DataFrame(suspected.groupby('Estado').size())
    susp_by_state.columns = ['Sospechosos']

    df = pop.merge(conf_by_state, on='Estado', how='left')
    df = df.merge(susp_by_state, on='Estado', how='left')

    df['confirmados_sobre_100k_habs'] = (df.Confirmados /
                                         (df.Poblacion / 100_000))
    df['sospechosos_sobre_100k_habs'] = (df.Sospechosos /
                                         (df.Poblacion / 100_000))

    df.to_csv(str(product), index=False)


def make(date_):

    date_str = date_.strftime('%Y.%m.%d')
    ROOT = Path('data', date_str)
    ROOT.mkdir(exist_ok=True, parents=True)

    dag = DAG()

    client = SQLAlchemyClient('sqlite:///metadata.db')
    dag.clients[GenericProduct] = client

    loader = SourceLoader(path='.')

    source = 'https://www.inegi.org.mx/contenidos/programas/ccpv/2010/datosabiertos/iter_nal_2010_csv.zip'
    population_zip = DownloadFromURL(source, File(ROOT / 'population.zip'),
                                     dag, name='population.zip')
    population = PythonCallable(_uncompress, File(ROOT / 'population'),
                                dag, name='population')
    pop_by_state = PythonCallable(_pop_by_state,
                                  File(ROOT / 'pop_by_state.csv'),
                                  dag, name='pop_by_state')

    population_zip >> population >> pop_by_state

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
        r'^(\d+)\s{1}([\w\s]+)\s{1}(FEMENINO|MASCULINO)\s{1}(\d+)\s{1}(.+)\s{1}(Confirmado)')
    suspected_regex = re.compile(
        r'^(\d+)\s{1}([\w\s]+)\s{1}(FEMENINO|MASCULINO)\s{1}(\d+)\s{1}(.+)\s?(Sospechoso)')

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

    agg = PythonCallable(_agg,
                         File(ROOT / 'cases_and_population.csv'),
                         dag,
                         name='cases_pop')

    confirmed >> clean_confirmed >> agg
    suspected >> clean_suspected >> agg
    pop_by_state >> agg

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

        upload_agg = UploadToS3('{{upstream["cases_pop"]}}',
                                GenericProduct(
                                    'mx-health-ministry/{}/cases_pop.csv'.format(date_str)),
                                dag,
                                bucket='mx-covid-data',
                                name='upload_cases_pop')

        agg >> upload_agg

    return dag


yesterday = date.today() - timedelta(days=1)

dag = make(yesterday)

dag.build()

table = dag.status()

print(table)
