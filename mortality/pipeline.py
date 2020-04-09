from pathlib import Path
import shutil
import zipfile

import pandas as pd

from ploomber import DAG
from ploomber.products import File
from ploomber.tasks import DownloadFromURL, PythonCallable

ROOT = Path('data/')
ROOT.mkdir(exist_ok=True)


def _uncompress(upstream, product):
    with zipfile.ZipFile(str(upstream.first), 'r') as zip_ref:
        zip_ref.extractall(str(product))


def _get_from_folder(upstream, product):
    filename = 'API_SP.POP.TOTL_DS2_en_csv_v2_887275.csv'
    src = Path(str(upstream.first), filename)
    shutil.copy(str(src), str(product))


def _clean(upstream, product):
    df = pd.read_csv(str(upstream.first), skiprows=3)
    df = df[['Country Name', '2018']]
    df.rename({'Country Name': 'country', '2018': 'population'},
              axis='columns', inplace=True)
    df.to_csv(str(product), index=False)


# https://data.worldbank.org/indicator/sp.pop.totl
dag = DAG()
source = 'http://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv'
compressed = DownloadFromURL(source, File(ROOT / 'population.zip'), dag,
                             name='compressed')
uncompress = PythonCallable(_uncompress, File(ROOT / 'population/'), dag,
                            name='uncompress')
get_from_folder = PythonCallable(_get_from_folder, File(ROOT / 'population.csv'), dag,
                                 name='get_from_folder')
clean = PythonCallable(_clean, File(
    ROOT / 'population_clean.csv'), dag, name='pop_clean')

compressed >> uncompress >> get_from_folder >> clean

# deaths
# source: https://github.com/CSSEGISandData/COVID-19

source_deaths = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'

deaths = DownloadFromURL(source_deaths, File(ROOT / 'deaths.csv'), dag,
                         name='deaths')


def _clean_deaths(upstream, product):
    df = pd.read_csv(str(upstream.first))
    df.drop(['Province/State', 'Lat', 'Long'], axis='columns', inplace=True)
    df.rename({'Country/Region': 'country'}, axis='columns', inplace=True)
    df = df.groupby('country').sum()
    df = df[[df.columns[-1]]]
    df.columns = ['deaths']
    df.to_csv(str(product))


clean_deaths = PythonCallable(_clean_deaths, File(ROOT / 'deaths_clean.csv'), dag,
                              name='deaths_clean')

deaths >> clean_deaths


def _mortality_rate(upstream, product):
    pop = pd.read_csv(str(upstream['pop_clean']))
    deaths = pd.read_csv(str(upstream['deaths_clean']))

    df = deaths.merge(pop, on='country', how='left')
    df['deaths_over_100k'] = df.deaths / (df.population / 100_000)
    df.to_csv(str(product), index=False)


rate = PythonCallable(_mortality_rate, File(ROOT / 'mortality_rate.csv'), dag,
                      name='mortality_rate')

(clean_deaths + clean) >> rate

dag.build()
