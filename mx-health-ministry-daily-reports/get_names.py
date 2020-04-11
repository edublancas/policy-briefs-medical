import argparse
import requests
from bs4 import BeautifulSoup

parser = argparse.ArgumentParser()
parser.add_argument('label')
parser.add_argument('date')
args = parser.parse_args()


res = requests.get(
    'https://www.gob.mx/salud/documentos/coronavirus-covid-19-comunicado-tecnico-diario-238449')

soup = BeautifulSoup(res.text, 'html.parser')

pdfs = [a.get('href') for a in soup.find_all('a')
        if a.get('href') and a.get('href').endswith('.pdf')]

selected = [name for name in pdfs if args.label in name]

if not selected:
    raise ValueError('Could find file')

if len(selected) > 1:
    raise ValueError('Found more than one file')


if args.date not in selected[0]:
    raise ValueError('Could not find date')

print('https://www.gob.mx'+selected[0])
