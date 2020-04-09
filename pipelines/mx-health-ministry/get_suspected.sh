URL=https://www.gob.mx/cms/uploads/attachment/file/545941/Tabla_casos_sospechosos_COVID-19_{{date_str}}.pdf

mkdir data/
wget -O {{product['pdf']}} $URL
java -jar bin/tabula.jar {{product['pdf']}} --pages all --outfile {{product['csv']}}