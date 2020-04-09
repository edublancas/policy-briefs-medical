URL=https://www.gob.mx/cms/uploads/attachment/file/545940/Tabla_casos_positivos_COVID-19_resultado_InDRE_{{date_str}}.pdf

mkdir data/
wget -O {{product['pdf']}} $URL
java -jar bin/tabula.jar {{product['pdf']}} --pages all --outfile {{product['csv']}}