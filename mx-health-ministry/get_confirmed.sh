URL=https://www.gob.mx/cms/uploads/attachment/file/545940/Tabla_casos_positivos_COVID-19_resultado_InDRE_2020.04.08.pdf

mkdir data/
wget -O data/confirmed.pdf $URL
java -jar bin/tabula.jar data/confirmed.pdf --pages all --outfile {{product}}