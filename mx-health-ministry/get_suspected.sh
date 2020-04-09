URL=https://www.gob.mx/cms/uploads/attachment/file/545941/Tabla_casos_sospechosos_COVID-19_2020.04.08.pdf

mkdir data/
wget -O data/suspected.pdf $URL
java -jar bin/tabula.jar data/suspected.pdf --pages all --outfile {{product}}