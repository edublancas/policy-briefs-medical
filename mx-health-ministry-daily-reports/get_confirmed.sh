URL=$(python get_names.py positivos {{date_str}})
mkdir -p data/
wget -O {{product['pdf']}} $URL
java -jar bin/tabula.jar {{product['pdf']}} --pages all --outfile {{product['csv']}}
