# wikidata-pylisting
Scripts per a recuperar dades de Wikidata i combinar-les amb altres API

# Instal·lació i ús

## pyenv

    pyenv virtualenv 3.7.4 pylisting
    pyenv shell pylisting
    pip install -r requirements

## Llistat d'últimes entrades de dones

SPARQL

    SELECT ?item ?itemLabel ?article WHERE {
      ?item wdt:P31 wd:Q5.
      ?item wdt:P21 wd:Q6581072 .
      ?article schema:about ?item .
      ?article schema:isPartOf <https://ca.wikipedia.org/> .
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],ca" } .
    } ORDER BY ?itemLabel

A continuació resultats per darrera creació. Màx. 50 títols per consulta

https://ca.wikipedia.org/w/api.php?action=query&prop=revisions&rvlimit=1&rvprop=timestamp&rvdir=newer&titles=A._Brun
