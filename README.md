# wikidata-pylisting
Scripts per a recuperar dades de Wikidata i combinar-les amb altres API

# Instal·lació i ús

## pyenv

Install [pyenv](https://github.com/pyenv/pyenv) and a Python version if necessary:

    pyenv install 3.7.4

Install [virtualenv](https://github.com/pyenv/pyenv-virtualenv) plugin and then enable the virtual environment:

    pyenv virtualenv 3.7.4 pylisting
    pyenv shell pylisting
    pip install -r requirements.txt

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

Query:

    curl -X GET https://query.wikidata.org/sparql?query=(SPARQL) -H "Accept: text/tab-separated-values"

# Autoritats

* Dumps de Wikidata a: https://dumps.wikimedia.org/wikidatawiki/entities/

      python autoritiesCheck.py -config /home/toniher/remote-work/mediawiki/allbios.json -authorities conf/autoritats.tsv -dump /scratch/wikidata/20200727/wikidata-20200727-all.json.gz

## WhatLinks

Recupera pàgines amb plantilla autoritat:

      python whatLinksHere.py -config ../allbios.json -title  Plantilla:Autoritat


## Addició de plantilla Autoritat

      python afegeixAutoritat.py -config ../allbios.json -file llistatpagines.txt

## Consultes

* Pàgines sense plantilla Autoritat amb entrades de Wikidata amb almenys 1 recurs d'autoritat

      select distinct( i.article ) from (select d.article, d.id from ( select b.article from bios b left join (select * from whatlinks where against='Plantilla:Autoritat') as   w on b.article=w.article where w.against is null ) as l left join wikidata d on d.article=l.article ) as i left join authorities a on i.id=a.id where a.authority is not null  order by article asc;

* Pàgines sense plantilla Autoritat amb entrades de Wikidata i sense recursos d'autoritat

      select distinct( i.article ), i.id from (select d.article, d.id from ( select b.article from bios b left join (select * from whatlinks where
      against='Plantilla:Autoritat') as   w on b.article=w.article where w.against is null ) as l left join wikidata d on d.article=l.article ) as i
      where i.id not in ( select distinct(id) from authorities ) order by i.article ASC;

* Pàgines amb plantilla Autoritat amb entrades de Wikidata i sense recursos d'autoritat

      select distinct( i.article ), i.id from (select d.article, d.id from ( select b.article from bios b left join (select * from whatlinks where
      against='Plantilla:Autoritat') as   w on b.article=w.article where w.against is not null ) as l left join wikidata d on d.article=l.article ) as i
      where i.id not in ( select distinct(id) from authorities ) order by i.article ASC;

* Pàgines sense plantilla Autoritat amb entrades de Wikidata amb només certs recursos i no d'altres (p. ex., VIAF i ORCID)

        select distinct( i.article ) from (select d.article, d.id from ( select b.article from bios b left join (select * from whatlinks where against='Plantilla:Autoritat') as   w on b.article=w.article where w.against is null ) as l left join wikidata d on d.article=l.article ) as i left join
        (
        select a.id, a.authority from authorities a where a.id not in  ( select distinct( s.id ) from  ( (
        select distinct(id), group_concat(distinct(authority) order by authority asc) as groups from authorities group by id having groups = "P214,P496"
        UNION
        select distinct(id), group_concat(distinct(authority)) as groups from authorities group by id having count(id) = 1 and groups in ("P214", "P496")  ) as s ) order by s.id asc )
        ) p
        on i.id=p.id where p.authority is not null  order by i.article asc;

## Estadístiques

PER FER

* Nombre de pàgines amb registres d'autoritat respecte a les totals
* Nombre de pàgines amb o sense plantilla autoritat i amb referències o no de registres d'autoritat
* Nombre total d'usos en pàgina d'autoritats per cada autoritat ( x Autoritat, y nombre de pàgines )
* Nombre d'usos en pàgina d'autoritats per total d'usos per pàgina ( x nombre d'autoritats, y nombre de pàgines )
* En pàgines utilitzades nombre d'usos totals d'autoritat respecte a altres autoritats ( x Autoritat, y mitja-desviació )
etc.
