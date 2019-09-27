from SPARQLWrapper import SPARQLWrapper, TSV


sparql = SPARQLWrapper("https://query.wikidata.org/sparql?query=")
sparql.setQuery("""
SELECT ?item ?itemLabel 
WHERE 
{
  ?item wdt:P31 wd:Q146.
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
""")
sparql.setReturnFormat(TSV)
results = sparql.query().convert()


print( results )