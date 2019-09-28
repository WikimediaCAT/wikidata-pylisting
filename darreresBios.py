query = """
SELECT ?item ?itemLabel 
WHERE 
{
  ?item wdt:P31 wd:Q146.
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
} limit 10
"""


import requests
import pandas as pd
import io

headers = {
    'Accept': 'text/tab-separated-values',
}
params = { 'query':  query }
response = requests.get('https://query.wikidata.org/sparql', headers=headers, params=params)

c=pd.read_csv(io.StringIO(response.content.decode('utf-8')))

print( c )
