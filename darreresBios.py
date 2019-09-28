import requests
import pandas as pd
import io

# Import JSON configuration

query = """
SELECT ?item ?itemLabel ?article WHERE {
  ?item wdt:P31 wd:Q5.
  ?item wdt:P21 wd:Q6581072 .
  ?article schema:about ?item .
  ?article schema:isPartOf <https://ca.wikipedia.org/> .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],ca" } .
} ORDER BY ?article
"""

headers = {
	'Accept': 'text/csv',
	'User-Agent': 'darreresBio/0.1.0 (https://github.com/WikimediaCAT/wikidata-pylisting; toniher@wikimedia.cat) Python/3.7',
}
params = { 'query':  query }
response = requests.get('https://query.wikidata.org/sparql', headers=headers, params=params)

c=pd.read_csv(io.StringIO(response.content.decode('utf-8')))

print( c )
