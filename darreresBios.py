import requests
import pandas as pd
import numpy as np
import io
from urllib.parse import unquote

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

c['article'] = c['article'].apply(lambda x: unquote( x.replace("https://ca.wikipedia.org/wiki/", "") ) )
c['item'] = c['item'].apply( lambda x: x.replace("http://www.wikidata.org/entity/", "") )

creation_date = np.zeros( ( len(c) ), dtype='datetime64' )

for g, df in c.groupby(np.arange(len(c)) // 50):
	titles = df['article'].str.cat(sep="|")


print( c )

