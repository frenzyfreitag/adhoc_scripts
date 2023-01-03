import datetime

import elasticsearch
from opensearchpy import OpenSearch

host = "https://<username>:<pass>@<rdoproj_opensearch_url>:443/elasticsearch/"

es = OpenSearch(host)

# Create an index with non-default settings.
index_fmt = 'subunit-rdoproject_org-%Y.%m.%d'
days = 7


def is_valid_index(es, index):
    try:
        es.search(index=index)
        return True
    except elasticsearch.exceptions.NotFoundError:
        return False

# today's index
datefmt = index_fmt
now = datetime.datetime.now(tz=datetime.timezone.utc)
indexes = []
latest_index = now.strftime(datefmt)
if is_valid_index(es, latest_index):
    indexes.append(latest_index)
for day in range(1, days):
    lastday = now - datetime.timedelta(days=day)
    index_name = lastday.strftime(datefmt)
    if is_valid_index(es, index_name):
        indexes.append(index_name)

## Need to refine this query
query = {"query": {
    "bool": {
      "must": [],
      "filter": [
        {
          "match_all": {}
        },
        {
          "range": {
            "@timestamp": {
              "gte": "2022-12-27T11:28:39.671Z",
              "lte": "2023-01-03T11:28:39.671Z",
              "format": "strict_date_optional_time"
            }
          }
        }
      ],
      "should": [],
      "must_not": []
    }
}}


results = es.search(index=indexes, body=query)
print(results)

