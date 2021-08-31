Pulling data from cspace, reconciling w wikidata.

config.json defines:
* which cspace authority to use (if you're querying cspace)
* the cspace id of the authority (i.e. of the authority itself, not any single item)
* how many rows to include in each wikidata reconciliation query

---


notes to self:
wikidata recinciliation service needs the json query to be formatted like so:

my_query_dict = {'q1': {'query': 'jaws', 'type': 'Q11424', 'properties': [{'pid': 'P1476', 'v': 'Tiburon'}]}, 'q2': {'query': 'Ernst Schwanhold'}}

query_payload = json.dumps({"queries":json.dumps(my_query_dict)})

requests.post(service_url,data=json.loads(query_payload))

and it returns this JSON:
'{"q1":{"result":[{"features":[{"id":"P1476","value":0},{"id":"all_labels","value":100}],"id":"Q189505","match":false,"name":"Jaws","score":71.42857142857143,"type":[{"id":"Q11424","name":"film"}]},{"features":[{"id":"P1476","value":17},{"id":"all_labels","value":80}],"id":"Q1199839","match":false,"name":"Jaws: The Revenge","score":62.0,"type":[{"id":"Q11424","name":"film"}]},{"features":[{"id":"P1476","value":0},{"id":"all_labels","value":80}],"id":"Q1199834","match":false,"name":"Jaws 2","score":57.142857142857146,"type":[{"id":"Q11424","name":"film"}]},{"features":[{"id":"P1476","value":0},{"id":"all_labels","value":80}],"id":"Q1199837","match":false,"name":"Jaws 3-D","score":57.142857142857146,"type":[{"id":"Q229390","name":"3D film"},{"id":"Q11424","name":"film"}]},{"features":[{"id":"P1476","value":12},{"id":"all_labels","value":57}],"id":"Q2409672","match":false,"name":"Cruel Jaws","score":44.142857142857146,"type":[{"id":"Q11424","name":"film"}]}]},"q2":{"result":[{"features":[{"id":"all_labels","value":100}],"id":"Q1360170","match":true,"name":"Ernst Schwanhold","score":100.0,"type":[{"id":"Q5","name":"human"}]}]}}'


null results look like this: "q2":{"result":[]}}
