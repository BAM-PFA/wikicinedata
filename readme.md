# WikiCineData

This is a script intended to aid in reconciling film title records in [CollectionSpace](https://collectionspace.org/) with data from [Wikidata](https://www.wikidata.org). It comes from a specific use case (just getting film data for reuse in CollectionSpace), but ideally this would be applicable to CSV input too, or other data sources. Since the Wikidata reconciliation [service](https://wikidata.reconci.link/), which was originally written for [OpenRefine](http://openrefine.org/), follows a proposed "reconciliation [API](https://reconciliation-api.github.io/specs/latest/)" spec, it might also be extensible to other reconciliation services/endpoints.

## Usage

`python3 driver.py -m cspace`

After setting up the config file (see below) you should be able to run the `driver.py` script as above and let it rip. Depending on the size of the data set it might take a long time!

## Background/details

Because there are so many variables in any give use case, there is a `config.json` file that allows you to specify things like which authority in CSpace you want to search (works, people, objects, etc.), along with the unique ID for that authority in your system; what item "type" you want to reconcile against in Wikidata ("films/Q11424","humans/Q5","paintings/Q3305213",etc.). It also allows you to fine tune how the script runs queries in parallel (more shortly).

In order to make the script more generalizable, it might be worth trying to make other elements customizable from the config file, like what columns the internal sqlite3 database uses, or other potentially nit-picky details.

There is also a `secrets.json` file that I'm using to store CSpace credentials.

You can look at the existing `config.json` file as well as the `sample_secrets.json` to see the expected format.

## Parallel queries

Since the main time consumer is data I/O during API calls, the Wikidata queries are set to run in parallel threads, with batch sizes set by the `chunk_size` variable in the config file. You can mess with this value to see how large of queries you can get away with :)

There's also a throttle on the number of threads allowed at once. It's currently hard coded into the `Database.chunk_me()` method at 10, but that might be good as a configurable point too. When the threads are unlimited it quickly crashes Python, runs into errors accessing the database file, and other goofy stuff.

Another potential config point would be the number of results returned per page in a CSpace query. Currently hard coded in `cspace_utils.fetch_cspace_items()`.

## Output

The current output is just a sqlite3 database containing one item per row, along with data from the top Wikidata match, if any. It also stores whether the top match is a "100% match" along with the matching score provided by the reconciliation service (the scoring is kind of opaque).

It should be pretty easy to make a CSV output of the sqlite data, but a potentially huge CSV file would be unwieldy. Maybe add another config point to say how many rows you want per CSV file output in a series of batches? Like "2,000 rows per csv" returning 20 individual CSV files or whatever. TBD.

## Reconciliation

One oddity is the way the Wikidata reconciliation service needs the JSON in each API call to be formatted. Here's an example:

```
my_query_dict = {'q1': {'query': 'jaws', 'type': 'Q11424', 'properties': [{'pid': 'P1476', 'v': 'Tiburon'}]}, 'q2': {'query': 'Ernst Schwanhold'}}

query_payload = json.dumps({"queries":json.dumps(my_query_dict)})

requests.post(service_url,data=json.loads(query_payload))
```

The return value for a single positive match looks like this:

```
{'q1':
	{'result': [
		{
			'features': [
				{'id': 'P577', 'value': 100},
				{'id': 'P57', 'value': 100},
				{'id': 'all_labels', 'value': 100}
			],
			'id': 'Q2740695',
			'match': True,
			'name': '10 on Ten',
			'score': 100.0,
			'type': [{'id': 'Q11424', 'name': 'film'}]
			},
		{
			'features': [
				{'id': 'P577', 'value': 0},
				{'id': 'P57', 'value': 38},
				{'id': 'all_labels', 'value': 58}
			],
			'id': 'Q746733',
			'match': False,
			'name': 'The Ten Commandments',
			'score': 40.666666666666664,
			'type': [{'id': 'Q11424', 'name': 'film'}]
		},
		{
			'features': [
				{'id': 'P577', 'value': 0},
				{'id': 'P57', 'value': 46},
				{'id': 'all_labels', 'value': 53}
			],
			'id': 'Q22098970',
			'match': False,
			'name': 'Ten Years',
			'score': 39.66666666666667,
			'type': [{'id': 'Q11424', 'name': 'film'}]
		}
		]
	}
}
```


Null results look like this:

```
{"q2":{
	"result":[]
	}
}
```

I initially tried to include things like alternative titles, multiple creators, etc. in the reconciliation call as additional match points, but it seems that the best match is retrieved by just using one main query point (one "title"), and one optional value per "extra parameter" (one creator, one date, etc.) and Wikidata will do some magic to retrieve options.

## Dependencies

`pip3 install lxml requests`
