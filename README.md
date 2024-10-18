# Conduit Connector Elasticsearch

## General
The Elasticsearch plugin is one of [Conduit](https://github.com/ConduitIO/conduit) plugins.
It currently provides only destination Elasticsearch connector, allowing for using it as a destination in a Conduit pipeline.

## How to build it
Run `make`.

# Destination

The Destination connector stores data in given index.
When Record has Key value set, then it is used as a Document ID.
Moreover, when Record has `action` entry in the Metadata, then action specified there is respected. Supported actions:
- `insert` when Record.Key is missing: stores a new Document without ID.
- `update`: stores or updates (upsert) a Document with ID. Default case when `action` is not set but Record.Key is set.
- `delete`: deletes a Document by its Record.Key.

For any other action a warning entry is added to log and Record is skipped.

## Configuration Options

| name                     | description                                                                                                                                                                                                                                      | required                                             | default  |
|--------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|----------|
| `version`                | The version of the Elasticsearch service. One of: `5`, `6`, `7`, `8`.                                                                                                                                                                            | `true`                                               |          |
| `host`                   | The Elasticsearch host and port (e.g.: http://127.0.0.1:9200).                                                                                                                                                                                   | `true`                                               |          |
| `username`               | [v: 5, 6, 7, 8] The username for HTTP Basic Authentication.                                                                                                                                                                                      | `false`                                              |          |
| `password`               | [v: 5, 6, 7, 8] The password for HTTP Basic Authentication.                                                                                                                                                                                      | `true` when username was provided, `false` otherwise |          |
| `cloudId`                | [v: 6, 7, 8] Endpoint for the Elastic Service (https://elastic.co/cloud).                                                                                                                                                                        | `false`                                              |          |
| `apiKey`                 | [v: 6, 7, 8] Base64-encoded token for authorization; if set, overrides username/password and service token.                                                                                                                                      | `false`                                              |          |
| `serviceToken`           | [v: 7, 8] Service token for authorization; if set, overrides username/password.                                                                                                                                                                  | `false`                                              |          |
| `certificateFingerprint` | [v: 7, 8] SHA256 hex fingerprint given by Elasticsearch on first launch.                                                                                                                                                                         | `false`                                              |          |
| `index`                  | The name of the index to write the data to.                                                                                                                                                                                                      | `true`                                               |          |
| `type`                   | [v: 5, 6] The name of the index's type to write the data to.                                                                                                                                                                                     | `true` for versions: `5` and `6`, `false` otherwise  |          |
| `bulkSize`               | The number of items stored in bulk in the index. The minimum value is `1`, maximum value is `10000`. Note that values greater than `1000` may require additional service configuration.                                                          | `true`                                               | `"1000"` |
| `retries`                | The maximum number of retries of failed operations. The minimum value is `0` which disabled retry logic. The maximum value is `255`. Note that the higher value, the longer it may take to process retries, as a result, ingest next operations. | `true`                                               | `"1000"` |


# Source
ElasticSearch source connector allows you to move data from multiple Elasticsearch indexes with the specified `host` and `indexes`. It uses elasticsearch search api to pull data from indexes. Upon starting it pulls batches of data from indexes, once all the data is retrieved, it then polls the search api to pull data at regular intervals. 

## Configuration Options
| name                     | description                                                                                                                                                                                                                                      | required                                             | default  |
|--------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|----------|
| `version`                | The version of the Elasticsearch service. One of: `5`, `6`, `7`, `8`.                                                                                                                                                                            | `true`                                               |          |
| `host`                   | The Elasticsearch host and port (e.g.: http://127.0.0.1:9200).                                                                                                                                                                                   | `true`                                               |          |
| `username`               | [v: 5, 6, 7, 8] The username for HTTP Basic Authentication.                                                                                                                                                                                      | `false`                                              |          |
| `password`               | [v: 5, 6, 7, 8] The password for HTTP Basic Authentication.                                                                                                                                                                                      | `true` when username was provided, `false` otherwise |          |
| `cloudId`                | [v: 6, 7, 8] Endpoint for the Elastic Service (https://elastic.co/cloud).                                                                                                                                                                        | `false`                                              |          |
| `apiKey`                 | [v: 6, 7, 8] Base64-encoded token for authorization; if set, overrides username/password and service token.                                                                                                                                      | `false`                                              |          |
| `serviceToken`           | [v: 7, 8] Service token for authorization; if set, overrides username/password.                                                                                                                                                                  | `false`                                              |          |
| `certificateFingerprint` | [v: 7, 8] SHA256 hex fingerprint given by Elasticsearch on first launch.                                                                                                                                                                         | `false`                                              |          |
| `indexes`                  | The name of the indexes to read the data from.                                                                                                                                                                                                      | `true`                                               |          |
| `indexes.*.sortBy`                  | The sortby field for each index to be used by elasticsearch search api.                                                                                                                                                                                                      | `true`                                               |          |
| `indexes.*.sortOrder`                  | The sortOrder (asc or desc) for each index to be used by elasticsearch search api.                                                                                                                                                                                                      | `true`                                               |          |
| `batchSize`               | The number of items to fetch from an index. The minimum value is `1`, maximum value is `10000`.                                                          | `false`                                               | `"1000"` |
| `pollingPeriod`                | The duration for polling the search api for fetching new records. | `false`                                               | `"5s"` |


# Testing

Run `make test` to run all the unit and integration tests, which require Docker to be installed and running. The command will handle starting and stopping docker containers for you.

# Local development

If you like to monitor Elasticsearch index with visual editor, there is [Kibana](https://www.elastic.co/kibana/) configuration created for local development that you can run with Elasticsearch Docker container.

```shell
docker-compose -f test/docker-compose.v8.overrides.yml -f test/docker-compose.v8.yml -p test-v8 up -d
```

Don't forget to include it when finishing your work:

```shell
docker-compose -f test/docker-compose.v8.overrides.yml -f test/docker-compose.v8.yml down
```

# References

- https://github.com/elastic/go-elasticsearch
- https://www.elastic.co/guide/en/elasticsearch/reference/7.17/docs-bulk.html
- https://www.elastic.co/guide/en/elasticsearch/reference/current/search-your-data.html

![scarf pixel](https://static.scarf.sh/a.png?x-pxid=715ebf4a-148c-44ad-8f64-6cc5780d34ae)
