# EmoDB Prototype Application

### Prerequisites

The project will require the following services running on the local host in order to be fully functional:

* EmoDB must be running on `localhost:8080`, otherwise the prototype will abort.
* Elasticsearch must be running on `localhost:9200` in order to use the Elasticsearch index (if it cannot be detected, the memory-based OCC hash map will be used for indexing instead)

### Installation

Clone GitHub repository:

```
git clone https://github.com/DominiqueDube/emodb.git
```

Run Maven:

```
mvn clean install
```
### Running and Stopping

Execute:

```
mvn exec:java
```
If you want to use the memory-based OCC hash map, simply do not run Elasticsearch on the local host.

Simply stop the running protoype with Ctrl-C.

### Basic Operation

When starting up, the prototype will attempt to delete any previously created EmoDB system of record table and subscription to that table. If Elasticsearch is used for indexing, the prototype will also attempt to delete any previously created index for data items. This way, the prototype can be executed multiple times without having to do a clean restart of the other services.

The prototype will then populate the system of record with an initial set of documents. These documents will also be stored in the used index (Elasticsearch or OCC hash map). The RESTful interface to query the index is then brought up (see next section).

In the final phase a subscription to the EmoDB databus is created to observe document changes, and a separate thread creates further document updates with a frequency of 1 second. The EmoDB databus is scanned for unacknowledged updates every 2 seconds and will process these updates accordingly, in turn updating the index. This can, for example, easily be observed by repeated queries to the same color and observe the number of retrieved documents change with time.

### Querying Documents

The prototype supports the querying of documents from the used index (either Elasticsearch or OCC hash map). The querying interface is RESTful and running on Jetty at `localhost:4567` (the port must not be in use, otherwise, the querying interface will not be available while the prototype will continue to run).

Note that when starting up the prototype, it will take some time for the querying interface to come up as the index is first populated with an initial set of documents.

You can check the status of the RESTful interface as follows:

```
curl "http://localhost:4567/ping"
```

The service will return `pong` if it is running and reachable.


Two types of queries are supported:

* get a document by ID from the index
* list all indexed documents with a certain color

To get a document by ID:

```
curl "http://localhost:4567/document?id=<id>"
```
The IDs of stored documents are in UUID format.

Example query:

```
curl "http://localhost:4567/document?id=7b8d8a82-77b6-4940-95fe-50ed99b23cb2" | jq .
```

The result is a JSON string containing the document.

Example result of a successful query:

```
{
 	"success": true,
 	"payload": {
  		"color": "blue",
    	"text": "Netus vitae."
  }
}
```

If a query is successful but the document is not found, the following JSON string is returned:

```
{
	"success": true,
    "payload": "no match found"
}
```

To list all documents with a certain color:

```
curl "http://localhost:4567/document?color=<color>"
```

Example query:

```
curl "http://localhost:4567/document?color=green" | jq .
```

The result is a JSON string containing an array of all the documents that match the color.

Example result of a successful query:

```
{
 	"success": true,
 	"size": 2,
    "payload": {
    	[
        	{
            	"color": "green",
    			"text": "Vitae fames."
  			},
    		{
            	"color": "green",
    			"text": "Lacus augue vitae dis orci natoque nonummy."
  			},
        ]
    }
}
```

If no documents are found with the color specified an empty array is returned (with size = 0).

If a query is not successful, the following JSON string is returned:

```
{
	"success": false
}
```

If you are running Elasticsearch locally, you can also query the Elasticsearch index directly and observe available documents as the number of documents in the index change with time.

For example, use the following to query for a document by its ID:

```
curl "http://localhost:9200/items/item/7b8d8a82-77b6-4940-95fe-50ed99b23cb2"
```

You can use the following to query for documents in a specific color:

```
curl -XPOST "http://localhost:9200/items/item/_search" \
	-d "{\"query\":{\"query_string\":{\"query\":\"blue\",\"fields\":[\"color\"]}}}" | jq.
```