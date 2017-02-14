package com.dubeanddube.emodb;

import com.dubeanddube.emodb.data.IDItem;
import com.dubeanddube.emodb.data.IDItemParser;
import com.dubeanddube.emodb.data.Item;
import com.dubeanddube.emodb.data.VersionedIDItem;
import com.dubeanddube.emodb.services.EmoBus;
import com.dubeanddube.emodb.services.EmoSor;
import com.dubeanddube.emodb.services.EmoGen;
import com.dubeanddube.emodb.services.Spark;
import com.dubeanddube.emodb.search.SearchIndex;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.SimpleLogger;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Executors;

/**
 * Entry point class.
 *
 * @author Dominique Dube
 */
public class App {

    private final Logger logger = LoggerFactory.getLogger(App.class); // some basic logging

    static {
        System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG");
    }

    // TODO: use Elasticsearch as default, the memory hash map will be fall back
    private static final SearchIndex.IndexType INDEX_TYPE = SearchIndex.IndexType.MEMORY_HASH_MAP;

    private SearchIndex searchIndex;

    private static final int SUBSCRIPTION_POLL_TIME_MILLIS = 2000;
    private static final int DOCUMENT_UPDATE_TIME_MILLIS = 1000;

    /**
     * Application entry point.
     *
     * @param args command line arguments. No arguments are defined for this application.
     */
    public static void main( String[] args ) {

        new App().start();
    }

    /**
     * Constructs a new instance of App.
     */
    private App() {
    }

    /**
     * Starts the application: prepares EmoDB's system of records and databus, prepares
     * the search index and real-time monitoring, initializes all services and functions.
     * Upon completion of the initialization phase, establishes a RESTful interface at
     * localhost:4567 for requests to the search index.
     *
     * @see Spark#Spark(SearchIndex) for details on possible requests at localhost:4567.
     */
    private void start() {

        logger.info("Starting application");

        if (! EmoGen.isUp()) {
            logger.error("EmoDB is not available - it must be running on localhost:8081 - aborting");
            return;
        }
        logger.info("Detected running EmoDB instance on localhost:8081 - good!");

        if (! EmoGen.isHealthy()) {
            logger.error("EmoDB seems to be not healthy - aborting");
            return;
        }
        logger.info("EmoDB looks healthy - good!");

        if (! EmoSor.dropTable()) {
            logger.warn("Items table not deleted - EmoDB clean start? - continuing");
        } else {
            logger.info("Items table deleted - starting fresh");
        }

        if (! EmoBus.unsubscribe()) {
            logger.warn("Did not unsubscribe from changes on items table - continuing");
        } else {
            logger.info("Subscription to the items table removed - starting fresh");
        }

        if (! EmoSor.createTable()) {
            logger.error("Failed to create items table in system of records - aborting");
            return;
        }
        logger.info("Successfully created items table");

        if (! populateTable()) {
            logger.warn("Failed to complete populating items table - continuing anyway");
        } else {
            logger.info("Successfully populated items table");
        }

        long tableSize = EmoSor.getTableSize();

        if (tableSize < 0) {
            logger.warn("Failed to read size of items table - continuing");
        } else {
            logger.info("Size of items table: " + tableSize +
                    (tableSize == 100 ? " (just what I expected)" : " (should be 100)"));
        }

        // TODO: ihandle search index creation also when Elasticsearch reverts to memory-based

        logger.info("Setting up search index of type " + INDEX_TYPE + "...");

        searchIndex = new SearchIndex(INDEX_TYPE);

        logger.info("Created search index of index type = " + searchIndex.getIndexType());

        logger.info("Loading sample data into search index...");

        if (! searchIndex.loadSampleData()) {
            logger.warn("Failed to load sample data into search index - continuing anyway");
        } else {
            logger.info("Successfully loaded sample data into search index");
        }

        // ready to start the query service at this point

        Spark spark = new Spark(searchIndex);

        int retries = 10; // wait 10 seconds max
        boolean success = false;

        while (! success && retries > 0) {

            success = spark.isUp();
            --retries;

            logger.info("waiting for Spark to ignite ...");

            try { Thread.sleep(1000); } catch (InterruptedException ignored) { }
        }

        if (! success) {
            logger.error("Spark is not available - it must be running on localhost:4567 - continuing anyway");
        } else {
            logger.info("Detected running Spark instance on localhost:4567 - good!");
        }

        // databus (milestone 2)

        // subscribe to all changes to items table

        if (! EmoBus.subscribe()) {
            logger.warn("failed to subscribe to items table - continuing anyway");
        } else {
            logger.info("successfully subscribed to items table");
        }

        logger.info("initializing subscription listener");
        initListener();

        logger.info("initializing document updates");
        initUpdates();
    }

    /**
     * Populates the items table in the system of records for the first time with the items in milestone0.txt.
     *
     * @return <code>true</code> if all items could be added to the items table in the system of records,
     *         <code>false</code> otherwise.
     */
    private boolean populateTable() {

        URL url = getClass().getResource("/milestone0.txt");

        IDItemParser parser;

        try {
            parser = new IDItemParser(url);
        } catch (IOException e) {
            return false;
        }

        boolean success = true;

        while (true) {

            IDItem nextItem;
            try {
                nextItem = parser.getNext();
            } catch (IOException e) {
                success = false;
                break;
            }

            if (nextItem == null) break; // no more items (end of file)

            // store the next item in the system of records
            if (EmoSor.updateDocument(nextItem, "initial-submission")) {
                logger.debug("added document " + nextItem.getId());
            } else {
                logger.warn("failed adding document " + nextItem.getId() + " - but will continue");
            }
        }

        try {
            parser.close();
        } catch (IOException e) {
            // ignore
        }

        return success;
    }

    /**
     * Initializes the subscription listener and runs it in its own thread.
     */
    private void initListener() {

        Executors.newSingleThreadExecutor().execute(() -> {

        logger.info("starting subscription listener thread");

        // Remark: provide means to terminate application cleanly would be a next step.

            while (true) {

                try {
                    Thread.sleep(SUBSCRIPTION_POLL_TIME_MILLIS);
                } catch (InterruptedException ignored) {
                }

                logger.debug("polling subscription again after " + SUBSCRIPTION_POLL_TIME_MILLIS + " milliseconds");

                int numUnclaimedEvents = EmoBus.getNumPendingEvents(); // just an estimate

                logger.debug("databus reports " + numUnclaimedEvents + " unclaimed events");

                List<String> eventKeysToAck = new ArrayList<>();

                String responseString = EmoBus.pollPendingEvents();

                JsonFactory factory = new JsonFactory();
                ObjectMapper mapper = new ObjectMapper(factory);

                JsonNode rootNode;

                try {
                    rootNode = mapper.readTree(responseString);
                } catch (IOException e) {
                    logger.warn("problem encountered during processing of response string");
                    continue;
                }

                if (! rootNode.isArray()) {
                    logger.warn("expected array of documents during processing of response string");
                    continue;
                }

                for (JsonNode root : rootNode) { // iterate the objects

                    JsonNode eventKeyNode = root.path("eventKey");
                    String eventKey = eventKeyNode.isMissingNode() ? null : eventKeyNode.asText();

                    JsonNode versionNode = root.path("content").path("~version");
                    String version = versionNode.isMissingNode() ? null : versionNode.asText();

                    JsonNode colorNode = root.path("content").path("color");
                    String color = colorNode.isMissingNode() ? null : colorNode.asText();

                    JsonNode textNode = root.path("content").path("text");
                    String text = textNode.isMissingNode() ? null : textNode.asText();

                    JsonNode idNode = root.path("content").path("~id"); // intrinsic field
                    String id = idNode.isMissingNode() ? null : idNode.asText();

                    // some very basic error handling
                    if (eventKeyNode.isMissingNode() || versionNode.isMissingNode() ||
                            colorNode.isMissingNode() || textNode.isMissingNode() || idNode.isMissingNode()) {
                        logger.warn("at least one relevant document node is missing");
                        continue;
                    }

                    Item item = new Item(color, text);

                    int versionInt = NumberUtils.toInt(version, -1);

                    if (versionInt == -1) {
                        logger.warn("encountered illegal version format");
                        continue;
                    }

                    VersionedIDItem idItem = new VersionedIDItem(id, item, versionInt);

                    if (! searchIndex.updateDocument(idItem)) {

                        logger.info("event key " + eventKey +
                                " was not updated in search index (possibly outdated)");
                    }

                    // document update was processed, add to ACK list
                    eventKeysToAck.add(eventKey);
                }

                // acknowledge all processed document updates

                String eventsString;

                try {
                    eventsString = mapper.writeValueAsString(eventKeysToAck);
                } catch (JsonProcessingException e) {
                    logger.warn("failed to write event array to JSON string");
                    continue;
                }

                if (EmoBus.acknowledgeEvents(eventsString)) {
                    logger.info("successfully acknowledged array of document updates (" + eventsString + ")");
                } else {
                    logger.warn("failed to acknowledge array of document updates (" + eventsString + ")");
                }
            }
        });
    }

    /**
     * Initializes the document updater and runs it in its own thread.
     */
    private void initUpdates() {

        Executors.newSingleThreadExecutor().execute(new Runnable() {

            @Override
            public void run() {

                logger.info("starting document updater thread");

                URL url = getClass().getResource("/milestone2.txt");

                IDItemParser parser;

                try {
                    parser = new IDItemParser(url);
                } catch (IOException e) {

                    logger.warn("failed to create item parser in document updater thread");
                    return;
                }

                while (true) {

                    try {
                        Thread.sleep(DOCUMENT_UPDATE_TIME_MILLIS);
                    } catch (InterruptedException ignored) { }

                    IDItem nextItem;
                    try {
                        nextItem = parser.getNext();
                    } catch (IOException e) {

                        logger.warn("parser error - terminating document update thread");
                        break;
                    }

                    if (nextItem == null) {

                        logger.info("no more document updates to parse");
                        break;
                    }

                    if (EmoSor.updateDocument(nextItem, "document-update")) {
                        logger.debug("updated document " + nextItem.getId() + " in SoR");
                    } else {
                        logger.warn("failed to update document " + nextItem.getId() + " in SoR");
                    }
                }

                try {
                    parser.close();
                } catch (IOException ignore) {
                }
            }
        });
    }
}
