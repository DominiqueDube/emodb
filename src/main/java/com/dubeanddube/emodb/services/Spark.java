package com.dubeanddube.emodb.services;

import com.dubeanddube.emodb.data.JsonUtils;
import com.dubeanddube.emodb.search.SearchIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static spark.Spark.get;

/**
 * Sets up the SPARK route for retrieving queries to the index structure (milestone 1).
 *
 * @author Dominique Dube
 */
public class Spark {

    private final Logger logger = LoggerFactory.getLogger(Spark.class); // some basic logging

    private static final int SPARK_PORT = 4567;

    /**
     * Sets up a SPARK micro framework service with embedded Jetty listening on port 4567.
     * Provides a RESTful interface to access the querying capabilities of this demonstrator.
     *
     * Example for retrieving a single document by ID:
     *
     * curl "http://localhost:4567/document?id=7b8d8a82-77b6-4940-95fe-50ed99b23cb2"
     *
     * Example for retrieving all documents with a speciifc color:
     *
     * curl "http://localhost:4567/document?color=green"
     *
     * @param searchIndex the search index instance.
     */
    public Spark(final SearchIndex searchIndex) {

        logger.info("setting up SPARK route /document");

        get("/ping", (request, response) -> "pong");

        get("/document", (request, response) -> {

            String idParam = request.queryParams("id");
            String colorParam = request.queryParams("color");

            String jsonString;

            if (idParam != null) {

                if (colorParam != null) logger.debug("ignoring color parameter");
                jsonString = searchIndex.getDocumentById(idParam);

            } else if (colorParam != null) {

                jsonString = searchIndex.getDocumentsByColor(colorParam);

            } else {

                jsonString = JsonUtils.NO_SUCCESS;
            }

            response.type("application/json");
            return jsonString;
        });
    }

    /**
     * Checks whether Spark is up and running.
     *
     * @return <code>true</code> if Spark is up and running, <code>false</code> otherwise.
     */
    public boolean isUp() {

        String url = "http://localhost:" + SPARK_PORT + "/ping";

        String responseString = HttpUtils.get(url);

        return responseString != null && responseString.startsWith("pong");
    }
}
