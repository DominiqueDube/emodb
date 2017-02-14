package com.dubeanddube.emodb.search;

import com.dubeanddube.emodb.data.*;
import com.dubeanddube.emodb.services.HttpUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements the index based on a local Elasticsearch instance.
 * Assumes that the Elasticsearch instance is running on localhost:9200.
 *
 * @author Dominique Dube
 */
public class ElasticIndex implements ItemIndex {

    private static final Logger staticLogger =
            LoggerFactory.getLogger(ElasticIndex.class); // some basic logging

    private final Logger logger = LoggerFactory.getLogger(ElasticIndex.class); // some basic logging

    /**
     * Checks and returns whether the EmoSor service is up and running.
     *
     * This method attempts to read from http://localhost:9200/ where the local instance
     * of Elasticsearch is expected to run. At least the <code>name</code> and
     * <code>cluster_name</code> JSON fields must be found for the call to succeed.
     *
     * @return <code>true</code> if Elasticsearch is up and reachable, <code>false</code> otherwise.
     */
    static boolean isUp() {

        String url = "http://localhost:9200/";

        String responseString = HttpUtils.get(url);

        ObjectMapper mapper = new ObjectMapper();

        JsonNode root;
        try {
            root = mapper.readTree(responseString);
        } catch (IOException e) {
            staticLogger.debug("could not retrieve Elasticsearch description from localhost:9200");
            return false;
        }

        JsonNode nameNode = root.path("name");
        JsonNode clusterNameNode = root.path("cluster_name");

        if (nameNode.isMissingNode() || clusterNameNode.isMissingNode()) {
            staticLogger.debug("Could not retrieve Elasticsearch name and cluster_name from localhost:9200");
            return false;
        }

        staticLogger.debug("Detected Elasticsearch (name: " + nameNode.asText() +
                ", cluster_name = " + clusterNameNode.asText() + ") instance on localhost:9200 - good!");

        return true;
    }

    // omitting default constructor

    /**
     * Deletes an existing Elasticsearch items index to start with a fresh index.
     *
     * @return <code>true</code> if the items index was successfully deleted,
     *         <code>false</code> otherwise.
     */
    static boolean deleteIndex() {

        String url = "http://localhost:9200/items";

        CloseableHttpClient client = HttpClientBuilder.create().build();
        CloseableHttpResponse response = null;

        HttpDelete request = new HttpDelete(url); // DELETE for drop table

        try {

            response = client.execute(request);
            HttpEntity entity = response.getEntity();
            String responseString = EntityUtils.toString(entity);

            staticLogger.debug("Elasticsearch delete response = " + responseString);

            ObjectMapper mapper = new ObjectMapper();

            JsonNode root;
            try {
                root = mapper.readTree(responseString);
            } catch (IOException e) {
                staticLogger.debug("could not retrieve Elasticsearch delete response from localhost:9200");
                return false;
            }

            JsonNode ackNode = root.path("acknowledged");

            return ! ackNode.isMissingNode() && BooleanUtils.toBoolean(ackNode.asText());

        } catch (IOException e) {
            return false;
        } finally {
            HttpUtils.cleanup(response, client);
        }
    }

    /**
     * @see ItemIndex#getDocumentById(String)
     */
    @Override
    public String getDocumentById(String id) {

        String url = "http://localhost:9200/items/item/" + id;

        String responseString = HttpUtils.get(url);

        logger.debug("Elasticsearch get document response = " + responseString);

        boolean found = false;

        ObjectMapper mapper = new ObjectMapper();

        JsonNode root = null;

        try {

            root = mapper.readTree(responseString);

            JsonNode foundNode = root.path("found");

            if (! foundNode.isMissingNode()) {
                found = BooleanUtils.toBoolean(foundNode.asText());
            }

        } catch (IOException e) {
            staticLogger.debug("could not retrieve Elasticsearch description from localhost:9200");
        }

        staticLogger.debug("object with ID " + id + (found ? "" : " not") + " found");

        JsonResult result;

        if (! found) {

            result = new MessageResult();
            ((MessageResult)result).success = true;
            ((MessageResult)result).payload = "no match found";

        } else {

            // for simplicity, additional error handling omitted

            JsonNode colorNode = root.path("_source").path("color");
            JsonNode textNode = root.path("_source").path("text");

            Item item = new Item(colorNode.asText(), textNode.asText());

            result = new ItemResult();

            ((ItemResult)result).success = true;
            ((ItemResult)result).payload = item;
        }

        try {
            return mapper.writeValueAsString(result);
        } catch (IOException e) {
            return JsonUtils.NO_SUCCESS;
        }
    }

    /**
     * @see ItemIndex#getDocumentsByColor(String)
     */
    @Override
    public String getDocumentsByColor(String color) {

        String url = "http://localhost:9200/items/item/_search?size=100";

        String dataBinary = "{\"query\":{\"query_string\":{\"query\":\"" + color
                + "\",\"fields\":[\"color\"]}}}";

        CloseableHttpClient client = HttpClientBuilder.create().build();
        CloseableHttpResponse response = null;

        HttpPost request = new HttpPost(url); // POST for create/modify document

        StringEntity params;
        try {
            params = new StringEntity(dataBinary);
        } catch (UnsupportedEncodingException e) {
            return JsonUtils.NO_SUCCESS;
        }
        request.setEntity(params);

        String responseString;

        try {
            response = client.execute(request);
            HttpEntity entity = response.getEntity();
            responseString = EntityUtils.toString(entity);

        } catch (IOException e) {
            return JsonUtils.NO_SUCCESS;
        } finally {
            HttpUtils.cleanup(response, client);
        }

        ObjectMapper mapper = new ObjectMapper();

        JsonNode root;

        try {
            root = mapper.readTree(responseString);
        } catch (IOException e) {
            return JsonUtils.NO_SUCCESS;
        }

        JsonNode hitsArray = root.path("hits").path("hits");

        if (! hitsArray.isArray()) return JsonUtils.NO_SUCCESS;

        List<Item> matchingItems = new ArrayList<>();

        for (JsonNode objectNode : hitsArray) {

            JsonNode colorNode = objectNode.path("_source").path("color");
            JsonNode textNode = objectNode.path("_source").path("text");

            Item item = new Item(colorNode.asText(), textNode.asText());

            matchingItems.add(item);
        }

        return ItemIndex.serializeItems(matchingItems);
    }

    /**
     * @see ItemIndex#updateDocument(VersionedIDItem)
     *
     * This method uses the external version feature of Elasticseach to implement OCC.
     * The external version is the intrinsic version of EmoDB.
     */
    @Override
    public boolean updateDocument(VersionedIDItem newItem) {

        String url = "http://localhost:9200/items/item/" + newItem.getId() +
                "?version=" + newItem.getVersion() + "&version_type=external";

        String dataBinary = "{\"color\":\"" + newItem.getItem().getColor() +
                "\",\"text\":\"" + newItem.getItem().getText() + "\"}";

        CloseableHttpClient client = HttpClientBuilder.create().build();
        CloseableHttpResponse response = null;

        HttpPut request = new HttpPut(url); // PUT for create table

        StringEntity params;
        try {
            params = new StringEntity(dataBinary);
        } catch (UnsupportedEncodingException e) {
            return false;
        }
        request.setEntity(params);

        String responseString;

        try {

            response = client.execute(request);
            HttpEntity entity = response.getEntity();
            responseString = EntityUtils.toString(entity);

            logger.debug("Elasticsearch update document replied with " + responseString);

        } catch (IOException e) {

            logger.debug("no valid reply from Elasticsearch during update document");

            return false;

        } finally {
            HttpUtils.cleanup(response, client);
        }

        ObjectMapper mapper = new ObjectMapper();

        JsonNode root;

        try {
            root = mapper.readTree(responseString);
        } catch (IOException e) {
            logger.debug("could not retrieve Elasticsearch description from localhost:9200");
            return false;
        }

        JsonNode resultNode = root.path("result");

        if (! resultNode.isMissingNode()) {

            if (resultNode.asText().equals("created")) {

                logger.debug("created Elasticsearch document " + newItem.getId());
                return true;

            } else if (resultNode.asText().equals("updated")) {

                logger.debug("updated Elasticsearch document" + newItem.getId());
                return true;

            } else {

                logger.debug("unrecognized result string (neither 'created' nor 'updated') for document "
                        + newItem.getId());

                return false;
            }

        } else {

            logger.debug("unable to locate result node in Elasticsearch update document reply");

            return false;
        }
    }
}
