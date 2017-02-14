package com.dubeanddube.emodb.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.BooleanUtils;

import java.io.IOException;

/**
 * Generic methods for communication with EmoDB.
 *
 * @author Dominique Dube
 */
public class EmoGen {

    /**
     * Checks and returns whether the EmoSor service is up and running.
     *
     * This method sends a ping to EmoSor and if no "pong" is received, EmoSor is assumed to be down.
     * For simplicity, this method does not distinguish the specific reason for not receiving a pong.
     * That is, EmoSor may still be up but some other reason (network, etc.) may prevent access to it.
     *
     * @return <code>true</code> if EmoSor is up and reachable, <code>false</code> otherwise.
     */
    public static boolean isUp() {

        String url = "http://localhost:8081/ping";

        String responseString = HttpUtils.get(url);

        return responseString != null && responseString.startsWith("pong");
    }

    /**
     * Checks and returns whether the running EmoSor is healthy.\
     *
     * Each of the five components (blob-cassandra, databus-cassandra, deadlocks, queue-cassandra,
     * sor-cassandra) are individually checked via the healthcheck REST call.
     *
     * @return <code>true</code> if all five EmoSor components report healthy, <code>false</code> otherwise.
     */
    public static boolean isHealthy() {

        String url = "http://localhost:8081/healthcheck";

        String responseString = HttpUtils.get(url);

        ObjectMapper mapper = new ObjectMapper();

        JsonNode root;
        try {
            root = mapper.readTree(responseString);
        } catch (IOException e) {
            return false;
        }

        JsonNode blobNode = root.path("blob-cassandra").path("healthy");
        JsonNode databusNode = root.path("databus-cassandra").path("healthy");
        JsonNode deadlocksNode = root.path("deadlocks").path("healthy");
        JsonNode queueNode = root.path("queue-cassandra").path("healthy");
        JsonNode sorNode = root.path("sor-cassandra").path("healthy");

        return ! blobNode.isMissingNode() && BooleanUtils.toBoolean(blobNode.asText()) &&
                ! databusNode.isMissingNode() && BooleanUtils.toBoolean(databusNode.asText()) &&
                ! deadlocksNode.isMissingNode() && BooleanUtils.toBoolean(deadlocksNode.asText()) &&
                ! queueNode.isMissingNode() && BooleanUtils.toBoolean(queueNode.asText()) &&
                ! sorNode.isMissingNode() && BooleanUtils.toBoolean(sorNode.asText());
    }

    /**
     * Checks whether the response from EmoSor is the standard success response and whether
     * that response indicates success.
     *
     * @param responseString the response from EmoSor that is to be checked.
     * @return <code>true</code> if the response could be identified as a standard success
     *         response, <code>false</code> otherwise.
     */
    static boolean isSuccess(String responseString) {

        ObjectMapper mapper = new ObjectMapper();

        JsonNode root;
        try {
            root = mapper.readTree(responseString);
        } catch (IOException e) {
            return false;
        }

        JsonNode successNode = root.path("success");

        return ! successNode.isMissingNode() && BooleanUtils.toBoolean(successNode.asText());
    }
}
