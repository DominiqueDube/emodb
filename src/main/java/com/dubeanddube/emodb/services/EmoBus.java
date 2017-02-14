package com.dubeanddube.emodb.services;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Bundles RESTful methods to access EmoDB's databus.
 *
 * @author Dominique Dube
 */
public class EmoBus {

    // Remark: there is lots of potential to extract more generic code portions in this class.

    private static final String BUS_SUBSCRIPTION_NAME = "items-subscription";

    public static boolean acknowledgeEvents(String eventString) {

        String url = "http://localhost:8080/bus/1/" + BUS_SUBSCRIPTION_NAME + "/ack";

        CloseableHttpClient client = HttpClientBuilder.create().build();
        CloseableHttpResponse response = null;

        HttpPost request = new HttpPost(url); // POST for acknowledgements

        HttpUtils.addApiHeader(request);
        request.addHeader("content-type", "application/json");

        StringEntity params;
        try {
            params = new StringEntity(eventString); // the events
        } catch (UnsupportedEncodingException e) {
            return false;
        }
        request.setEntity(params);

        try {

            response = client.execute(request);
            HttpEntity entity = response.getEntity();
            String responseString = EntityUtils.toString(entity);

            return EmoGen.isSuccess(responseString);

        } catch (IOException e) {
            return false;
        } finally {
            HttpUtils.cleanup(response, client);
        }
    }

    /**
     * Polls up to 5 pending events from the databus.
     *
     * @return a JSON string containing up to 5 pending events, <code>null</code> if the request
     *         was unsuccessful for some reason.
     */
    public static String pollPendingEvents() {

        String url = "http://localhost:8080/bus/1/" + BUS_SUBSCRIPTION_NAME + "/poll?ttl=10&limit=5";

        return HttpUtils.get(url);
    }

    /**
     * Returns the approximate number of unacknowledged pending events for the items table subscription.
     * This number is approximate and may be higher or lower than the actual number.
     * Consult the API documentation for more details.
     *
     * @return the approximate number of unacknowledged pending events on the databus.
     *         Returns -1 if the call fails for some reason.
     */
    public static int getNumPendingEvents() {

        String url = "http://localhost:8080/bus/1/" + BUS_SUBSCRIPTION_NAME + "/size?limit=10";
        String result = HttpUtils.get(url);
        return NumberUtils.toInt(result, -1);
    }

    /**
     * Unsubscribes from changes to the SoR items table.
     *
     * @return <code>true</code> if unsubscribing was successful, <code>false</code> otherwise.
     */
    public static boolean unsubscribe() {

        String url = "http://localhost:8080/bus/1/" + BUS_SUBSCRIPTION_NAME;

        CloseableHttpClient client = HttpClientBuilder.create().build();
        CloseableHttpResponse response = null;

        HttpDelete request = new HttpDelete(url); // DELETE for unsubscribe

        HttpUtils.addApiHeader(request);

        try {

            response = client.execute(request);
            HttpEntity entity = response.getEntity();
            String responseString = EntityUtils.toString(entity);

            return EmoGen.isSuccess(responseString);

        } catch (IOException e) {
            return false;
        } finally {
            HttpUtils.cleanup(response, client);
        }
    }

    /**
     * Subscribes to any change in the SoR items table.
     *
     * @return <code>true</code> if subscribing was successful, <code>false</code> otherwise.
     */
    public static boolean subscribe() {

        String url = "http://localhost:8080/bus/1/" + BUS_SUBSCRIPTION_NAME;

        String dataBinary = "intrinsic(\"~table\":\"items\")";

        CloseableHttpClient client = HttpClientBuilder.create().build();
        CloseableHttpResponse response = null;

        HttpPut request = new HttpPut(url); // PUT for subscribe

        HttpUtils.addApiHeader(request);
        request.addHeader("content-type", "application/x.json-condition");

        StringEntity params;
        try {
            params = new StringEntity(dataBinary);
        } catch (UnsupportedEncodingException e) {
            return false;
        }
        request.setEntity(params);

        try {
            response = client.execute(request);
            HttpEntity entity = response.getEntity();
            String responseString = EntityUtils.toString(entity);

            return EmoGen.isSuccess(responseString);

        } catch (IOException e) {
            return false;
        } finally {
            HttpUtils.cleanup(response, client);
        }
    }
}
