package com.dubeanddube.emodb.services;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.AbstractHttpMessage;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

/**
 * Bundles some generic HTTP-related functions for convenience.
 *
 * @author Dominique Dube
 */
class HttpUtils {

    /**
     * Adds the API header to the specified request.
     *
     * @param request the request to which the API header will be added.
     */
    static void addApiHeader(AbstractHttpMessage request) {

        request.addHeader("X-BV-API-Key", "local_admin");
    }

    /**
     * Executes an HTTP GET request via Apache's HTTP lib.
     *
     * @param url the URL to get.
     * @return the response from the GET, <code>null</code> if unsuccessful.
     */
    static String get(String url) {

        CloseableHttpClient client = HttpClientBuilder.create().build();
        CloseableHttpResponse response = null;

        HttpGet request = new HttpGet(url);

        HttpUtils.addApiHeader(request);

        try {

            response = client.execute(request);
            HttpEntity entity = response.getEntity();
            return EntityUtils.toString(entity);

        } catch (IOException e) {
            return null;
        } finally {
            cleanup(response, client);
        }
    }

    /**
     * Cleans up the response and client object of an HTTP request procedure.
     *
     * @param response the response that is to be cleaned up.
     * @param client the client that is to be cleaned up.
     */
    static void cleanup(CloseableHttpResponse response, CloseableHttpClient client) {

        try {
            if (response != null) response.close();
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
