package com.dubeanddube.emodb.services;

import com.dubeanddube.emodb.data.IDItem;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Bundles RESTful methods to access EmoDB's system of records.
 *
 * @author Dominique Dube
 */
public class EmoSor {

    private static final String SOR_TABLE_NAME = "items";

    public static String readItems() {

        // Remark: a possible next task would be to scan tables partially with the 'from' keyword.

        String url = "http://localhost:8080/sor/1/" + SOR_TABLE_NAME + "?limit=1000";

        return HttpUtils.get(url);
    }

    /**
     * Returns the size of the items table.
     *
     * @return the size of the items table, -1 if the size could not be read from the system of records.
     */
    public static long getTableSize() {

        String url = "http://localhost:8080/sor/1/_table/" + SOR_TABLE_NAME + "/size?limit=1000";

        String responseString = HttpUtils.get(url);

        // good practice: do not use NumberFormatException-based approach
        return NumberUtils.toLong(responseString, -1L);
    }

    /**
     * Updates the document with the specified ID item's ID (UUID) with the color and text contents
     * of the wrapped item. If a document with the ID does not exist, it is created.
     *
     * The POST method is used for both document creation and updating (see API description).
     *
     * @param idItem the ID item that contains the color and text that is to be updated.
     * @param comment the comment for the audit, e.g. "initial-submission" for document creation.
     * @return <code>true</code> if the document update was successful, <code>false</code> otherwise.
     */
    public static boolean updateDocument(IDItem idItem, String comment) {

        String documentId = idItem.getId();

        String url = "http://localhost:8080/sor/1/" + SOR_TABLE_NAME + "/" +
                documentId + "?audit=comment:'" + comment + "',host:localhost";

        String dataBinary = "{.., \"color\":\"" + idItem.getItem().getColor() +
                "\",\"text\":\"" + idItem.getItem().getText() + "\"}";

        CloseableHttpClient client = HttpClientBuilder.create().build();
        CloseableHttpResponse response = null;

        HttpPost request = new HttpPost(url); // POST for create/modify document

        HttpUtils.addApiHeader(request);
        request.addHeader("content-type", "application/x.json-delta");

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

    /**
     * Creates the items table for milestone 0.
     *
     * @return <code>true</code> if the items table was successfully created, <code>false</code> otherwise.
     */
    public static boolean createTable() {

        String url = "http://localhost:8080/sor/1/_table/" + SOR_TABLE_NAME +
                "?options=placement:'ugc_global:ugc'&audit=comment:'initial+provisioning',host:localhost";

        String dataBinary = "{\"type\":\"review\",\"client\":\"TestCustomer\"}";

        CloseableHttpClient client = HttpClientBuilder.create().build();
        CloseableHttpResponse response = null;

        HttpPut request = new HttpPut(url); // PUT for create table

        HttpUtils.addApiHeader(request);
        request.addHeader("content-type", "application/json");

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

    /**
     * Drops the items table.
     *
     * @return <code>true</code> if the items table was dropped successfully,
     *         <code>false</code> otherwise.
     */
    public static boolean dropTable() {

        String url = "http://localhost:8080/sor/1/_table/" + SOR_TABLE_NAME +
                "?audit=comment:'clean-slate',host:localhost";

        CloseableHttpClient client = HttpClientBuilder.create().build();
        CloseableHttpResponse response = null;

        HttpDelete request = new HttpDelete(url); // DELETE for drop table

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
}
