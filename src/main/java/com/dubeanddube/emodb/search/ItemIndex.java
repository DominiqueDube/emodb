package com.dubeanddube.emodb.search;

import com.dubeanddube.emodb.data.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

/**
 * Defines the interface to items indexes.
 * This prototype supports two index implementions (memory hash map, local Elasticsearch).
 *
 * @author Dominique Dube
 */
public interface ItemIndex {

    /**
     * Helper function to serialize a list of items to a JSON array string.
     *
     * @param items the items that are to be serialized.
     * @return a JSON array string with the serialized items,
     *         <code>JsonUtils.NO_SUCCESS</code> if the serialization fails.
     */
    static String serializeItems(List<Item> items) {

        ItemArrayResult result = new ItemArrayResult();
        result.success = true;
        result.size = items.size();
        result.payload = items;

        try {
            return (new ObjectMapper()).writeValueAsString(result);
        } catch (JsonProcessingException e) {
            return JsonUtils.NO_SUCCESS;
        }
    }

    /**
     * Returns the document with the specified ID.
     *
     * @see ItemResult for JSON format of returned JSON string.
     *
     * @param id the requested document ID.
     * @return the document with the specified ID in JSON format.
     */
    String getDocumentById(String id);

    /**
     * Returns all documents with the specified color.
     *
     * @see ItemResult for JSON format of returned JSON string.
     *
     * @param color the requested document color.
     * @return all documents with the specified color in JSON format, en empty JSON array
     *         if no matching documents were found.
     */
    String getDocumentsByColor(String color);

    /**
     * Updates a document in the index. The update will only succeed if the corresponding
     * document does not yet exist or if the version of the update is newer than the
     * version of the existing document.
     *
     * @param newItem the item that is to be updated.
     * @return <code>true</code> if the item was updated successfully, <code>false</code> otherwise.
     */
    boolean updateDocument(VersionedIDItem newItem);
}
