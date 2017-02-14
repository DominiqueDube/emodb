package com.dubeanddube.emodb.search;

import com.dubeanddube.emodb.data.*;
import com.dubeanddube.emodb.services.EmoSor;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vvcephei.occ_map.OCCHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implements the search index, either based on an internal OCC hash map or Elasticsearch.
 *
 * @author Dominique Dube
 */
public class SearchIndex {

    private final Logger logger = LoggerFactory.getLogger(SearchIndex.class); // some basic logging

    public enum IndexType {

        // manages an index in a local hash map (OCC hash map for demo)
        MEMORY_HASH_MAP,

        // manages an index on a local elasticsearch instance (port 9200)
        LOCAL_ELASTIC_SEARCH
    }

    private IndexType indexType;

    private OCCHashMap<String, VersionedIDItem> itemsMap = new OCCHashMap<>();

    /**
     * Constructs a search index with the specified index type.
     * If LOCAL_ELASTIC_SEARCH is selected and the elastic search instance
     * cannot be located at port 9200, the implementation will revert to the memory-based hash map.
     * (This is for demonstration only).
     *
     * @param indexType the type of index that is to be used by this search index.
     */
    public SearchIndex(IndexType indexType) {

        this.indexType = indexType;
    }

    /**
     * Returns the index type that is used by this search index.
     *
     * @return the index type that is used by this search index.
     */
    public IndexType getIndexType() {

        return indexType;
    }

    /**
     * Returns the document with the specified ID.
     *
     * @see ItemResult for JSON format of returned JSON string.
     *
     * @param id the requested document ID.
     * @return the document with the specified ID in JSON format.
     */
    public String getDocumentById(String id) {

        VersionedIDItem idItem = itemsMap.get(id);

        JsonResult result;

        if (idItem == null) {

            result = new MessageResult();
            ((MessageResult)result).success = true;
            ((MessageResult)result).payload = "no match found";

        } else {

            result = new ItemResult();
            ((ItemResult)result).success = true;
            ((ItemResult)result).payload = idItem.getItem();
        }

        ObjectMapper mapper = new ObjectMapper();

        try {
            return mapper.writeValueAsString(result);
        } catch (IOException e) {
            return JsonUtils.NO_SUCCESS;
        }
    }

    /**
     * Returns all documents with the specified color.
     *
     * @see ItemResult for JSON format of returned JSON string.
     *
     * @param color the requested document color.
     * @return all documents with the specified color in JSON format.
     */
    public String getDocumentsByColor(String color) {

        Collection<VersionedIDItem> idItems = itemsMap.values();

        List<Item> matchingItems = idItems.stream()
                .map(VersionedIDItem::getItem)
                .filter(item -> item.matchesColor(color))
                .collect(Collectors.toCollection(ArrayList::new));

        ItemArrayResult result = new ItemArrayResult();
        result.success = true;
        result.size = matchingItems.size();
        result.payload = matchingItems;

        ObjectMapper mapper = new ObjectMapper();

        try {
            return mapper.writeValueAsString(result);
        } catch (JsonProcessingException e) {
            return JsonUtils.NO_SUCCESS;
        }
    }

    /**
     * Updates a document in the index (either hash table or Elasticsearch). The update will only succeed
     * if the corresponding document does not yet exist or if the version of the update is newer than the
     * version of the existing document.
     *
     * @param newItem the item that is to be updated.
     * @return <code>true</code> if the item was updated successfully, <code>false</code> otherwise.
     */
    public boolean updateDocument(VersionedIDItem newItem) {

        // TODO: maximum number of retries

        String id = newItem.getId();
        long versionLong = newItem.getVersion();

        VersionedIDItem prevItem = itemsMap.putIfAbsent(id, newItem);

        boolean success = prevItem == null;

        while (! success && versionLong > prevItem.getVersion()) {

            success = itemsMap.replace(id, prevItem, newItem);
            if (success) break; // shortcut

            prevItem = itemsMap.get(id);
        }

        logger.info("hash map size = " + itemsMap.size());

        return success;
    }

    /**
     * Loads the sample data in the items table into this search index (milestone 1).
     *
     * @return <code>true</code> if all sample data was loaded successfully, <code>false</code> otherwise.
     */
    public boolean loadSampleData() {

        String responseString = EmoSor.readItems();

        // Attempt to parse JSON objects from the response string.
        // Note that error handling is trivial here - in a productive system, more
        // should be done to account for all sorts of problems (e.g. no response
        // at all, unexpected objects, JSON format problems, etc.).

        JsonFactory factory = new JsonFactory();
        ObjectMapper mapper = new ObjectMapper(factory);

        JsonNode rootNode;

        try {
            rootNode = mapper.readTree(responseString);
        } catch (IOException e) {
            logger.debug("Unable to create root node from which to obtain items");
            return false;
        }

        if (! rootNode.isArray()) {
            logger.debug("Root node is not of type array");
            return false;
        }

        for (JsonNode root : rootNode) { // iterate documents

            JsonNode colorNode = root.path("color");
            String color = colorNode.isMissingNode() ? null : colorNode.asText();

            JsonNode textNode = root.path("text");
            String text = textNode.isMissingNode() ? null : textNode.asText();

            JsonNode idNode = root.path("~id");
            String id = idNode.isMissingNode() ? null : idNode.asText();

            JsonNode versionNode = root.path("~version");
            String version = versionNode.isMissingNode() ? null : versionNode.asText();

            // some simple error handling
            if (color == null || text == null || id == null || version == null) {
                logger.debug("missing JSON node");
                return false;
            }

            Item item = new Item(color, text);

            int versionInt = NumberUtils.toInt(version, -1);

            if (versionInt == -1) {
                logger.debug("encountered illegal version format");
                return false;
            }

            VersionedIDItem idItem = new VersionedIDItem(id, item, versionInt);
            updateDocument(idItem); // update document for the first time
        }

        return true; // all document updates succeeded
    }
}
