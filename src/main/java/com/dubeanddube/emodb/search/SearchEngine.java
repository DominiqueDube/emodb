package com.dubeanddube.emodb.search;

import com.dubeanddube.emodb.data.*;
import com.dubeanddube.emodb.services.EmoSor;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Implements the search index, either based on an internal OCC hash map or Elasticsearch.
 * If Elasticsearch was chosen but no instance of Elasticsearch is running on localhost:9200,
 * the search engine reverts to an internal OCC hash map.
 *
 * @author Dominique Dube
 */
public class SearchEngine implements ItemIndex {

    private final Logger logger = LoggerFactory.getLogger(SearchEngine.class); // some basic logging

    public enum IndexType {

        // manages an index in a local hash map (OCC hash map for demo)
        MEMORY_HASH_MAP,

        // manages an index on a local Elasticsearch instance (localhost:9200)
        LOCAL_ELASTIC_SEARCH
    }

    private IndexType indexType;

    private ItemIndex itemIndex;

    /**
     * Constructs a search index with the specified index type.
     * If LOCAL_ELASTIC_SEARCH is selected and the elastic search instance cannot be located
     * at localhost:9200 the implementation will revert to the memory-based hash map.
     * (This is for demonstration only).
     *
     * @param indexType the type of index that is to be used by this search engine.
     */
    public SearchEngine(IndexType indexType) {

        this.indexType = indexType;

        if (indexType == IndexType.LOCAL_ELASTIC_SEARCH) {

            if (ElasticIndex.isUp()) {

                logger.info("using Elasticsearch-based indexing via localhost:9200");

                this.itemIndex = new ElasticIndex();

                // start clean: delete index (if it exists from a previous run)


                if (ElasticIndex.deleteIndex()) {
                    logger.info("deleted existing Elasticsearch items index");
                } else {
                    logger.warn("could not delete Elasticsearch items index - clean start?");
                }

            } else {

                logger.warn("no Elasticsearch service found at localhost:9200 - " + "" +
                        "reverting to memory-based OCC hash map indexing");

                this.indexType = IndexType.MEMORY_HASH_MAP;
                itemIndex = new MemoryIndex(); // revert to hash map
            }

        } else {

            logger.info("using memory-based OCC hash map indexing");
            itemIndex = new MemoryIndex();
        }
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
     * @see ItemIndex#getDocumentById(String)
     */
    @Override
    public String getDocumentById(String id) {

        return itemIndex.getDocumentById(id);
    }

    /**
     * @see ItemIndex#getDocumentsByColor(String)
     */
    @Override
    public String getDocumentsByColor(String color) {

        return itemIndex.getDocumentsByColor(color);
    }

    /**
     * @see ItemIndex#updateDocument(VersionedIDItem)
     */
    @Override
    public boolean updateDocument(VersionedIDItem newItem) {

        return itemIndex.updateDocument(newItem);
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
