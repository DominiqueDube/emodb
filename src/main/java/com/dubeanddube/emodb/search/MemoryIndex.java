package com.dubeanddube.emodb.search;

import com.dubeanddube.emodb.data.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vvcephei.occ_map.OCCHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implements the memory-based index using the OCC hash map.
 *
 * @author Dominique Dube
 */
class MemoryIndex implements ItemIndex {

    private final Logger logger = LoggerFactory.getLogger(MemoryIndex.class); // some basic logging

    private OCCHashMap<String, VersionedIDItem> itemsMap = new OCCHashMap<>();

    // omitting default constructor

    /**
     * @see ItemIndex#getDocumentById(String)
     */
    @Override
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
     * @see ItemIndex#getDocumentsByColor(String)
     */
    @Override
    public String getDocumentsByColor(String color) {

        Collection<VersionedIDItem> idItems = itemsMap.values();

        List<Item> matchingItems = idItems.stream()
                .map(VersionedIDItem::getItem)
                .filter(item -> item.matchesColor(color))
                .collect(Collectors.toCollection(ArrayList::new));


        return ItemIndex.serializeItems(matchingItems);
    }

    /**
     * @see ItemIndex#updateDocument(VersionedIDItem)
     */
    @Override
    public boolean updateDocument(VersionedIDItem newItem) {

        // remark: maximum number of retries possible extension

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
}
