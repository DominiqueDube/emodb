package com.dubeanddube.emodb.data;

import java.util.UUID;

/**
 * Wraps an item (color, text) with an additional ID (UUID).
 *
 * @author Dominique Dube
 */
public class IDItem {

    UUID uuid;
    Item item;

    /**
     * Constructs an ID item with the specified ID and item.
     *
     * @param id the ID of this ID item in UUID format.
     * @param item the wrapped item (color, text).
     * @throws IllegalArgumentException if the specified ID is not a valid UUID.
     */
    IDItem(String id, Item item) throws IllegalArgumentException {

        this.uuid = UUID.fromString(id);
        this.item = item;
    }

    /**
     * Returns the ID of this ID item.
     *
     * @return the ID of this ID item (in UUID format).
     */
    public String getId() {

        return uuid.toString();
    }

    /**
     * Returns the item that is wrapped by this ID item.
     *
     * @return the item that is wrapped by this ID item.
     */
    public Item getItem() {

        return item;
    }

    /**
     * Checks whether the specified ID conforms with the format of a UUID.
     *
     * @param id the ID that is to be checked for conformance.
     * @return <code>true</code> if the specified ID conforms with the format
     *         of a UUID, <code>false</code> otherwise.
     */
    static boolean isValidId(String id) {

        // check UUID validity without relying on throwing class UUID's exception
        return id.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
    }

    /**
     * Returns a string representation of this ID item.
     *
     * @return a string representation of this ID item in JSON format.
     */
    @Override
    public String toString() {

        return "\"" + getId() + "\":" + item.toString();
    }
}
