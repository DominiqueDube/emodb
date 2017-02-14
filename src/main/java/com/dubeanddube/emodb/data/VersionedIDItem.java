package com.dubeanddube.emodb.data;

import org.vvcephei.occ_map.Versioned;

/**
 * Creates a versioned ID entry for use in conjunction with John Roesler's OCCMap.
 *
 * @author Dominique Dube
 */
public class VersionedIDItem extends IDItem implements Versioned {

    private int version;

    /**
     * Constructs a versioned ID item.
     *
     * @param id the ID of this ID item in UUID format.
     * @param item the wrapped item (color, text).
     * @param version the version of this versioned ID item.
     * @throws IllegalArgumentException if the specified ID is not a valid UUID.
     */
    public VersionedIDItem(String id, Item item, int version) throws IllegalArgumentException {
        super(id, item);

        this.version = version;
    }

    /**
     * Checks whether this item and the specified object are equal. Both objects
     * are equal when the objects" versions, IDs and items are equal.
     *
     * @param other the object to which this versioned ID item is to be compared.
     * @return <code>true</code> if this item and the specified object are equal,
     *         <code>false</code> otherwise.
     */
    @Override
    public boolean equals(final Object other) {

        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;

        final VersionedIDItem that = (VersionedIDItem)other;

        return version == that.version &&
                ! (uuid != null ? ! uuid.equals(that.uuid) : that.uuid != null) &&
                ! (item != null ? ! item.equals(that.item) : that.item != null);
    }

    /**
     * Computes a hash code for this versioned ID item.
     *
     * @return a hash code for this versioned ID item.
     */
    @Override
    public int hashCode() {

        int result = item != null ? item.hashCode() : 0;
        result = 31 * result + version;
        return result;
    }

    /**
     * Returns the version of this versioned ID item.
     *
     * @return the version of this versioned ID item.
     */
    @Override
    public long getVersion() {

        return version;
    }
}
