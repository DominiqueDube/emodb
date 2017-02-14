package com.dubeanddube.emodb.data;

/**
 * Defines an item that contains a color and some text.
 *
 * @author Dominique Dube
 */
public class Item {

    private String color;
    private String text;

    /**
     * Default constructor, required for Jackson JSON-to-POJO conversion.
     */
    public Item() {
        this(null, null);
    }

    /**
     * Constructs an item with the specified color and text.
     *
     * @param color this item's color.
     * @param text this item's text.
     */
    public Item(String color, String text) {

        this.color = color;
        this.text = text;
    }

    /**
     * Returns this item's color.
     *
     * @return the color of this item.
     */
    public String getColor() {

        return color;
    }

    /**
     * Checks whether this item matches the given color.
     *
     * @param color the color that will be matched.
     * @return <code>true</code> if this item matches the given color, <code>false</code> otherwise.
     */
    public boolean matchesColor(String color) {

        return this.color != null ? this.color.equals(color) : color == null;
    }

    /**
     * Returns this item's text.
     *
     * @return the text of this item.
     */
    public String getText() {

        return text;
    }

    /**
     * Returns a string representation of this item.
     *
     * @return a string representation of this item in JSON format.
     */
    @Override
    public String toString() {

        return "{\"color\":" + color + ",\"text\":" + text + "}";
    }
}
