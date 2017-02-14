package com.dubeanddube.emodb.data;

import java.util.List;

/**
 * POJO containing a list of items that can be serialized to a JSON string.
 *
 * @author Dominique Dube
 */
public class ItemArrayResult extends JsonResult {

    public boolean success;
    public int size;
    public List<Item> payload;
}
