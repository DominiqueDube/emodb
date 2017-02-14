package com.dubeanddube.emodb.data;

/**
 * POJO containing a single document result of a RESTful query to the search index.
 *
 * Format of the deserialized JSON:
 *
 * In case of success:
 *
 * {
 *     "success": true,
 *     "payload": {
 *       "color": "green",
 *        "text": "Lacus augue vitae dis orci natoque nonummy."
 *     }
 * }
 *
 * In case of failure (omitting reasons for simplicity):
 *
 * {
 *     "success": false
 * }
 *
 * @author Dominique Dube
 */
public class ItemResult extends JsonResult {

    public boolean success;
    public Item payload;
}
