package com.dubeanddube.emodb.data;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URL;

/**
 * Custom parser for the retrieval of ID items from the two given milestone text files.
 * This parser does not parse the whole file at once but rather retrieves individual ID items.
 *
 * Example and expected JSON format:
 *
 * {
 *     "7b8d8a82-77b6-4940-95fe-50ed99b23cb2": {
 *         "color": "green",
 *         "text": "Lacus augue vitae dis orci natoque nonummy."
 *     },
 *     "4f2bc7d1-2974-4848-ba40-9d8cb997bfac": {
 *         "color": "indigo",
 *         "text": "Nulla morbi sit nunc laoreet."
 *     }
 * }
 *
 * @author Dominique Dube
 */
public class IDItemParser {

    private JsonParser parser;
    private ObjectMapper mapper;

    private boolean eof = false;

    /**
     * Constructs an ID item parser and prepares (opens) the parser for retrieving consecutive document
     * updates (items with IDs) from the specified URL.
     *
     * @param url the URL pointing to the file resource that will be parsed.
     * @throws IOException if the parser could not be prepared, in particular, if no URL was specified
     *         (<code>null</code>), or if the parser could not open the URL, or if the file starts with
     *         an unexpected token (the first token must be the start of an object, <code>{</code>).
     */
    public IDItemParser(URL url) throws IOException {

        if (url == null) throw new IOException("URL not specified (null)");

        JsonFactory factory = new JsonFactory();

        try {
            parser = factory.createParser(url);
        } catch (IOException e) {
            throw new IOException("failed to open file parser for resource " + url, e);
        }

        try {

            // skip start of object

            if (parser.nextToken() != JsonToken.START_OBJECT) {
                parser.close(); // release parser
                throw new IOException("expected start of object");
            }

        } catch (IOException e) {
            throw new IOException("failed to obtain next token", e);
        }

        // parser is now ready to retrieve individual ID items using an object mapper
        this.mapper = new ObjectMapper();
    }

    /**
     * Closes this ID item parser. If the parser is already closed, a call to this method has no effect.
     *
     * @throws IOException if this parser could not be closed for some reason.
     */
    public void close() throws IOException {

        try {
            parser.close();
        } catch (IOException e) {
            throw new IOException("failed to close parser", e);
        }
    }

    /**
     * Retrieves the next ID item from this parser's URL resource.
     *
     * @return the next ID item, <code>null</code> if the end of file (EOF) was reached.
     * @throws IOException if the next ID item could not be obtained from the file resource.
     */
    public IDItem getNext() throws IOException {

        if (eof) return null; // return immediately if EOF was reached before

        JsonToken token;

        try {
            token = parser.nextToken();
        } catch (IOException e) {
            throw new IOException("failed to obtain next token", e);
        }

        if (token == JsonToken.END_OBJECT) {

            // for simplicity, we do not handle the case where more tokens follow
            // the end of object token, but simply ignore them and assume EOF
            eof = true;
            return null;

        } else if (token != JsonToken.FIELD_NAME) {

            throw new IOException("expected field name");
        }

        String idText;

        try {
            idText = parser.getText(); // get text of current token
        } catch (IOException e) {
            throw new IOException("expected text", e);
        }

        if (! IDItem.isValidId(idText)) throw new IOException("found invalid ID in text (must be UUID)");

        try {
            token = parser.nextToken();
        } catch (IOException e) {
            throw new IOException("failed to obtain next token");
        }

        if (token != JsonToken.START_OBJECT) throw new IOException("expected start of object");

        Item item;

        try {
            item = mapper.readValue(parser, Item.class);
        } catch (IOException e) {
            throw new IOException("expected item (color and text)", e);
        }

        return new IDItem(idText, item);
    }
}
