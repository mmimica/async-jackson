package com.mmimica.ajackson;

import java.io.IOException;
import java.util.LinkedList;
import java.util.function.Consumer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.async.ByteArrayFeeder;
import com.fasterxml.jackson.core.json.async.NonBlockingJsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class AsyncJsonParser {
    private final JsonFactory jsonFactory = new JsonFactory();

    private final Consumer<JsonNode> onDone;

    private NonBlockingJsonParser parser;
    private String fieldName;

    private static final class Stack {
        private final LinkedList<JsonNode> list = new LinkedList<>();

        JsonNode pop() {
            return list.removeLast();
        }

        JsonNode top() {
            if (list.isEmpty())
                return null;
            return list.getLast();
        }

        void push(JsonNode n) {
            list.add(n);
        }

        void clear() {
            list.clear();
        }

        boolean isEmpty() {
            return list.isEmpty();
        }
    }

    private final Stack stack = new Stack();

    public AsyncJsonParser(Consumer<JsonNode> onDone) throws IOException {
        this.onDone = onDone;
        reset();
    }

    public void consume(byte[] bytes) throws IOException {
        consume(bytes, bytes.length);
    }

    public void consume(byte[] bytes, int length) throws IOException {
        ByteArrayFeeder feeder = parser.getNonBlockingInputFeeder();
        feeder.feedInput(bytes, 0, length);

        JsonToken event;
        while ((event = parser.nextToken()) != JsonToken.NOT_AVAILABLE) {
            JsonNode root = buildTree(event);
            if (root != null) {
                reset();
                onDone.accept(root);
                return;
            }
        }
    }

    private void reset() throws IOException {
        parser = (NonBlockingJsonParser) jsonFactory.createNonBlockingByteArrayParser();
        fieldName = null;
        stack.clear();
    }

    /**
     * @return The root node when the whole tree is built.
     **/
    private JsonNode buildTree(JsonToken event) throws IOException {
        switch (event) {
        case FIELD_NAME:
            fieldName = parser.getCurrentName();
            return null;

        case START_OBJECT:
            stack.push(createNode(stack.top()));
            return null;

        case START_ARRAY:
            stack.push(createArray(stack.top()));
            return null;

        case END_OBJECT:
        case END_ARRAY:
            JsonNode current = stack.pop();
            if (stack.isEmpty())
                return current;
            else
                return null;

        case VALUE_NUMBER_INT:
            addLong(stack.top(), parser.getLongValue());
            return null;

        case VALUE_STRING:
            addString(stack.top(), parser.getValueAsString());
            return null;

        case VALUE_NUMBER_FLOAT:
            addFloat(stack.top(), parser.getFloatValue());
            return null;

        case VALUE_NULL:
            addNull(stack.top());
            return null;

        case VALUE_TRUE:
            addBoolean(stack.top(), true);
            return null;

        case VALUE_FALSE:
            addBoolean(stack.top(), false);
            return null;

        default:
            throw new RuntimeException("Unknow json event " + event);
        }
    }

    private JsonNode createNode(JsonNode current) {
        if (ObjectNode.class.isInstance(current))
            return ObjectNode.class.cast(current).putObject(fieldName);
        else if (ArrayNode.class.isInstance(current))
            return ArrayNode.class.cast(current).addObject();
        else
            return JsonNodeFactory.instance.objectNode();
    }

    private JsonNode createArray(JsonNode current) {
        if (ObjectNode.class.isInstance(current))
            return ObjectNode.class.cast(current).putArray(fieldName);
        else if (ArrayNode.class.isInstance(current))
            return ArrayNode.class.cast(current).addArray();
        else
            return JsonNodeFactory.instance.arrayNode();
    }

    private void addLong(JsonNode current, long v) {
        assert current != null;

        if (ObjectNode.class.isInstance(current))
            ObjectNode.class.cast(current).put(fieldName, v);
        else
            ArrayNode.class.cast(current).add(v);
    }

    private void addString(JsonNode current, String s) {
        assert current != null;

        if (ObjectNode.class.isInstance(current))
            ObjectNode.class.cast(current).put(fieldName, s);
        else
            ArrayNode.class.cast(current).add(s);
    }

    private void addFloat(JsonNode current, float f) {
        assert current != null;

        if (ObjectNode.class.isInstance(current))
            ObjectNode.class.cast(current).put(fieldName, f);
        else
            ArrayNode.class.cast(current).add(f);
    }

    private void addNull(JsonNode current) {
        assert current != null;

        if (ObjectNode.class.isInstance(current))
            ObjectNode.class.cast(current).putNull(fieldName);
        else
            ArrayNode.class.cast(current).addNull();
    }

    private void addBoolean(JsonNode current, boolean b) {
        assert current != null;

        if (ObjectNode.class.isInstance(current))
            ObjectNode.class.cast(current).put(fieldName, b);
        else
            ArrayNode.class.cast(current).add(b);
    }
}