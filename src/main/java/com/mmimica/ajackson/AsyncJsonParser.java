package com.mmimica.ajackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.async.ByteArrayFeeder;
import com.fasterxml.jackson.core.json.async.NonBlockingJsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.LinkedList;
import java.util.function.Consumer;

public class AsyncJsonParser {
    private final Consumer<JsonNode> onNodeDone;

    private final NonBlockingJsonParser parser;
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

        boolean isEmpty() {
            return list.isEmpty();
        }
    }

    private final Stack stack = new Stack();

    public AsyncJsonParser(Consumer<JsonNode> onNodeDone) throws IOException {
        this(new JsonFactory(), onNodeDone);
    }

    public AsyncJsonParser(JsonFactory jsonFactory, Consumer<JsonNode> onNodeDone) throws IOException {
        this.onNodeDone = onNodeDone;
        this.parser = (NonBlockingJsonParser) jsonFactory.createNonBlockingByteArrayParser();
    }

    public void consume(byte[] bytes) throws IOException {
        consume(bytes, bytes.length);
    }

    public void consume(byte[] bytes, int length) throws IOException {
        ByteArrayFeeder feeder = parser.getNonBlockingInputFeeder();
        boolean consumed = false;
        while (!consumed) {
            if (feeder.needMoreInput()) {
                feeder.feedInput(bytes, 0, length);
                consumed = true;
            }
    
            JsonToken event;
            while ((event = parser.nextToken()) != JsonToken.NOT_AVAILABLE) {
                JsonNode root = buildTree(event);
                if (root != null) {
                    onNodeDone.accept(root);
                }
            }
        }
    }

    /**
     * @return The root node when the whole tree is built.
     **/
    private JsonNode buildTree(JsonToken event) throws IOException {
        switch (event) {
            case FIELD_NAME:
                assert !stack.isEmpty();
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
                assert !stack.isEmpty();
                JsonNode current = stack.pop();
                if (stack.isEmpty())
                    return current;
                else
                    return null;

            case VALUE_NUMBER_INT:
                assert !stack.isEmpty();
                addLong(stack.top(), parser.getLongValue());
                return null;

            case VALUE_STRING:
                assert !stack.isEmpty();
                addString(stack.top(), parser.getValueAsString());
                return null;

            case VALUE_NUMBER_FLOAT:
                assert !stack.isEmpty();
                addFloat(stack.top(), parser.getFloatValue());
                return null;

            case VALUE_NULL:
                assert !stack.isEmpty();
                addNull(stack.top());
                return null;

            case VALUE_TRUE:
                assert !stack.isEmpty();
                addBoolean(stack.top(), true);
                return null;

            case VALUE_FALSE:
                assert !stack.isEmpty();
                addBoolean(stack.top(), false);
                return null;

            default:
                throw new RuntimeException("Unknown json event " + event);
        }
    }

    private JsonNode createNode(JsonNode current) {
        if (current instanceof ObjectNode)
            return ((ObjectNode) current).putObject(fieldName);
        else if (current instanceof ArrayNode)
            return ((ArrayNode) current).addObject();
        else
            return JsonNodeFactory.instance.objectNode();
    }

    private JsonNode createArray(JsonNode current) {
        if (current instanceof ObjectNode)
            return ((ObjectNode) current).putArray(fieldName);
        else if (current instanceof ArrayNode)
            return ((ArrayNode) current).addArray();
        else
            return JsonNodeFactory.instance.arrayNode();
    }

    private void addLong(JsonNode current, long v) {
        assert current != null;

        if (current instanceof ObjectNode)
            ((ObjectNode) current).put(fieldName, v);
        else
            ((ArrayNode) current).add(v);
    }

    private void addString(JsonNode current, String s) {
        assert current != null;

        if (current instanceof ObjectNode)
            ((ObjectNode) current).put(fieldName, s);
        else
            ((ArrayNode) current).add(s);
    }

    private void addFloat(JsonNode current, float f) {
        assert current != null;

        if (current instanceof ObjectNode)
            ((ObjectNode) current).put(fieldName, f);
        else
            ((ArrayNode) current).add(f);
    }

    private void addNull(JsonNode current) {
        assert current != null;

        if (current instanceof ObjectNode)
            ((ObjectNode) current).putNull(fieldName);
        else
            ((ArrayNode) current).addNull();
    }

    private void addBoolean(JsonNode current, boolean b) {
        assert current != null;

        if (current instanceof ObjectNode)
            ((ObjectNode) current).put(fieldName, b);
        else
            ((ArrayNode) current).add(b);
    }
}
