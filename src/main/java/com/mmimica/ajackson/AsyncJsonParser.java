package com.mmimica.ajackson;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.function.Consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.undercouch.actson.JsonEvent;
import de.undercouch.actson.JsonParser;

public class AsyncJsonParser {
    private final Consumer<JsonNode> onDone;

    private JsonParser parser;
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

    public AsyncJsonParser(Consumer<JsonNode> onDone) {
        this.onDone = onDone;
        reset();
    }

    public void consume(byte[] bytes) throws JsonProcessingException {
        consume(bytes, bytes.length);
    }

    public void consume(byte[] bytes, int length) throws JsonProcessingException {
        int pos = 0;
        while (pos < length) {
            pos += parser.getFeeder().feed(bytes, pos, length - pos);

            int event;
            while ((event = parser.nextEvent()) != JsonEvent.NEED_MORE_INPUT) {
                if (event == JsonEvent.ERROR) {
                    reset();
                    throw new RuntimeException("actson parsing exception at pos " + parser.getParsedCharacterCount());
                } else {
                    JsonNode root = buildTree(event);
                    if (root != null) {
                        reset();
                        onDone.accept(root);
                        return;
                    }
                }
            }
        }
    }

    private void reset() {
        parser = new JsonParser(StandardCharsets.UTF_8);
        fieldName = null;
        stack.clear();
    }

    /**
     * @return The root node when the whole tree is built.
     **/
    private JsonNode buildTree(int event) {
        switch (event) {
        case JsonEvent.FIELD_NAME:
            fieldName = parser.getCurrentString();
            return null;

        case JsonEvent.START_OBJECT:
            stack.push(createNode(stack.top()));
            return null;

        case JsonEvent.START_ARRAY:
            stack.push(createArray(stack.top()));
            return null;

        case JsonEvent.END_OBJECT:
        case JsonEvent.END_ARRAY:
            JsonNode current = stack.pop();
            if (stack.isEmpty())
                return current;
            else
                return null;

        case JsonEvent.VALUE_INT:
            addLong(stack.top(), Long.parseLong(parser.getCurrentString()));
            return null;

        case JsonEvent.VALUE_STRING:
            addString(stack.top(), parser.getCurrentString());
            return null;

        case JsonEvent.VALUE_DOUBLE:
            addDouble(stack.top(), parser.getCurrentDouble());
            return null;

        case JsonEvent.VALUE_NULL:
            addNull(stack.top());
            return null;

        case JsonEvent.VALUE_TRUE:
            addBoolean(stack.top(), true);
            return null;

        case JsonEvent.VALUE_FALSE:
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

    private void addDouble(JsonNode current, double d) {
        assert current != null;

        if (ObjectNode.class.isInstance(current))
            ObjectNode.class.cast(current).put(fieldName, d);
        else
            ArrayNode.class.cast(current).add(d);
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