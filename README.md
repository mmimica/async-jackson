# ajackson
A library that provides non-blocking parsing behaviour for jackson [parser](https://github.com/FasterXML/jackson-core/).

## async mapper example
Use Jackson's non-blocking parser for async non-blocking parsing. Use `jackson mapper` for superb mapping features.

```java
Consumer<JsonNode> callback = (jsonRoot) -> {
    Model model = mapper.treeToValue(jsonRoot, Model.class);
};
AsyncJsonParser parser = new AsyncJsonParser(callback);

// ... in some other thread
parser.feed(bytes)
```