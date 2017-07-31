# ajackson
A library that wraps [actson](https://github.com/michel-kraemer/actson) to provide non-blocking behaviour for jackson [parser](https://github.com/FasterXML/jackson-core/).

## async mapper example
Use `actson` for async non-blocking parsing. Use `jackson` for superb mapping features.

```java
Consumer<JsonNode> callback = (jsonRoot) -> {
    Model model = mapper.treeToValue(jsonRoot, Model.class);
};
AsyncJsonParser parser = new AsyncJsonParser(callback);

// ... in some other thread
parser.feed(bytes)
```