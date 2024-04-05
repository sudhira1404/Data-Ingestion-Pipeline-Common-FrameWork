package com.target.kelsaapi.common.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class JsonFlattener {

    private final Map<String, JsonNode> json = new LinkedHashMap<>(1000000);
    private final JsonNode root;

    public JsonFlattener(JsonNode node) {
        this.root = Objects.requireNonNull(node);
    }

    public Map<String, JsonNode> flatten() {
        process(root, "");
        return json;
    }

    /**
     * Processes json node
     *
     * @param node
     * @param prefix
     */
    private void process(JsonNode node, String prefix) {
        if (node.isObject()) {
            ObjectNode object = (ObjectNode) node;
            object
                    .fields()
                    .forEachRemaining(
                            entry -> {
                                process(entry.getValue(), prefix + "/" + entry.getKey());
                            });
        } else if (node.isArray()) {
            ArrayNode array = (ArrayNode) node;
            AtomicInteger counter = new AtomicInteger();
            array
                    .elements()
                    .forEachRemaining(
                            item -> {
                                process(item, prefix + "/" + counter.getAndIncrement());
                            });
        } else {
            json.put(prefix, node);
        }
    }
}