package org.lareferencia.core.export;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import java.io.*;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import javax.xml.xpath.*;
import org.w3c.dom.*;

/** Versioned, strict XOAI projection. Instances are immutable and reusable. */
public final class XoaiExportMapping {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final JsonNode specification;

    public XoaiExportMapping(JsonNode specification) {
        this.specification = specification.deepCopy();
        allowed(specification, Set.of("specVersion", "name", "version", "inputSchema", "fields"));
        if (!specification.path("specVersion").isIntegralNumber() || !specification.path("specVersion").bigIntegerValue().equals(BigInteger.ONE)
                || !specification.path("inputSchema").asText().equals("xoai"))
            throw new IllegalArgumentException("Expected specVersion=1 and inputSchema=xoai");
        for (String key : List.of("name", "version"))
            if (!specification.path(key).isTextual() || specification.path(key).asText().isBlank())
                throw new IllegalArgumentException("Missing " + key);
        validateFields(specification.path("fields"));
    }

    public JsonNode specification() { return specification.deepCopy(); }

    public ObjectNode project(String xml) throws Exception {
        return project(ExportXml.parse(xml));
    }

    public ObjectNode project(Document document) throws Exception {
        Element root = document.getDocumentElement();
        if (!"metadata".equals(root.getLocalName())) throw new IllegalArgumentException("Expected XOAI metadata root");
        String namespace = root.getNamespaceURI();
        if (namespace != null && !namespace.isEmpty() && !namespace.equals("http://www.lyncode.com/xoai"))
            throw new IllegalArgumentException("Unsupported XOAI namespace: " + namespace);
        return fields(root, specification.get("fields"));
    }

    private static void allowed(JsonNode node, Set<String> keys) {
        if (!node.isObject()) throw new IllegalArgumentException("Expected object");
        node.fieldNames().forEachRemaining(k -> { if (!keys.contains(k)) throw new IllegalArgumentException("Unknown property: " + k); });
    }

    private static void validateFields(JsonNode fields) {
        if (!fields.isObject()) throw new IllegalArgumentException("fields must be an object");
        fields.elements().forEachRemaining(rule -> {
            if (rule.has("forEach")) {
                allowed(rule, Set.of("forEach", "fields", "occurrenceStart", "includeContext"));
                selectionExpression(rule.get("forEach"));
                validateFields(rule.path("fields"));
                if (rule.has("includeContext")) {
                    JsonNode context = rule.get("includeContext");
                    if (!context.isArray()) throw new IllegalArgumentException("includeContext must be an array");
                    Set<String> seen = new HashSet<>();
                    for (JsonNode key : context) {
                        if (!key.isTextual() || !Set.of("schema", "qualifier", "path").contains(key.asText()) || !seen.add(key.asText()))
                            throw new IllegalArgumentException("Invalid or duplicate includeContext property");
                        if (rule.path("fields").has(key.asText())) throw new IllegalArgumentException("Context field collision: " + key.asText());
                    }
                }
                if (rule.has("occurrenceStart")) {
                    if (!rule.get("occurrenceStart").isTextual() || !rule.get("occurrenceStart").asText().matches("[A-Za-z_][A-Za-z0-9_-]*"))
                        throw new IllegalArgumentException("occurrenceStart must be a field name");
                    rule.path("fields").elements().forEachRemaining(field -> {
                        if (field.has("forEach") || !field.path("select").asText().matches(":[A-Za-z_][A-Za-z0-9_-]*"))
                            throw new IllegalArgumentException("occurrenceStart requires direct :field selectors");
                    });
                }
            } else {
                allowed(rule, Set.of("select", "type", "many", "normalize", "unique", "maxLength"));
                expression(rule.path("select"), false);
                if (!Set.of("string", "integer", "boolean").contains(rule.path("type").asText()))
                    throw new IllegalArgumentException("Unsupported type");
                for (String key : List.of("many", "unique"))
                    if (rule.has(key) && !rule.get(key).isBoolean()) throw new IllegalArgumentException(key + " must be boolean");
                if (rule.has("maxLength") && (!rule.get("maxLength").canConvertToInt() || rule.get("maxLength").asInt() < 0))
                    throw new IllegalArgumentException("maxLength must be a non-negative integer");
                if (rule.has("maxLength") && !rule.path("type").asText().equals("string"))
                    throw new IllegalArgumentException("maxLength is only supported for string fields");
                if (rule.has("normalize")) {
                    if (!rule.get("normalize").isArray()) throw new IllegalArgumentException("normalize must be array");
                    rule.get("normalize").forEach(n -> { if (!n.isTextual() || !n.asText().equals("trim")) throw new IllegalArgumentException("Unknown normalization"); });
                }
            }
        });
    }

    /** XPath union keeps source document order and selects overlapping elements once. */
    private static String selectionExpression(JsonNode selector) {
        if (!selector.isArray()) return expression(selector, true);
        if (selector.isEmpty()) throw new IllegalArgumentException("forEach selectors cannot be empty");
        List<String> paths = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (JsonNode item : selector) {
            String path = expression(item, true);
            if (!seen.add(path)) throw new IllegalArgumentException("Duplicate forEach selector");
            paths.add(path);
        }
        return String.join(" | ", paths);
    }

    private static ObjectNode withContext(ObjectNode result, Node element, JsonNode rule) {
        List<String> path = new ArrayList<>();
        for (Node node = element; node instanceof Element e && "element".equals(e.getLocalName()); node = node.getParentNode())
            path.add(0, e.getAttribute("name"));
        for (JsonNode key : rule.path("includeContext")) {
            switch (key.asText()) {
                case "schema":
                    if (!path.isEmpty()) result.put("schema", path.get(0));
                    break;
                case "path": result.put("path", String.join(".", path)); break;
                case "qualifier":
                    if (path.size() > 2) {
                        String qualifier = String.join(".", path.subList(2, path.size()));
                        if (!qualifier.equals("none")) result.put("qualifier", qualifier);
                    }
                    break;
                default: throw new IllegalArgumentException("Unknown context property");
            }
        }
        return result;
    }

    private static String expression(JsonNode selector, boolean elements) {
        if (selector == null || !selector.isTextual()) throw new IllegalArgumentException("Selector must be string");
        String value = selector.asText();
        if (!value.matches("(?:[A-Za-z_][A-Za-z0-9_-]*|\\*)(?:\\.(?:[A-Za-z_][A-Za-z0-9_-]*|\\*))*(:[A-Za-z_][A-Za-z0-9_-]*)?|:[A-Za-z_][A-Za-z0-9_-]*"))
            throw new IllegalArgumentException("Invalid selector: " + value);
        String[] parts = value.split(":", -1);
        if (elements && parts.length > 1) throw new IllegalArgumentException("forEach selects elements");
        StringBuilder xpath = new StringBuilder(".");
        if (!parts[0].isEmpty()) for (String part : parts[0].split("\\."))
            xpath.append(part.equals("*") ? "//*[local-name()='element']" : "/*[local-name()='element' and @name='" + part + "']");
        if (!elements) xpath.append("/*[local-name()='field' and @name='").append(parts.length > 1 ? parts[1] : "value").append("']");
        return xpath.toString();
    }

    private static ObjectNode fields(Node context, JsonNode definitions) throws Exception {
        ObjectNode result = JSON.createObjectNode();
        Iterator<Map.Entry<String, JsonNode>> iterator = definitions.fields();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = iterator.next();
            JsonNode rule = entry.getValue();
            boolean grouped = rule.has("forEach");
            NodeList nodes = (NodeList) XPathFactory.newInstance().newXPath().evaluate(
                    grouped ? selectionExpression(rule.get("forEach")) : expression(rule.get("select"), false), context, XPathConstants.NODESET);
            ArrayNode values = JSON.createArrayNode();
            Set<JsonNode> seen = new HashSet<>();
            for (int i = 0; i < nodes.getLength(); i++) {
                JsonNode value;
                if (grouped && rule.has("occurrenceStart")) {
                    for (Node occurrence : occurrences(nodes.item(i), rule.path("occurrenceStart").asText()))
                        values.add(withContext(fields(occurrence, rule.get("fields")), nodes.item(i), rule));
                    continue;
                }
                if (grouped) value = withContext(fields(nodes.item(i), rule.get("fields")), nodes.item(i), rule);
                else {
                    String text = nodes.item(i).getTextContent();
                    if (rule.path("normalize").isArray() && rule.get("normalize").size() > 0) text = text.trim();
                    if (rule.has("maxLength") && text.codePointCount(0, text.length()) > rule.get("maxLength").asInt()) continue;
                    switch (rule.get("type").asText()) {
                        case "integer":
                            if (!text.matches("[+-]?[0-9]+")) throw new IllegalArgumentException("Invalid integer");
                            value = BigIntegerNode.valueOf(new BigInteger(text)); break;
                        case "boolean":
                            if (!text.equals("true") && !text.equals("false")) throw new IllegalArgumentException("Invalid boolean");
                            value = BooleanNode.valueOf(Boolean.parseBoolean(text)); break;
                        default: value = TextNode.valueOf(text);
                    }
                }
                if (!rule.path("unique").asBoolean() || seen.add(value)) values.add(value);
            }
            if (grouped || rule.path("many").asBoolean(true)) result.set(entry.getKey(), values);
            else if (values.size() > 1) throw new IllegalArgumentException("Multiple values for scalar " + entry.getKey());
            else if (values.size() == 1) result.set(entry.getKey(), values.get(0));
        }
        return result;
    }

    /** Each start field opens a group; following sibling fields belong to it. Never zip arrays. */
    private static List<Node> occurrences(Node element, String start) {
        List<Node> groups = new ArrayList<>();
        Node current = null;
        for (Node child = element.getFirstChild(); child != null; child = child.getNextSibling()) {
            if (!(child instanceof Element field) || !"field".equals(field.getLocalName())) continue;
            if (start.equals(field.getAttribute("name"))) {
                current = element.cloneNode(false);
                groups.add(current);
            }
            if (current == null)
                throw new IllegalArgumentException("Ambiguous occurrence: field " + field.getAttribute("name") + " precedes " + start);
            current.appendChild(child.cloneNode(true));
        }
        return groups;
    }

    public static String canonicalHash(JsonNode data) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(JSON.writeValueAsString(canonical(data)).getBytes(StandardCharsets.UTF_8));
        return "sha256:" + HexFormat.of().formatHex(digest);
    }

    private static JsonNode canonical(JsonNode node) {
        if (node.isObject()) {
            ObjectNode sorted = JSON.createObjectNode();
            TreeSet<String> names = new TreeSet<>();
            node.fieldNames().forEachRemaining(names::add);
            names.forEach(name -> sorted.set(name, canonical(node.get(name))));
            return sorted;
        }
        if (node.isArray()) {
            ArrayNode array = JSON.createArrayNode();
            node.forEach(value -> array.add(canonical(value)));
            return array;
        }
        return node;
    }
}
