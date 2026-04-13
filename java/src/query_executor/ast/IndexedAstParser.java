package query_executor.ast;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import query_executor.BooleanOperator;
import query_executor.ComparisonOperator;
import query_executor.FilterCondition;
import query_executor.FilterExpression;
import query_executor.FilterGroup;

public final class IndexedAstParser {
    public AstDocument parse(String astText) {
        if (astText == null || astText.isBlank()) {
            throw new IllegalArgumentException("AST text must not be blank.");
        }

        List<String> lines = astText.lines().toList();
        List<AstNodeRecord> nodes = new ArrayList<>();
        int index = 0;
        while (index < lines.size()) {
            String line = lines.get(index).trim();
            // Blank lines separate node blocks and are otherwise ignored.
            if (line.isEmpty()) {
                index++;
                continue;
            }
            if (!line.startsWith("Node ")) {
                throw new IllegalArgumentException("Expected node header at line " + (index + 1));
            }
            ParseResult result = parseNode(lines, index);
            nodes.add(result.node());
            index = result.nextIndex();
        }

        AstDocument document = new AstDocument(nodes);
        document.validateReferences();
        document.root();
        return document;
    }

    private ParseResult parseNode(List<String> lines, int startIndex) {
        int nodeId = parseNodeId(lines.get(startIndex).trim(), startIndex + 1);
        int index = startIndex + 1;

        String typeLine = requireLine(lines, index++, "type:");
        AstNodeType nodeType = parseNodeType(extractValue(typeLine, "type:"), startIndex + 2);

        List<Integer> childIds = List.of();
        Map<String, Integer> childRoles = Map.of();

        if (index < lines.size()) {
            String nextLine = lines.get(index).trim();
            // Positional children are only used where child order is enough to define meaning.
            if (nextLine.startsWith("children:")) {
                childIds = parseIntegerList(extractValue(nextLine, "children:"));
                index++;
            } else if (nextLine.startsWith("childRoles:")) {
                ParseChildRoleResult childRoleResult = parseChildRoles(lines, index + 1);
                childRoles = childRoleResult.childRoles();
                index = childRoleResult.nextIndex();
            }
        }

        requireLine(lines, index, "params:");
        index++;
        ParseParamsResult paramsResult = parseParams(lines, index);
        return new ParseResult(
            new AstNodeRecord(nodeId, nodeType, List.copyOf(childIds), Map.copyOf(childRoles), Map.copyOf(paramsResult.params())),
            paramsResult.nextIndex()
        );
    }

    private ParseChildRoleResult parseChildRoles(List<String> lines, int startIndex) {
        LinkedHashMap<String, Integer> childRoles = new LinkedHashMap<>();
        int index = startIndex;
        while (index < lines.size()) {
            String rawLine = lines.get(index);
            String line = rawLine.trim();
            // Child-role parsing stops when the next block section begins.
            if (line.isEmpty() || !line.startsWith("- ")) {
                break;
            }
            int separatorIndex = line.indexOf(':');
            if (separatorIndex < 0) {
                throw new IllegalArgumentException("Malformed child role at line " + (index + 1));
            }
            String roleName = line.substring(2, separatorIndex).trim();
            String childIdText = line.substring(separatorIndex + 1).trim();
            childRoles.put(roleName, Integer.parseInt(childIdText));
            index++;
        }
        if (childRoles.isEmpty()) {
            throw new IllegalArgumentException("childRoles block must contain at least one role.");
        }
        return new ParseChildRoleResult(childRoles, index);
    }

    private ParseParamsResult parseParams(List<String> lines, int startIndex) {
        LinkedHashMap<String, Object> params = new LinkedHashMap<>();
        int index = startIndex;
        while (index < lines.size()) {
            String line = lines.get(index).trim();
            // Params stop when the next node block begins or the file ends.
            if (line.isEmpty()) {
                index++;
                break;
            }
            if (line.startsWith("Node ")) {
                break;
            }
            if (!line.startsWith("- ")) {
                throw new IllegalArgumentException("Malformed params entry at line " + (index + 1));
            }
            int separatorIndex = line.indexOf(':');
            if (separatorIndex < 0) {
                throw new IllegalArgumentException("Malformed param line at line " + (index + 1));
            }
            String key = line.substring(2, separatorIndex).trim();
            String rawValue = line.substring(separatorIndex + 1).trim();
            params.put(key, parseValue(key, rawValue, index + 1));
            index++;
        }
        return new ParseParamsResult(params, index);
    }

    private Object parseValue(String key, String rawValue, int lineNumber) {
        if ("filter".equals(key)) {
            return parseFilter(rawValue, lineNumber);
        }
        if (rawValue.startsWith("[") && rawValue.endsWith("]")) {
            return parseList(rawValue, lineNumber);
        }
        return parseScalar(rawValue, lineNumber);
    }

    private List<Object> parseList(String rawValue, int lineNumber) {
        String inner = rawValue.substring(1, rawValue.length() - 1).trim();
        if (inner.isEmpty()) {
            return List.of();
        }
        List<String> parts = splitTopLevel(inner, ',');
        List<Object> values = new ArrayList<>();
        // List parsing preserves source order because the executor uses it for targets and source names.
        for (String part : parts) {
            values.add(parseValue("", part.trim(), lineNumber));
        }
        return values;
    }

    private Object parseScalar(String rawValue, int lineNumber) {
        if (rawValue.startsWith("\"") && rawValue.endsWith("\"")) {
            return unescapeQuoted(rawValue);
        }
        if ("true".equals(rawValue) || "false".equals(rawValue)) {
            return Boolean.parseBoolean(rawValue);
        }
        try {
            return Integer.parseInt(rawValue);
        } catch (NumberFormatException ignored) {
            // Non-numeric identifiers remain strings because enum conversion happens in the binder.
        }
        if (rawValue.isBlank()) {
            throw new IllegalArgumentException("Blank scalar value at line " + lineNumber);
        }
        return rawValue;
    }

    private FilterExpression parseFilter(String rawValue, int lineNumber) {
        FilterTokenizer tokenizer = new FilterTokenizer(rawValue, lineNumber);
        FilterExpression expression = parseFilterExpression(tokenizer);
        // The filter string must be fully consumed so malformed trailing tokens cannot slip through.
        if (tokenizer.hasNext()) {
            throw new IllegalArgumentException("Unexpected trailing filter tokens at line " + lineNumber);
        }
        return expression;
    }

    private FilterExpression parseFilterExpression(FilterTokenizer tokenizer) {
        tokenizer.expect("(");
        String operatorToken = tokenizer.nextToken();
        // Boolean operators produce nested filter groups over one or more child expressions.
        if (isBooleanOperator(operatorToken)) {
            BooleanOperator operator = BooleanOperator.valueOf(operatorToken);
            List<FilterExpression> children = new ArrayList<>();
            while (!")".equals(tokenizer.peekToken())) {
                children.add(parseFilterExpression(tokenizer));
            }
            tokenizer.expect(")");
            if (operator == BooleanOperator.NOT && children.size() != 1) {
                throw new IllegalArgumentException("NOT filter groups require exactly one child expression.");
            }
            if (children.isEmpty()) {
                throw new IllegalArgumentException(operator + " filter groups require at least one child expression.");
            }
            return new FilterGroup(operator, List.copyOf(children));
        }
        // Comparison operators terminate in one column reference and one literal value.
        ComparisonOperator operator = parseComparisonOperator(operatorToken);
        String columnName = tokenizer.nextToken();
        Object comparisonValue = parseScalar(tokenizer.nextToken(), tokenizer.lineNumber());
        tokenizer.expect(")");
        return new FilterCondition(columnName, operator, comparisonValue);
    }

    private ComparisonOperator parseComparisonOperator(String token) {
        try {
            return ComparisonOperator.valueOf(token);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("Unknown comparison operator in filter: " + token, exception);
        }
    }

    private boolean isBooleanOperator(String token) {
        return "AND".equals(token) || "OR".equals(token) || "NOT".equals(token);
    }

    private List<Integer> parseIntegerList(String rawList) {
        if (!rawList.startsWith("[") || !rawList.endsWith("]")) {
            throw new IllegalArgumentException("Expected integer list value but found: " + rawList);
        }
        String inner = rawList.substring(1, rawList.length() - 1).trim();
        if (inner.isEmpty()) {
            return List.of();
        }
        List<String> parts = splitTopLevel(inner, ',');
        List<Integer> values = new ArrayList<>();
        for (String part : parts) {
            values.add(Integer.parseInt(part.trim()));
        }
        return values;
    }

    private List<String> splitTopLevel(String raw, char separator) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int bracketDepth = 0;
        int parenDepth = 0;
        boolean inQuotes = false;
        for (int index = 0; index < raw.length(); index++) {
            char character = raw.charAt(index);
            if (character == '"' && (index == 0 || raw.charAt(index - 1) != '\\')) {
                inQuotes = !inQuotes;
            }
            if (!inQuotes) {
                if (character == '[') {
                    bracketDepth++;
                } else if (character == ']') {
                    bracketDepth--;
                } else if (character == '(') {
                    parenDepth++;
                } else if (character == ')') {
                    parenDepth--;
                } else if (character == separator && bracketDepth == 0 && parenDepth == 0) {
                    parts.add(current.toString());
                    current.setLength(0);
                    continue;
                }
            }
            current.append(character);
        }
        parts.add(current.toString());
        return parts;
    }

    private String unescapeQuoted(String rawValue) {
        String inner = rawValue.substring(1, rawValue.length() - 1);
        return inner.replace("\\\"", "\"").replace("\\\\", "\\");
    }

    private int parseNodeId(String headerLine, int lineNumber) {
        try {
            return Integer.parseInt(headerLine.substring("Node ".length()).trim());
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Malformed node header at line " + lineNumber, exception);
        }
    }

    private AstNodeType parseNodeType(String rawType, int lineNumber) {
        try {
            return AstNodeType.valueOf(rawType);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("Unknown AST node type '" + rawType + "' at line " + lineNumber, exception);
        }
    }

    private String requireLine(List<String> lines, int index, String expectedPrefix) {
        if (index >= lines.size()) {
            throw new IllegalArgumentException("Expected line starting with '" + expectedPrefix + "' but reached end of document.");
        }
        String line = lines.get(index).trim();
        if (!line.startsWith(expectedPrefix)) {
            throw new IllegalArgumentException("Expected line starting with '" + expectedPrefix + "' at line " + (index + 1));
        }
        return line;
    }

    private String extractValue(String line, String prefix) {
        return line.substring(prefix.length()).trim();
    }

    private record ParseResult(AstNodeRecord node, int nextIndex) {
    }

    private record ParseChildRoleResult(Map<String, Integer> childRoles, int nextIndex) {
    }

    private record ParseParamsResult(Map<String, Object> params, int nextIndex) {
    }

    private static final class FilterTokenizer {
        private final List<String> tokens;
        private final int lineNumber;
        private int index;

        private FilterTokenizer(String rawFilter, int lineNumber) {
            this.tokens = tokenize(rawFilter);
            this.lineNumber = lineNumber;
        }

        private static List<String> tokenize(String rawFilter) {
            List<String> tokens = new ArrayList<>();
            StringBuilder current = new StringBuilder();
            boolean inQuotes = false;
            for (int index = 0; index < rawFilter.length(); index++) {
                char character = rawFilter.charAt(index);
                if (character == '"' && (index == 0 || rawFilter.charAt(index - 1) != '\\')) {
                    inQuotes = !inQuotes;
                    current.append(character);
                    continue;
                }
                if (!inQuotes && (character == '(' || character == ')')) {
                    if (!current.isEmpty()) {
                        tokens.add(current.toString().trim());
                        current.setLength(0);
                    }
                    tokens.add(String.valueOf(character));
                    continue;
                }
                if (!inQuotes && Character.isWhitespace(character)) {
                    if (!current.isEmpty()) {
                        tokens.add(current.toString().trim());
                        current.setLength(0);
                    }
                    continue;
                }
                current.append(character);
            }
            if (!current.isEmpty()) {
                tokens.add(current.toString().trim());
            }
            return tokens;
        }

        private boolean hasNext() {
            return index < tokens.size();
        }

        private String nextToken() {
            if (!hasNext()) {
                throw new IllegalArgumentException("Unexpected end of filter expression at line " + lineNumber);
            }
            return tokens.get(index++);
        }

        private String peekToken() {
            if (!hasNext()) {
                throw new IllegalArgumentException("Unexpected end of filter expression at line " + lineNumber);
            }
            return tokens.get(index);
        }

        private void expect(String expectedToken) {
            String actualToken = nextToken();
            if (!expectedToken.equals(actualToken)) {
                throw new IllegalArgumentException(
                    "Expected filter token '" + expectedToken + "' but found '" + actualToken + "' at line " + lineNumber
                );
            }
        }

        private int lineNumber() {
            return lineNumber;
        }
    }
}
