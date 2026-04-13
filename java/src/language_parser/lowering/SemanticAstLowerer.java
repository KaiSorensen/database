package language_parser.lowering;

import crud_engine.CrudEngineInterface;
import crud_engine.DatabaseSchema;
import crud_engine.ObjectSchema;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import language_parser.ast.AstNodes;
import query_executor.BooleanOperator;
import query_executor.ComparisonOperator;
import query_executor.ast.AstNodeType;

public final class SemanticAstLowerer {
    private final CrudEngineInterface crudEngine;

    public SemanticAstLowerer() {
        this.crudEngine = null;
    }

    public SemanticAstLowerer(CrudEngineInterface crudEngine) {
        this.crudEngine = crudEngine;
    }

    public String lowerToIndexedAstText(AstNodes.ScriptNode script) {
        if (script.statements().size() != 1) {
            throw new IllegalArgumentException("Lowering to one executor AST text requires exactly one statement.");
        }
        return lowerStatement(script.statements().getFirst());
    }

    public String lowerToIndexedAstText(AstNodes.StatementNode statement) {
        return lowerStatement(statement);
    }

    private String lowerStatement(AstNodes.StatementNode statement) {
        Builder builder = new Builder();
        if (statement instanceof AstNodes.ObjectStatementNode objectStatement) {
            lowerObjectStatement(builder, objectStatement);
        } else if (statement instanceof AstNodes.AttributeStatementNode attributeStatement) {
            lowerAttributeStatement(builder, attributeStatement);
        } else if (statement instanceof AstNodes.ReadStatementNode readStatement) {
            lowerReadStatement(builder, readStatement);
        } else if (statement instanceof AstNodes.CreateRowsStatementNode createRowsStatement) {
            lowerCreateRowsStatement(builder, createRowsStatement);
        } else if (statement instanceof AstNodes.UpdateStatementNode updateStatement) {
            lowerUpdateStatement(builder, updateStatement);
        } else if (statement instanceof AstNodes.DeleteRowsStatementNode deleteRowsStatement) {
            lowerDeleteRowsStatement(builder, deleteRowsStatement);
        } else if (statement instanceof AstNodes.DeleteAttributesStatementNode deleteAttributesStatement) {
            lowerDeleteAttributesStatement(builder, deleteAttributesStatement);
        } else {
            throw new IllegalArgumentException("Unsupported statement type: " + statement.getClass().getName());
        }
        return builder.toIndexedAstText();
    }

    private void lowerObjectStatement(Builder builder, AstNodes.ObjectStatementNode statement) {
        Builder.Node node = switch (statement.kind()) {
            case CREATE -> builder.addNode(AstNodeType.ObjectCreate);
            case READ -> builder.addNode(AstNodeType.ObjectRead);
            case UPDATE -> builder.addNode(AstNodeType.ObjectUpdate);
            case DELETE -> builder.addNode(AstNodeType.ObjectDelete);
        };
        node.param("objectName", slug(statement.objectName()));
        if (statement.parentObjectName() != null) {
            node.param("parentObjectName", slug(statement.parentObjectName()));
            node.param("extendParentAttributes", true);
        }
        if (statement.newObjectName() != null) {
            node.param("newObjectName", slug(statement.newObjectName()));
        }
        if (statement.includeAttributeMetadata()) {
            node.param("includeAttributeMetadata", true);
        }
        if (statement.includeRowCount()) {
            node.param("includeRowCount", true);
        }
    }

    private void lowerAttributeStatement(Builder builder, AstNodes.AttributeStatementNode statement) {
        if (statement.kind() == AstNodes.AttributeStatementKind.CREATE && statement.derivedExpression() != null) {
            lowerDerivedAttributeCreate(builder, statement);
            return;
        }
        Builder.Node node = switch (statement.kind()) {
            case CREATE -> builder.addNode(AstNodeType.AttributeCreate);
            case READ -> builder.addNode(AstNodeType.AttributeRead);
            case UPDATE -> builder.addNode(AstNodeType.AttributeUpdate);
            case DELETE -> builder.addNode(AstNodeType.AttributeDelete);
        };
        node.param("objectName", slug(statement.objectName()));
        node.param("attributeName", slug(statement.attributeName()));
        if (statement.attributeType() != null) {
            node.param("attributeType", statement.attributeType().name());
        }
        if (statement.newAttributeName() != null) {
            node.param("newAttributeName", slug(statement.newAttributeName()));
        }
        if (statement.includeAttributeType()) {
            node.param("includeAttributeType", true);
        }
    }

    private void lowerDerivedAttributeCreate(Builder builder, AstNodes.AttributeStatementNode statement) {
        validateDerivedType(statement.attributeType(), statement.derivedExpression());
        String objectName = slug(statement.objectName());
        Builder.Node root = builder.addNode(AstNodeType.DataCreate)
            .param("objectName", objectName)
            .param("targetNames", List.of(qualify(objectName, statement.attributeName())))
            .param("createdColumnName", slug(statement.attributeName()));
        Builder.Node rowOperationNode = lowerRowExpressionNode(
            builder,
            objectName,
            statement.derivedExpression(),
            null,
            qualify(objectName, statement.attributeName())
        );
        root.childRole("valueExpression", rowOperationNode.id());
    }

    private void lowerReadStatement(Builder builder, AstNodes.ReadStatementNode statement) {
        if (statement.source() instanceof AstNodes.ObjectSourceNode objectSource) {
            lowerReadFromObject(builder, objectSource, statement.projection(), statement.predicate());
            return;
        }
        if (statement.source() instanceof AstNodes.JoinSourceNode joinSource) {
            if (statement.predicate() != null) {
                throw new IllegalArgumentException("read(join(...), ..., where(...)) is not supported in v1.");
            }
            if (!(statement.projection() instanceof AstNodes.AttributeProjectionNode attributeProjection)) {
                throw new IllegalArgumentException("Join reads support attributes(...) projections only in v1.");
            }
            lowerJoinRead(builder, joinSource, attributeProjection);
            return;
        }
        throw new IllegalArgumentException("Unsupported read source type.");
    }

    private void lowerReadFromObject(
        Builder builder,
        AstNodes.ObjectSourceNode objectSource,
        AstNodes.ProjectionNode projection,
        AstNodes.PredicateExpressionNode predicate
    ) {
        String objectName = slug(objectSource.objectName());
        Builder.Node root = builder.addNode(AstNodeType.DataRead);
        if (projection instanceof AstNodes.AttributeProjectionNode attributeProjection) {
            Builder.Node selection = builder.addNode(AstNodeType.Selection)
                .param("objectName", objectName)
                .param("attributes", qualifyReferencesForObject(objectName, attributeProjection.attributes()));
            if (predicate != null) {
                selection.param("filter", serializePredicate(predicate, objectName));
            }
            root.childRole("source", selection.id());
            return;
        }
        if (projection instanceof AstNodes.DeriveProjectionNode deriveProjection) {
            Builder.Node rowOperation = lowerRowExpressionNode(
                builder,
                objectName,
                deriveProjection.expression(),
                predicate,
                qualify(objectName, deriveProjection.outputName())
            );
            root.childRole("source", rowOperation.id());
            return;
        }
        if (projection instanceof AstNodes.SummarizeProjectionNode summarizeProjection) {
            Builder.Node columnOperation = lowerColumnExpressionNode(
                builder,
                objectName,
                summarizeProjection.expression(),
                predicate,
                qualify(objectName, summarizeProjection.outputName()),
                true
            );
            root.childRole("source", columnOperation.id());
            return;
        }
        throw new IllegalArgumentException("Unsupported read projection.");
    }

    private void lowerJoinRead(
        Builder builder,
        AstNodes.JoinSourceNode joinSource,
        AstNodes.AttributeProjectionNode projection
    ) {
        if (!(joinSource.left() instanceof AstNodes.ObjectSourceNode leftObjectSource)
            || !(joinSource.right() instanceof AstNodes.ObjectSourceNode rightObjectSource)) {
            throw new IllegalArgumentException("Nested join sources are not supported in v1.");
        }

        String leftObject = slug(leftObjectSource.objectName());
        String rightObject = slug(rightObjectSource.objectName());
        QualifiedReference leftJoinReference = qualifyReferenceForObject(leftObject, joinSource.leftReference());
        QualifiedReference rightJoinReference = qualifyReferenceForObject(rightObject, joinSource.rightReference());

        List<QualifiedReference> requestedReferences = qualifyReferencesForJoin(leftObject, rightObject, projection.attributes());
        List<String> leftAttributes = dedupePreservingOrder(collectAttributesForObject(requestedReferences, leftObject, leftJoinReference.qualifiedName()));
        List<String> rightAttributes = dedupePreservingOrder(collectAttributesForObject(requestedReferences, rightObject, rightJoinReference.qualifiedName()));

        Builder.Node root = builder.addNode(AstNodeType.DataRead);
        Builder.Node join = builder.addNode(AstNodeType.Join)
            .param("joinKind", "INNER")
            .param("leftObjectName", leftObject)
            .param("rightObjectName", rightObject)
            .param("leftColumnName", leftJoinReference.qualifiedName())
            .param("rightColumnName", rightJoinReference.qualifiedName());
        root.childRole("source", join.id());

        Builder.Node leftRead = builder.addNode(AstNodeType.DataRead);
        Builder.Node rightRead = builder.addNode(AstNodeType.DataRead);
        join.childRole("left", leftRead.id());
        join.childRole("right", rightRead.id());

        Builder.Node leftSelection = builder.addNode(AstNodeType.Selection)
            .param("objectName", leftObject)
            .param("attributes", leftAttributes);
        Builder.Node rightSelection = builder.addNode(AstNodeType.Selection)
            .param("objectName", rightObject)
            .param("attributes", rightAttributes);
        leftRead.childRole("source", leftSelection.id());
        rightRead.childRole("source", rightSelection.id());
    }

    private void lowerCreateRowsStatement(Builder builder, AstNodes.CreateRowsStatementNode statement) {
        String objectName = slug(statement.objectName());
        CanonicalRows canonicalRows = canonicalizeRows(statement.rows());
        Builder.Node root = builder.addNode(AstNodeType.DataCreate)
            .param("objectName", objectName)
            .param("targetNames", canonicalRows.columnNames().stream().map(name -> qualify(objectName, name)).toList());
        Builder.Node literalRows = builder.addNode(AstNodeType.LiteralRows)
            .param("objectName", objectName)
            .param("columnNames", canonicalRows.columnNames())
            .param("rows", canonicalRows.rows());
        root.childRole("valueExpression", literalRows.id());
    }

    private void lowerUpdateStatement(Builder builder, AstNodes.UpdateStatementNode statement) {
        String objectName = slug(statement.objectName());
        String targetName = qualify(objectName, statement.attributeName());
        Builder.Node root = builder.addNode(AstNodeType.DataUpdate)
            .param("objectName", objectName)
            .param("targetNames", List.of(targetName));

        Builder.Node targetSelection = builder.addNode(AstNodeType.Selection)
            .param("objectName", objectName)
            .param("attributes", List.of(targetName))
            .param("role", "targetSelection");
        if (statement.predicate() != null) {
            targetSelection.param("filter", serializePredicate(statement.predicate(), objectName));
        }
        root.childRole("targetSelection", targetSelection.id());

        Builder.Node valueExpressionNode = lowerUpdateValueExpression(
            builder,
            objectName,
            statement.valueExpression(),
            statement.predicate(),
            qualify(objectName, "__update_value")
        );
        root.childRole("valueExpression", valueExpressionNode.id());
    }

    private Builder.Node lowerUpdateValueExpression(
        Builder builder,
        String objectName,
        AstNodes.ValueExpressionNode valueExpression,
        AstNodes.PredicateExpressionNode predicate,
        String outputName
    ) {
        if (valueExpression instanceof AstNodes.ReferenceNode reference) {
            Builder.Node selection = builder.addNode(AstNodeType.Selection)
                .param("objectName", objectName)
                .param("attributes", List.of(qualifyReferenceForObject(objectName, reference).qualifiedName()));
            if (predicate != null) {
                selection.param("filter", serializePredicate(predicate, objectName));
            }
            return selection;
        }
        if (valueExpression instanceof AstNodes.RowExpressionNode rowExpression) {
            return lowerRowExpressionNode(builder, objectName, rowExpression, predicate, outputName);
        }
        if (valueExpression instanceof AstNodes.ColumnExpressionNode columnExpression) {
            return lowerColumnExpressionNode(builder, objectName, columnExpression, null, outputName, false);
        }
        throw new IllegalArgumentException("Unsupported update value expression.");
    }

    private void lowerDeleteRowsStatement(Builder builder, AstNodes.DeleteRowsStatementNode statement) {
        String objectName = slug(statement.objectName());
        List<String> fullAttributes = objectAttributesFor(objectName).stream().map(attribute -> qualify(objectName, attribute)).toList();
        Builder.Node root = builder.addNode(AstNodeType.DataDelete)
            .param("objectName", objectName)
            .param("targetNames", fullAttributes);
        Builder.Node selection = builder.addNode(AstNodeType.Selection)
            .param("objectName", objectName)
            .param("attributes", fullAttributes);
        if (statement.predicate() != null) {
            selection.param("filter", serializePredicate(statement.predicate(), objectName));
        }
        root.childRole("targetSelection", selection.id());
    }

    private void lowerDeleteAttributesStatement(Builder builder, AstNodes.DeleteAttributesStatementNode statement) {
        String objectName = slug(statement.objectName());
        List<String> attributes = qualifyReferencesForObject(objectName, statement.attributes());
        Builder.Node root = builder.addNode(AstNodeType.DataDelete)
            .param("objectName", objectName)
            .param("targetNames", attributes);
        Builder.Node selection = builder.addNode(AstNodeType.Selection)
            .param("objectName", objectName)
            .param("attributes", attributes);
        root.childRole("targetSelection", selection.id());
    }

    private Builder.Node lowerRowExpressionNode(
        Builder builder,
        String objectName,
        AstNodes.RowExpressionNode rowExpression,
        AstNodes.PredicateExpressionNode predicate,
        String outputName
    ) {
        List<String> sourceNames = rowExpression.arguments().stream()
            .map(reference -> qualifyReferenceForObject(objectName, reference).qualifiedName())
            .toList();
        Builder.Node rowOperation = builder.addNode(AstNodeType.RowOperation)
            .param("operationKind", rowExpression.operationName().name())
            .param("sourceNames", sourceNames)
            .param("outputColumnName", outputName);
        Builder.Node selection = builder.addNode(AstNodeType.Selection)
            .param("objectName", objectName)
            .param("attributes", dedupePreservingOrder(sourceNames));
        if (predicate != null) {
            selection.param("filter", serializePredicate(predicate, objectName));
        }
        rowOperation.child(selection.id());
        return rowOperation;
    }

    private Builder.Node lowerColumnExpressionNode(
        Builder builder,
        String objectName,
        AstNodes.ColumnExpressionNode columnExpression,
        AstNodes.PredicateExpressionNode predicate,
        String outputName,
        boolean applyPredicate
    ) {
        String sourceName = qualifyReferenceForObject(objectName, columnExpression.argument()).qualifiedName();
        Builder.Node columnOperation = builder.addNode(AstNodeType.ColumnOperation)
            .param("operationKind", columnExpression.operationName().name())
            .param("sourceColumnName", sourceName)
            .param("outputValueName", outputName);
        Builder.Node selection = builder.addNode(AstNodeType.Selection)
            .param("objectName", objectName)
            .param("attributes", List.of(sourceName));
        if (applyPredicate && predicate != null) {
            selection.param("filter", serializePredicate(predicate, objectName));
        }
        columnOperation.child(selection.id());
        return columnOperation;
    }

    private String serializePredicate(AstNodes.PredicateExpressionNode predicate, String objectName) {
        if (predicate instanceof AstNodes.ComparisonPredicateNode comparison) {
            return "("
                + mapComparison(comparison.operationName()).name()
                + " "
                + serializePredicateValue(comparison.left(), objectName)
                + " "
                + serializePredicateValue(comparison.right(), objectName)
                + ")";
        }
        AstNodes.BooleanPredicateNode booleanPredicate = (AstNodes.BooleanPredicateNode) predicate;
        return "("
            + mapBoolean(booleanPredicate.operationName()).name()
            + " "
            + booleanPredicate.children().stream()
                .map(child -> serializePredicate(child, objectName))
                .reduce((left, right) -> left + " " + right)
                .orElse("")
            + ")";
    }

    private String serializePredicateValue(AstNodes.ValueExpressionNode valueExpression, String objectName) {
        if (valueExpression instanceof AstNodes.ReferenceNode reference) {
            return qualifyReferenceForObject(objectName, reference).qualifiedName();
        }
        if (valueExpression instanceof AstNodes.LiteralNode literal) {
            return renderScalar(literal.value());
        }
        throw new IllegalArgumentException("Predicates support reference and literal operands only in v1.");
    }

    private QualifiedReference qualifyReferenceForObject(String objectName, AstNodes.ReferenceNode reference) {
        String attributeName = slug(reference.attributeName());
        if (reference.objectNameNullable() != null && !slug(reference.objectNameNullable()).equals(objectName)) {
            throw new IllegalArgumentException("Reference " + reference.objectNameNullable() + "." + reference.attributeName()
                + " does not belong to object " + objectName + ".");
        }
        return new QualifiedReference(objectName, attributeName);
    }

    private List<String> qualifyReferencesForObject(String objectName, Collection<AstNodes.ReferenceNode> references) {
        return references.stream()
            .map(reference -> qualifyReferenceForObject(objectName, reference).qualifiedName())
            .toList();
    }

    private List<QualifiedReference> qualifyReferencesForJoin(
        String leftObject,
        String rightObject,
        List<AstNodes.ReferenceNode> references
    ) {
        List<QualifiedReference> resolved = new ArrayList<>();
        for (AstNodes.ReferenceNode reference : references) {
            if (reference.qualified()) {
                String qualifiedObject = slug(reference.objectNameNullable());
                if (!qualifiedObject.equals(leftObject) && !qualifiedObject.equals(rightObject)) {
                    throw new IllegalArgumentException("Unknown join object reference: " + reference.objectNameNullable());
                }
                resolved.add(new QualifiedReference(qualifiedObject, slug(reference.attributeName())));
                continue;
            }
            boolean leftHas = objectAttributesFor(leftObject).contains(slug(reference.attributeName()));
            boolean rightHas = objectAttributesFor(rightObject).contains(slug(reference.attributeName()));
            if (leftHas == rightHas) {
                throw new IllegalArgumentException("Ambiguous join attribute reference: " + reference.attributeName());
            }
            resolved.add(new QualifiedReference(leftHas ? leftObject : rightObject, slug(reference.attributeName())));
        }
        return resolved;
    }

    private List<String> objectAttributesFor(String objectName) {
        if (crudEngine == null) {
            throw new IllegalArgumentException("Schema-aware lowering requires a CRUD engine.");
        }
        try {
            DatabaseSchema schema = crudEngine.readSchema();
            ObjectSchema objectSchema = schema.getObjects().get(objectName);
            if (objectSchema == null) {
                throw new IllegalArgumentException("Unknown object in schema: " + objectName);
            }
            return new ArrayList<>(objectSchema.getAttributes().keySet());
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to read schema while lowering.", exception);
        }
    }

    private void validateDerivedType(AstNodes.AttributeTypeName attributeType, AstNodes.RowExpressionNode rowExpression) {
        AstNodes.AttributeTypeName inferred = switch (rowExpression.operationName()) {
            case ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULO -> AstNodes.AttributeTypeName.INT;
            case CONTAINS -> AstNodes.AttributeTypeName.BOOL;
        };
        if (attributeType != inferred) {
            throw new IllegalArgumentException("Derived attribute type " + attributeType + " does not match expression type " + inferred + ".");
        }
    }

    private ComparisonOperator mapComparison(AstNodes.ComparisonOperationName comparisonOperationName) {
        return switch (comparisonOperationName) {
            case EQUALS -> ComparisonOperator.EQUALS;
            case NOT_EQUALS -> ComparisonOperator.NOT_EQUALS;
            case GREATER_THAN -> ComparisonOperator.GREATER_THAN;
            case LESS_THAN -> ComparisonOperator.LESS_THAN;
            case GREATER_THAN_OR_EQUALS -> ComparisonOperator.GREATER_THAN_OR_EQUALS;
            case LESS_THAN_OR_EQUALS -> ComparisonOperator.LESS_THAN_OR_EQUALS;
            case CONTAINS -> ComparisonOperator.CONTAINS;
        };
    }

    private BooleanOperator mapBoolean(AstNodes.BooleanOperationName booleanOperationName) {
        return switch (booleanOperationName) {
            case AND -> BooleanOperator.AND;
            case OR -> BooleanOperator.OR;
            case NOT -> BooleanOperator.NOT;
        };
    }

    private CanonicalRows canonicalizeRows(List<AstNodes.RowLiteralNode> rows) {
        LinkedHashSet<String> canonicalColumns = new LinkedHashSet<>();
        for (AstNodes.RowLiteralNode row : rows) {
            for (AstNodes.RowEntryNode entry : row.entries()) {
                canonicalColumns.add(slug(entry.attributeName()));
            }
        }
        List<String> columnNames = List.copyOf(canonicalColumns);
        List<List<Object>> values = new ArrayList<>();
        for (AstNodes.RowLiteralNode row : rows) {
            Map<String, Object> valuesByColumn = new LinkedHashMap<>();
            for (AstNodes.RowEntryNode entry : row.entries()) {
                valuesByColumn.put(slug(entry.attributeName()), entry.value().value());
            }
            List<Object> orderedValues = new ArrayList<>();
            for (String columnName : columnNames) {
                orderedValues.add(valuesByColumn.get(columnName));
            }
            values.add(orderedValues);
        }
        return new CanonicalRows(columnNames, List.copyOf(values));
    }

    private List<String> collectAttributesForObject(
        List<QualifiedReference> requestedReferences,
        String objectName,
        String requiredJoinAttribute
    ) {
        List<String> attributes = new ArrayList<>();
        attributes.add(requiredJoinAttribute);
        for (QualifiedReference reference : requestedReferences) {
            if (reference.objectName().equals(objectName)) {
                attributes.add(reference.qualifiedName());
            }
        }
        return attributes;
    }

    private List<String> dedupePreservingOrder(List<String> values) {
        return new ArrayList<>(new LinkedHashSet<>(values));
    }

    private String qualify(String objectName, String attributeName) {
        return objectName + "." + slug(attributeName);
    }

    private String slug(String rawName) {
        return rawName.trim().toLowerCase().replaceAll("[^a-z0-9]+", "_").replaceAll("^_+|_+$", "");
    }

    private String renderScalar(Object value) {
        if (value instanceof String stringValue) {
            return "\"" + stringValue.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
        }
        if (value instanceof Boolean booleanValue) {
            return Boolean.toString(booleanValue);
        }
        return String.valueOf(value);
    }

    private record QualifiedReference(String objectName, String attributeName) {
        private String qualifiedName() {
            return objectName + "." + attributeName;
        }
    }

    private record CanonicalRows(List<String> columnNames, List<List<Object>> rows) {
    }

    private static final class Builder {
        private final List<Node> nodes = new ArrayList<>();
        private int nextId;

        private Node addNode(AstNodeType nodeType) {
            Node node = new Node(nextId++, nodeType);
            nodes.add(node);
            return node;
        }

        private String toIndexedAstText() {
            StringBuilder builder = new StringBuilder();
            for (int index = 0; index < nodes.size(); index++) {
                Node node = nodes.get(index);
                builder.append("Node ").append(node.id).append('\n');
                builder.append("type: ").append(node.nodeType.name()).append('\n');
                if (!node.children.isEmpty()) {
                    builder.append("children: ").append(renderValue(node.children)).append('\n');
                } else if (!node.childRoles.isEmpty()) {
                    builder.append("childRoles:\n");
                    for (Map.Entry<String, Integer> entry : node.childRoles.entrySet()) {
                        builder.append("- ").append(entry.getKey()).append(": ").append(entry.getValue()).append('\n');
                    }
                }
                builder.append("params:\n");
                for (Map.Entry<String, Object> entry : node.params.entrySet()) {
                    builder.append("- ").append(entry.getKey()).append(": ").append(renderValue(entry.getValue())).append('\n');
                }
                if (index + 1 < nodes.size()) {
                    builder.append('\n');
                }
            }
            return builder.toString();
        }

        private String renderValue(Object value) {
            if (value instanceof String stringValue) {
                if (looksLikeRawFilter(stringValue)) {
                    return stringValue;
                }
                if (looksLikeEnum(stringValue)) {
                    return stringValue;
                }
                return "\"" + stringValue.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
            }
            if (value instanceof Boolean booleanValue) {
                return Boolean.toString(booleanValue);
            }
            if (value instanceof List<?> listValue) {
                return "[" + listValue.stream().map(this::renderValue).reduce((left, right) -> left + ", " + right).orElse("") + "]";
            }
            return String.valueOf(value);
        }

        private boolean looksLikeRawFilter(String value) {
            return value.startsWith("(") && value.endsWith(")");
        }

        private boolean looksLikeEnum(String value) {
            return value.equals(value.toUpperCase()) && value.chars().allMatch(character ->
                character == '_' || Character.isUpperCase(character) || Character.isDigit(character));
        }

        private static final class Node {
            private final int id;
            private final AstNodeType nodeType;
            private final List<Integer> children = new ArrayList<>();
            private final LinkedHashMap<String, Integer> childRoles = new LinkedHashMap<>();
            private final LinkedHashMap<String, Object> params = new LinkedHashMap<>();

            private Node(int id, AstNodeType nodeType) {
                this.id = id;
                this.nodeType = nodeType;
            }

            private int id() {
                return id;
            }

            private Node child(int childId) {
                children.add(childId);
                return this;
            }

            private Node childRole(String roleName, int childId) {
                childRoles.put(roleName, childId);
                return this;
            }

            private Node param(String key, Object value) {
                params.put(key, value);
                return this;
            }
        }
    }
}
