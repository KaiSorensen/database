package language_parser.ast;

import java.util.List;

public final class AstNodes {
    private AstNodes() {}

    public record ScriptNode(List<StatementNode> statements) {
        public ScriptNode {
            statements = List.copyOf(statements);
        }
    }

    public sealed interface StatementNode permits
        ObjectStatementNode,
        AttributeStatementNode,
        ReadStatementNode,
        CreateRowsStatementNode,
        UpdateStatementNode,
        DeleteRowsStatementNode,
        DeleteAttributesStatementNode {
    }

    public enum ObjectStatementKind {
        CREATE,
        READ,
        UPDATE,
        DELETE
    }

    public record ObjectStatementNode(
        ObjectStatementKind kind,
        String objectName,
        String parentObjectName,
        String newObjectName,
        boolean includeAttributeMetadata,
        boolean includeRowCount
    ) implements StatementNode {
    }

    public enum AttributeStatementKind {
        CREATE,
        READ,
        UPDATE,
        DELETE
    }

    public enum AttributeTypeName {
        INT,
        STRING,
        BOOL,
        ID
    }

    public record AttributeStatementNode(
        AttributeStatementKind kind,
        String objectName,
        String attributeName,
        AttributeTypeName attributeType,
        String newAttributeName,
        boolean includeAttributeType,
        RowExpressionNode derivedExpression
    ) implements StatementNode {
    }

    public record ReadStatementNode(
        SourceNode source,
        ProjectionNode projection,
        PredicateExpressionNode predicate
    ) implements StatementNode {
    }

    public record CreateRowsStatementNode(
        String objectName,
        List<RowLiteralNode> rows
    ) implements StatementNode {
        public CreateRowsStatementNode {
            rows = List.copyOf(rows);
        }
    }

    public record UpdateStatementNode(
        String objectName,
        String attributeName,
        ValueExpressionNode valueExpression,
        PredicateExpressionNode predicate
    ) implements StatementNode {
    }

    public record DeleteRowsStatementNode(
        String objectName,
        PredicateExpressionNode predicate
    ) implements StatementNode {
    }

    public record DeleteAttributesStatementNode(
        String objectName,
        List<ReferenceNode> attributes
    ) implements StatementNode {
        public DeleteAttributesStatementNode {
            attributes = List.copyOf(attributes);
        }
    }

    public sealed interface SourceNode permits ObjectSourceNode, JoinSourceNode {
    }

    public record ObjectSourceNode(String objectName) implements SourceNode {
    }

    public record JoinSourceNode(
        SourceNode left,
        SourceNode right,
        ReferenceNode leftReference,
        ReferenceNode rightReference
    ) implements SourceNode {
    }

    public sealed interface ProjectionNode permits
        AttributeProjectionNode,
        DeriveProjectionNode,
        SummarizeProjectionNode {
    }

    public record AttributeProjectionNode(List<ReferenceNode> attributes) implements ProjectionNode {
        public AttributeProjectionNode {
            attributes = List.copyOf(attributes);
        }
    }

    public record DeriveProjectionNode(
        String outputName,
        RowExpressionNode expression
    ) implements ProjectionNode {
    }

    public record SummarizeProjectionNode(
        String outputName,
        ColumnExpressionNode expression
    ) implements ProjectionNode {
    }

    public sealed interface ValueExpressionNode permits
        ReferenceNode,
        LiteralNode,
        RowExpressionNode,
        ColumnExpressionNode {
    }

    public record ReferenceNode(
        String objectNameNullable,
        String attributeName
    ) implements ValueExpressionNode {
        public boolean qualified() {
            return objectNameNullable != null && !objectNameNullable.isBlank();
        }
    }

    public record LiteralNode(Object value) implements ValueExpressionNode {
    }

    public enum RowOperationName {
        ADD,
        SUBTRACT,
        MULTIPLY,
        DIVIDE,
        MODULO,
        CONTAINS
    }

    public record RowExpressionNode(
        RowOperationName operationName,
        List<ReferenceNode> arguments
    ) implements ValueExpressionNode {
        public RowExpressionNode {
            arguments = List.copyOf(arguments);
        }
    }

    public enum ColumnOperationName {
        SUM,
        AVERAGE,
        MIN,
        MAX,
        COUNT,
        MEDIAN,
        MODE
    }

    public record ColumnExpressionNode(
        ColumnOperationName operationName,
        ReferenceNode argument
    ) implements ValueExpressionNode {
    }

    public sealed interface PredicateExpressionNode permits
        ComparisonPredicateNode,
        BooleanPredicateNode {
    }

    public enum ComparisonOperationName {
        EQUALS,
        NOT_EQUALS,
        GREATER_THAN,
        LESS_THAN,
        GREATER_THAN_OR_EQUALS,
        LESS_THAN_OR_EQUALS,
        CONTAINS
    }

    public record ComparisonPredicateNode(
        ComparisonOperationName operationName,
        ValueExpressionNode left,
        ValueExpressionNode right
    ) implements PredicateExpressionNode {
    }

    public enum BooleanOperationName {
        AND,
        OR,
        NOT
    }

    public record BooleanPredicateNode(
        BooleanOperationName operationName,
        List<PredicateExpressionNode> children
    ) implements PredicateExpressionNode {
        public BooleanPredicateNode {
            children = List.copyOf(children);
        }
    }

    public record RowEntryNode(
        String attributeName,
        LiteralNode value
    ) {
    }

    public record RowLiteralNode(List<RowEntryNode> entries) {
        public RowLiteralNode {
            entries = List.copyOf(entries);
        }
    }
}
