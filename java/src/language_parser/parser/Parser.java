package language_parser.parser;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import language_parser.ast.AstNodes;
import language_parser.lexer.Lexer;
import language_parser.lexer.Token;
import language_parser.lexer.TokenType;

public final class Parser {
    private final Lexer lexer;
    private List<Token> tokens;
    private int currentIndex;

    public Parser() {
        this.lexer = new Lexer();
    }

    public AstNodes.ScriptNode parse(String source) {
        this.tokens = lexer.lex(source);
        this.currentIndex = 0;
        List<AstNodes.StatementNode> statements = new ArrayList<>();
        while (!isAtEnd()) {
            statements.add(parseStatement());
            consume(TokenType.SEMICOLON, "Expected ';' after statement.");
        }
        return new AstNodes.ScriptNode(statements);
    }

    private AstNodes.StatementNode parseStatement() {
        if (matchWord("create_object")) {
            return parseCreateObjectStatement();
        }
        if (matchWord("read_object")) {
            return parseReadObjectStatement();
        }
        if (matchWord("update_object")) {
            return parseUpdateObjectStatement();
        }
        if (matchWord("delete_object")) {
            return parseDeleteObjectStatement();
        }
        if (matchWord("create_attribute")) {
            return parseCreateAttributeStatement();
        }
        if (matchWord("read_attribute")) {
            return parseReadAttributeStatement();
        }
        if (matchWord("update_attribute")) {
            return parseUpdateAttributeStatement();
        }
        if (matchWord("delete_attribute")) {
            return parseDeleteAttributeStatement();
        }
        if (matchWord("read")) {
            return parseReadStatement();
        }
        if (matchWord("create_rows")) {
            return parseCreateRowsStatement();
        }
        if (matchWord("update")) {
            return parseUpdateStatement();
        }
        if (matchWord("delete_rows")) {
            return parseDeleteRowsStatement();
        }
        if (matchWord("delete_attributes")) {
            return parseDeleteAttributesStatement();
        }
        throw error(peek(), "Expected statement command.");
    }

    private AstNodes.ObjectStatementNode parseCreateObjectStatement() {
        consume(TokenType.LEFT_PAREN, "Expected '(' after create_object.");
        String objectName = consumeIdentifier("Expected object name.");
        String parentObjectName = null;
        if (match(TokenType.COMMA)) {
            consumeWord("extends", "Expected extends(...) clause.");
            consume(TokenType.LEFT_PAREN, "Expected '(' after extends.");
            parentObjectName = consumeIdentifier("Expected parent object name.");
            consume(TokenType.RIGHT_PAREN, "Expected ')' after extends parent.");
        }
        consume(TokenType.RIGHT_PAREN, "Expected ')' after create_object.");
        return new AstNodes.ObjectStatementNode(
            AstNodes.ObjectStatementKind.CREATE,
            objectName,
            parentObjectName,
            null,
            false,
            false
        );
    }

    private AstNodes.ObjectStatementNode parseReadObjectStatement() {
        consume(TokenType.LEFT_PAREN, "Expected '(' after read_object.");
        String objectName = consumeIdentifier("Expected object name.");
        boolean includeAttributes = false;
        boolean includeRowCount = false;
        while (match(TokenType.COMMA)) {
            if (matchWord("include_attributes")) {
                consume(TokenType.LEFT_PAREN, "Expected '(' after include_attributes.");
                consume(TokenType.RIGHT_PAREN, "Expected ')' after include_attributes.");
                includeAttributes = true;
            } else if (matchWord("include_row_count")) {
                consume(TokenType.LEFT_PAREN, "Expected '(' after include_row_count.");
                consume(TokenType.RIGHT_PAREN, "Expected ')' after include_row_count.");
                includeRowCount = true;
            } else {
                throw error(peek(), "Expected include_attributes() or include_row_count().");
            }
        }
        consume(TokenType.RIGHT_PAREN, "Expected ')' after read_object.");
        return new AstNodes.ObjectStatementNode(
            AstNodes.ObjectStatementKind.READ,
            objectName,
            null,
            null,
            includeAttributes,
            includeRowCount
        );
    }

    private AstNodes.ObjectStatementNode parseUpdateObjectStatement() {
        consume(TokenType.LEFT_PAREN, "Expected '(' after update_object.");
        String objectName = consumeIdentifier("Expected object name.");
        consume(TokenType.COMMA, "Expected ',' before rename clause.");
        String newName = parseRenameClause();
        consume(TokenType.RIGHT_PAREN, "Expected ')' after update_object.");
        return new AstNodes.ObjectStatementNode(
            AstNodes.ObjectStatementKind.UPDATE,
            objectName,
            null,
            newName,
            false,
            false
        );
    }

    private AstNodes.ObjectStatementNode parseDeleteObjectStatement() {
        consume(TokenType.LEFT_PAREN, "Expected '(' after delete_object.");
        String objectName = consumeIdentifier("Expected object name.");
        consume(TokenType.RIGHT_PAREN, "Expected ')' after delete_object.");
        return new AstNodes.ObjectStatementNode(
            AstNodes.ObjectStatementKind.DELETE,
            objectName,
            null,
            null,
            false,
            false
        );
    }

    private AstNodes.AttributeStatementNode parseCreateAttributeStatement() {
        consume(TokenType.LEFT_PAREN, "Expected '(' after create_attribute.");
        String objectName = consumeIdentifier("Expected object name.");
        consume(TokenType.COMMA, "Expected ',' after object name.");
        String attributeName = consumeIdentifier("Expected attribute name.");
        consume(TokenType.COMMA, "Expected ',' after attribute name.");
        AstNodes.AttributeTypeName attributeType = parseAttributeType();
        AstNodes.RowExpressionNode derivedExpression = null;
        if (match(TokenType.COMMA)) {
            consumeWord("derive", "Expected derive(...) clause.");
            consume(TokenType.LEFT_PAREN, "Expected '(' after derive.");
            derivedExpression = parseRowExpression();
            consume(TokenType.RIGHT_PAREN, "Expected ')' after derive expression.");
        }
        consume(TokenType.RIGHT_PAREN, "Expected ')' after create_attribute.");
        return new AstNodes.AttributeStatementNode(
            AstNodes.AttributeStatementKind.CREATE,
            objectName,
            attributeName,
            attributeType,
            null,
            false,
            derivedExpression
        );
    }

    private AstNodes.AttributeStatementNode parseReadAttributeStatement() {
        consume(TokenType.LEFT_PAREN, "Expected '(' after read_attribute.");
        String objectName = consumeIdentifier("Expected object name.");
        consume(TokenType.COMMA, "Expected ',' after object name.");
        String attributeName = consumeIdentifier("Expected attribute name.");
        boolean includeType = false;
        if (match(TokenType.COMMA)) {
            consumeWord("include_type", "Expected include_type() clause.");
            consume(TokenType.LEFT_PAREN, "Expected '(' after include_type.");
            consume(TokenType.RIGHT_PAREN, "Expected ')' after include_type.");
            includeType = true;
        }
        consume(TokenType.RIGHT_PAREN, "Expected ')' after read_attribute.");
        return new AstNodes.AttributeStatementNode(
            AstNodes.AttributeStatementKind.READ,
            objectName,
            attributeName,
            null,
            null,
            includeType,
            null
        );
    }

    private AstNodes.AttributeStatementNode parseUpdateAttributeStatement() {
        consume(TokenType.LEFT_PAREN, "Expected '(' after update_attribute.");
        String objectName = consumeIdentifier("Expected object name.");
        consume(TokenType.COMMA, "Expected ',' after object name.");
        String attributeName = consumeIdentifier("Expected attribute name.");
        consume(TokenType.COMMA, "Expected ',' before rename clause.");
        String newName = parseRenameClause();
        consume(TokenType.RIGHT_PAREN, "Expected ')' after update_attribute.");
        return new AstNodes.AttributeStatementNode(
            AstNodes.AttributeStatementKind.UPDATE,
            objectName,
            attributeName,
            null,
            newName,
            false,
            null
        );
    }

    private AstNodes.AttributeStatementNode parseDeleteAttributeStatement() {
        consume(TokenType.LEFT_PAREN, "Expected '(' after delete_attribute.");
        String objectName = consumeIdentifier("Expected object name.");
        consume(TokenType.COMMA, "Expected ',' after object name.");
        String attributeName = consumeIdentifier("Expected attribute name.");
        consume(TokenType.RIGHT_PAREN, "Expected ')' after delete_attribute.");
        return new AstNodes.AttributeStatementNode(
            AstNodes.AttributeStatementKind.DELETE,
            objectName,
            attributeName,
            null,
            null,
            false,
            null
        );
    }

    private AstNodes.ReadStatementNode parseReadStatement() {
        consume(TokenType.LEFT_PAREN, "Expected '(' after read.");
        AstNodes.SourceNode source = parseSource();
        consume(TokenType.COMMA, "Expected ',' after source.");
        AstNodes.ProjectionNode projection = parseProjection();
        AstNodes.PredicateExpressionNode predicate = null;
        if (match(TokenType.COMMA)) {
            predicate = parseWhereClause();
        }
        consume(TokenType.RIGHT_PAREN, "Expected ')' after read.");
        return new AstNodes.ReadStatementNode(source, projection, predicate);
    }

    private AstNodes.CreateRowsStatementNode parseCreateRowsStatement() {
        consume(TokenType.LEFT_PAREN, "Expected '(' after create_rows.");
        String objectName = consumeIdentifier("Expected object name.");
        consume(TokenType.COMMA, "Expected ',' after object name.");
        consumeWord("values", "Expected values(...) clause.");
        consume(TokenType.LEFT_PAREN, "Expected '(' after values.");
        List<AstNodes.RowLiteralNode> rows = new ArrayList<>();
        rows.add(parseRowLiteral());
        while (match(TokenType.COMMA)) {
            rows.add(parseRowLiteral());
        }
        consume(TokenType.RIGHT_PAREN, "Expected ')' after values.");
        consume(TokenType.RIGHT_PAREN, "Expected ')' after create_rows.");
        return new AstNodes.CreateRowsStatementNode(objectName, rows);
    }

    private AstNodes.UpdateStatementNode parseUpdateStatement() {
        consume(TokenType.LEFT_PAREN, "Expected '(' after update.");
        String objectName = consumeIdentifier("Expected object name.");
        consume(TokenType.COMMA, "Expected ',' after object name.");
        consumeWord("set", "Expected set(...) clause.");
        consume(TokenType.LEFT_PAREN, "Expected '(' after set.");
        String attributeName = consumeIdentifier("Expected target attribute name.");
        consume(TokenType.COMMA, "Expected ',' after target attribute.");
        AstNodes.ValueExpressionNode valueExpression = parseUpdateValueExpression();
        consume(TokenType.RIGHT_PAREN, "Expected ')' after set value.");
        AstNodes.PredicateExpressionNode predicate = null;
        if (match(TokenType.COMMA)) {
            predicate = parseWhereClause();
        }
        consume(TokenType.RIGHT_PAREN, "Expected ')' after update.");
        return new AstNodes.UpdateStatementNode(objectName, attributeName, valueExpression, predicate);
    }

    private AstNodes.DeleteRowsStatementNode parseDeleteRowsStatement() {
        consume(TokenType.LEFT_PAREN, "Expected '(' after delete_rows.");
        String objectName = consumeIdentifier("Expected object name.");
        AstNodes.PredicateExpressionNode predicate = null;
        if (match(TokenType.COMMA)) {
            predicate = parseWhereClause();
        }
        consume(TokenType.RIGHT_PAREN, "Expected ')' after delete_rows.");
        return new AstNodes.DeleteRowsStatementNode(objectName, predicate);
    }

    private AstNodes.DeleteAttributesStatementNode parseDeleteAttributesStatement() {
        consume(TokenType.LEFT_PAREN, "Expected '(' after delete_attributes.");
        String objectName = consumeIdentifier("Expected object name.");
        consume(TokenType.COMMA, "Expected ',' after object name.");
        consumeWord("attributes", "Expected attributes(...) clause.");
        consume(TokenType.LEFT_PAREN, "Expected '(' after attributes.");
        List<AstNodes.ReferenceNode> attributes = new ArrayList<>();
        attributes.add(parseReference());
        while (match(TokenType.COMMA)) {
            attributes.add(parseReference());
        }
        consume(TokenType.RIGHT_PAREN, "Expected ')' after attributes.");
        consume(TokenType.RIGHT_PAREN, "Expected ')' after delete_attributes.");
        return new AstNodes.DeleteAttributesStatementNode(objectName, attributes);
    }

    private AstNodes.SourceNode parseSource() {
        if (matchWord("join")) {
            consume(TokenType.LEFT_PAREN, "Expected '(' after join.");
            AstNodes.SourceNode left = parseSource();
            consume(TokenType.COMMA, "Expected ',' after left join source.");
            AstNodes.SourceNode right = parseSource();
            consume(TokenType.COMMA, "Expected ',' before on(...) clause.");
            consumeWord("on", "Expected on(...) clause.");
            consume(TokenType.LEFT_PAREN, "Expected '(' after on.");
            AstNodes.PredicateExpressionNode predicate = parsePredicate();
            consume(TokenType.RIGHT_PAREN, "Expected ')' after on predicate.");
            consume(TokenType.RIGHT_PAREN, "Expected ')' after join.");
            if (!(predicate instanceof AstNodes.ComparisonPredicateNode comparison)
                || comparison.operationName() != AstNodes.ComparisonOperationName.EQUALS
                || !(comparison.left() instanceof AstNodes.ReferenceNode leftReference)
                || !(comparison.right() instanceof AstNodes.ReferenceNode rightReference)) {
                throw error(previous(), "Join on(...) must contain equals(left_ref, right_ref).");
            }
            return new AstNodes.JoinSourceNode(left, right, leftReference, rightReference);
        }
        return new AstNodes.ObjectSourceNode(consumeIdentifier("Expected object source name."));
    }

    private AstNodes.ProjectionNode parseProjection() {
        if (matchWord("attributes")) {
            consume(TokenType.LEFT_PAREN, "Expected '(' after attributes.");
            List<AstNodes.ReferenceNode> attributes = new ArrayList<>();
            attributes.add(parseReference());
            while (match(TokenType.COMMA)) {
                attributes.add(parseReference());
            }
            consume(TokenType.RIGHT_PAREN, "Expected ')' after attributes.");
            return new AstNodes.AttributeProjectionNode(attributes);
        }
        if (matchWord("derive")) {
            consume(TokenType.LEFT_PAREN, "Expected '(' after derive.");
            String outputName = consumeIdentifier("Expected derived output name.");
            consume(TokenType.COMMA, "Expected ',' after derived output name.");
            AstNodes.RowExpressionNode rowExpression = parseRowExpression();
            consume(TokenType.RIGHT_PAREN, "Expected ')' after derive.");
            return new AstNodes.DeriveProjectionNode(outputName, rowExpression);
        }
        if (matchWord("summarize")) {
            consume(TokenType.LEFT_PAREN, "Expected '(' after summarize.");
            String outputName = consumeIdentifier("Expected summarized output name.");
            consume(TokenType.COMMA, "Expected ',' after summarized output name.");
            AstNodes.ColumnExpressionNode columnExpression = parseColumnExpression();
            consume(TokenType.RIGHT_PAREN, "Expected ')' after summarize.");
            return new AstNodes.SummarizeProjectionNode(outputName, columnExpression);
        }
        throw error(peek(), "Expected attributes(...), derive(...), or summarize(...).");
    }

    private AstNodes.PredicateExpressionNode parseWhereClause() {
        consumeWord("where", "Expected where(...) clause.");
        consume(TokenType.LEFT_PAREN, "Expected '(' after where.");
        AstNodes.PredicateExpressionNode predicate = parsePredicate();
        consume(TokenType.RIGHT_PAREN, "Expected ')' after where predicate.");
        return predicate;
    }

    private AstNodes.PredicateExpressionNode parsePredicate() {
        if (matchWord("and")) {
            consume(TokenType.LEFT_PAREN, "Expected '(' after and.");
            List<AstNodes.PredicateExpressionNode> children = new ArrayList<>();
            children.add(parsePredicate());
            consume(TokenType.COMMA, "Expected ',' between and(...) predicates.");
            children.add(parsePredicate());
            while (match(TokenType.COMMA)) {
                children.add(parsePredicate());
            }
            consume(TokenType.RIGHT_PAREN, "Expected ')' after and predicates.");
            return new AstNodes.BooleanPredicateNode(AstNodes.BooleanOperationName.AND, children);
        }
        if (matchWord("or")) {
            consume(TokenType.LEFT_PAREN, "Expected '(' after or.");
            List<AstNodes.PredicateExpressionNode> children = new ArrayList<>();
            children.add(parsePredicate());
            consume(TokenType.COMMA, "Expected ',' between or(...) predicates.");
            children.add(parsePredicate());
            while (match(TokenType.COMMA)) {
                children.add(parsePredicate());
            }
            consume(TokenType.RIGHT_PAREN, "Expected ')' after or predicates.");
            return new AstNodes.BooleanPredicateNode(AstNodes.BooleanOperationName.OR, children);
        }
        if (matchWord("not")) {
            consume(TokenType.LEFT_PAREN, "Expected '(' after not.");
            AstNodes.PredicateExpressionNode child = parsePredicate();
            consume(TokenType.RIGHT_PAREN, "Expected ')' after not predicate.");
            return new AstNodes.BooleanPredicateNode(AstNodes.BooleanOperationName.NOT, List.of(child));
        }

        AstNodes.ComparisonOperationName operationName = parseComparisonOperationName();
        consume(TokenType.LEFT_PAREN, "Expected '(' after comparison function.");
        AstNodes.ValueExpressionNode left = parsePredicateValue();
        consume(TokenType.COMMA, "Expected ',' after left comparison operand.");
        AstNodes.ValueExpressionNode right = parsePredicateValue();
        consume(TokenType.RIGHT_PAREN, "Expected ')' after comparison operands.");
        return new AstNodes.ComparisonPredicateNode(operationName, left, right);
    }

    private AstNodes.ValueExpressionNode parsePredicateValue() {
        if (check(TokenType.STRING) || check(TokenType.INTEGER) || check(TokenType.BOOLEAN)) {
            return parseLiteral();
        }
        return parseReference();
    }

    private AstNodes.ValueExpressionNode parseUpdateValueExpression() {
        if (isWord(peek(), "add")
            || isWord(peek(), "subtract")
            || isWord(peek(), "multiply")
            || isWord(peek(), "divide")
            || isWord(peek(), "modulo")
            || isWord(peek(), "contains")) {
            return parseRowExpression();
        }
        if (isWord(peek(), "sum")
            || isWord(peek(), "average")
            || isWord(peek(), "min")
            || isWord(peek(), "max")
            || isWord(peek(), "count")
            || isWord(peek(), "median")
            || isWord(peek(), "mode")) {
            return parseColumnExpression();
        }
        return parseReference();
    }

    private AstNodes.RowExpressionNode parseRowExpression() {
        AstNodes.RowOperationName operationName = parseRowOperationName();
        consume(TokenType.LEFT_PAREN, "Expected '(' after row expression.");
        List<AstNodes.ReferenceNode> arguments = new ArrayList<>();
        arguments.add(parseReference());
        consume(TokenType.COMMA, "Expected ',' between row expression arguments.");
        arguments.add(parseReference());
        while (match(TokenType.COMMA)) {
            arguments.add(parseReference());
        }
        consume(TokenType.RIGHT_PAREN, "Expected ')' after row expression.");
        return new AstNodes.RowExpressionNode(operationName, arguments);
    }

    private AstNodes.ColumnExpressionNode parseColumnExpression() {
        AstNodes.ColumnOperationName operationName = parseColumnOperationName();
        consume(TokenType.LEFT_PAREN, "Expected '(' after summarize function.");
        AstNodes.ReferenceNode argument = parseReference();
        consume(TokenType.RIGHT_PAREN, "Expected ')' after summarize function argument.");
        return new AstNodes.ColumnExpressionNode(operationName, argument);
    }

    private AstNodes.RowLiteralNode parseRowLiteral() {
        consumeWord("row", "Expected row(...) literal.");
        consume(TokenType.LEFT_PAREN, "Expected '(' after row.");
        List<AstNodes.RowEntryNode> entries = new ArrayList<>();
        Set<String> seenKeys = new LinkedHashSet<>();
        entries.add(parseRowEntry(seenKeys));
        while (match(TokenType.COMMA)) {
            entries.add(parseRowEntry(seenKeys));
        }
        consume(TokenType.RIGHT_PAREN, "Expected ')' after row literal.");
        return new AstNodes.RowLiteralNode(entries);
    }

    private AstNodes.RowEntryNode parseRowEntry(Set<String> seenKeys) {
        String attributeName = consumeIdentifier("Expected row entry attribute name.");
        String lowered = attributeName.toLowerCase();
        if (!seenKeys.add(lowered)) {
            throw error(previous(), "Duplicate row entry attribute: " + attributeName);
        }
        consume(TokenType.LEFT_PAREN, "Expected '(' after row entry name.");
        AstNodes.LiteralNode literal = parseLiteral();
        consume(TokenType.RIGHT_PAREN, "Expected ')' after row entry literal.");
        return new AstNodes.RowEntryNode(attributeName, literal);
    }

    private AstNodes.ReferenceNode parseReference() {
        String left = consumeIdentifier("Expected identifier.");
        if (match(TokenType.DOT)) {
            String attributeName = consumeIdentifier("Expected attribute name after '.'.");
            return new AstNodes.ReferenceNode(left, attributeName);
        }
        return new AstNodes.ReferenceNode(null, left);
    }

    private AstNodes.LiteralNode parseLiteral() {
        if (match(TokenType.STRING)) {
            return new AstNodes.LiteralNode(previous().literalValue());
        }
        if (match(TokenType.INTEGER)) {
            return new AstNodes.LiteralNode(previous().literalValue());
        }
        if (match(TokenType.BOOLEAN)) {
            return new AstNodes.LiteralNode(previous().literalValue());
        }
        throw error(peek(), "Expected literal value.");
    }

    private AstNodes.AttributeTypeName parseAttributeType() {
        if (matchWord("int")) {
            return AstNodes.AttributeTypeName.INT;
        }
        if (matchWord("string")) {
            return AstNodes.AttributeTypeName.STRING;
        }
        if (matchWord("bool")) {
            return AstNodes.AttributeTypeName.BOOL;
        }
        if (matchWord("id")) {
            return AstNodes.AttributeTypeName.ID;
        }
        throw error(peek(), "Expected attribute type.");
    }

    private AstNodes.RowOperationName parseRowOperationName() {
        if (matchWord("add")) {
            return AstNodes.RowOperationName.ADD;
        }
        if (matchWord("subtract")) {
            return AstNodes.RowOperationName.SUBTRACT;
        }
        if (matchWord("multiply")) {
            return AstNodes.RowOperationName.MULTIPLY;
        }
        if (matchWord("divide")) {
            return AstNodes.RowOperationName.DIVIDE;
        }
        if (matchWord("modulo")) {
            return AstNodes.RowOperationName.MODULO;
        }
        if (matchWord("contains")) {
            return AstNodes.RowOperationName.CONTAINS;
        }
        throw error(peek(), "Expected row operation.");
    }

    private AstNodes.ColumnOperationName parseColumnOperationName() {
        if (matchWord("sum")) {
            return AstNodes.ColumnOperationName.SUM;
        }
        if (matchWord("average")) {
            return AstNodes.ColumnOperationName.AVERAGE;
        }
        if (matchWord("min")) {
            return AstNodes.ColumnOperationName.MIN;
        }
        if (matchWord("max")) {
            return AstNodes.ColumnOperationName.MAX;
        }
        if (matchWord("count")) {
            return AstNodes.ColumnOperationName.COUNT;
        }
        if (matchWord("median")) {
            return AstNodes.ColumnOperationName.MEDIAN;
        }
        if (matchWord("mode")) {
            return AstNodes.ColumnOperationName.MODE;
        }
        throw error(peek(), "Expected summarize operation.");
    }

    private AstNodes.ComparisonOperationName parseComparisonOperationName() {
        if (matchWord("equals")) {
            return AstNodes.ComparisonOperationName.EQUALS;
        }
        if (matchWord("not_equals")) {
            return AstNodes.ComparisonOperationName.NOT_EQUALS;
        }
        if (matchWord("greater_than")) {
            return AstNodes.ComparisonOperationName.GREATER_THAN;
        }
        if (matchWord("less_than")) {
            return AstNodes.ComparisonOperationName.LESS_THAN;
        }
        if (matchWord("greater_than_or_equals")) {
            return AstNodes.ComparisonOperationName.GREATER_THAN_OR_EQUALS;
        }
        if (matchWord("less_than_or_equals")) {
            return AstNodes.ComparisonOperationName.LESS_THAN_OR_EQUALS;
        }
        if (matchWord("contains")) {
            return AstNodes.ComparisonOperationName.CONTAINS;
        }
        throw error(peek(), "Expected predicate function.");
    }

    private String parseRenameClause() {
        consumeWord("rename", "Expected rename(...) clause.");
        consume(TokenType.LEFT_PAREN, "Expected '(' after rename.");
        String newName = consumeIdentifier("Expected rename target.");
        consume(TokenType.RIGHT_PAREN, "Expected ')' after rename target.");
        return newName;
    }

    private boolean match(TokenType tokenType) {
        if (!check(tokenType)) {
            return false;
        }
        advance();
        return true;
    }

    private boolean matchWord(String lexeme) {
        if (!isWord(peek(), lexeme)) {
            return false;
        }
        advance();
        return true;
    }

    private boolean isWord(Token token, String lexeme) {
        return (token.tokenType() == TokenType.KEYWORD || token.tokenType() == TokenType.IDENTIFIER)
            && token.lexeme().equalsIgnoreCase(lexeme);
    }

    private Token consume(TokenType tokenType, String message) {
        if (check(tokenType)) {
            return advance();
        }
        throw error(peek(), message);
    }

    private void consumeWord(String lexeme, String message) {
        if (!matchWord(lexeme)) {
            throw error(peek(), message);
        }
    }

    private String consumeIdentifier(String message) {
        if (check(TokenType.IDENTIFIER)) {
            return advance().lexeme();
        }
        throw error(peek(), message);
    }

    private boolean check(TokenType tokenType) {
        if (isAtEnd()) {
            return tokenType == TokenType.EOF;
        }
        return peek().tokenType() == tokenType;
    }

    private Token advance() {
        if (!isAtEnd()) {
            currentIndex++;
        }
        return previous();
    }

    private boolean isAtEnd() {
        return peek().tokenType() == TokenType.EOF;
    }

    private Token peek() {
        return tokens.get(currentIndex);
    }

    private Token previous() {
        return tokens.get(currentIndex - 1);
    }

    private ParseException error(Token token, String message) {
        return new ParseException(message + " At line " + token.line() + ", column " + token.column() + ".");
    }
}
