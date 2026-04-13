package query_executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import query_executor.ast.AstDocument;
import query_executor.ast.AstNodeRecord;
import query_executor.ast.AstNodeType;
import query_executor.ast.IndexedAstParser;

public class IndexedAstParserTest {
    private final IndexedAstParser parser = new IndexedAstParser();

    @Test
    void parsesChildRolesListsAndFilterParams() {
        AstDocument document = parser.parse(
            """
            Node 0
            type: DataRead
            childRoles:
            - source: 1
            params:
            - objectName: people
            - requestedAttributeNames: [people.name, people.age]

            Node 1
            type: Selection
            params:
            - role: source
            - objectName: people
            - attributes: [people.name, people.age]
            - filter: (AND (GREATER_THAN age 20) (CONTAINS name "a"))
            """
        );

        AstNodeRecord root = document.root();
        AstNodeRecord selection = document.node(1);

        assertEquals(AstNodeType.DataRead, root.nodeType());
        assertEquals(1, root.childRoles().get("source"));
        assertEquals(List.of("people.name", "people.age"), root.params().get("requestedAttributeNames"));
        assertEquals(AstNodeType.Selection, selection.nodeType());
        assertInstanceOf(FilterExpression.class, selection.params().get("filter"));
        assertTrue(((FilterExpression) selection.params().get("filter")).referencedColumns().contains("age"));
    }

    @Test
    void rejectsDuplicateNodeIds() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> parser.parse(
                """
                Node 0
                type: DataRead
                params:

                Node 0
                type: Selection
                params:
                - objectName: people
                - attributes: [people.name]
                """
            )
        );

        assertTrue(exception.getMessage().contains("Duplicate"));
    }

    @Test
    void rejectsMalformedFilterExpressions() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> parser.parse(
                """
                Node 0
                type: Selection
                params:
                - objectName: people
                - attributes: [people.name]
                - filter: (AND (GREATER_THAN age 20)
                """
            )
        );

        assertTrue(exception.getMessage().contains("filter"));
    }

    @Test
    void parsesLiteralRowsNestedLists() {
        AstDocument document = parser.parse(
            """
            Node 0
            type: DataCreate
            childRoles:
            - valueExpression: 1
            params:
            - objectName: people
            - targetNames: [people.name, people.age, people.bonus]

            Node 1
            type: LiteralRows
            params:
            - objectName: people
            - columnNames: [name, age, bonus]
            - rows: [["Bob", 30, 7], ["Cara", 25, 9]]
            """
        );

        AstNodeRecord literalRows = document.node(1);

        assertEquals(AstNodeType.LiteralRows, literalRows.nodeType());
        assertEquals(List.of("name", "age", "bonus"), literalRows.params().get("columnNames"));
        assertEquals(List.of(List.of("Bob", 30, 7), List.of("Cara", 25, 9)), literalRows.params().get("rows"));
    }
}
