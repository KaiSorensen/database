package query_executor.nodes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;
import query_executor.ast.AstDocument;
import query_executor.ast.IndexedAstParser;

public class IndexedAstBinderTest {
    private final IndexedAstParser parser = new IndexedAstParser();
    private final IndexedAstBinder binder = new IndexedAstBinder();

    @Test
    void bindsDataReadSelectionTree() {
        AstDocument document = parser.parse(
            """
            Node 0
            type: DataRead
            childRoles:
            - source: 1
            params:

            Node 1
            type: Selection
            params:
            - objectName: people
            - attributes: [people.name, people.age]
            - filter: (GREATER_THAN age 20)
            """
        );

        Node root = binder.bind(document);

        assertInstanceOf(DataReadNode.class, root);
        assertInstanceOf(SelectionNode.class, ((DataReadNode) root).selectionChild);
        assertEquals("people", ((DataReadNode) root).selectionChild.objectName);
    }

    @Test
    void bindsDataUpdateTargetAndValueExpression() {
        AstDocument document = parser.parse(
            """
            Node 0
            type: DataUpdate
            childRoles:
            - targetSelection: 1
            - valueExpression: 2
            params:
            - objectName: people
            - targetNames: [Age]

            Node 1
            type: Selection
            params:
            - role: targetSelection
            - objectName: people
            - attributes: [people.age]

            Node 2
            type: RowOperation
            children: [3, 4]
            params:
            - operationKind: ADD
            - sourceNames: [people.age, people.bonus]
            - outputColumnName: people.updated_age

            Node 3
            type: Selection
            params:
            - objectName: people
            - attributes: [people.age]

            Node 4
            type: Selection
            params:
            - objectName: people
            - attributes: [people.bonus]
            """
        );

        Node root = binder.bind(document);

        assertInstanceOf(DataUpdateNode.class, root);
        assertInstanceOf(SelectionNode.class, ((DataUpdateNode) root).targetSelectionChild);
        assertInstanceOf(RowOperationNode.class, ((DataUpdateNode) root).valueRowOperationChild);
    }

    @Test
    void bindsJoinWithNamedLeftAndRightChildren() {
        AstDocument document = parser.parse(
            """
            Node 0
            type: Join
            childRoles:
            - left: 1
            - right: 2
            params:
            - joinKind: INNER
            - leftObjectName: people
            - rightObjectName: pets
            - leftColumnName: people.id
            - rightColumnName: pets.owner_id

            Node 1
            type: DataRead
            childRoles:
            - source: 3
            params:

            Node 2
            type: DataRead
            childRoles:
            - source: 4
            params:

            Node 3
            type: Selection
            params:
            - objectName: people
            - attributes: [people.id]

            Node 4
            type: Selection
            params:
            - objectName: pets
            - attributes: [pets.owner_id]
            """
        );

        Node root = binder.bind(document);

        assertInstanceOf(JoinNode.class, root);
        assertInstanceOf(DataReadNode.class, ((JoinNode) root).leftChild);
        assertInstanceOf(DataReadNode.class, ((JoinNode) root).rightChild);
    }

    @Test
    void rejectsWrongChildTypeForDataUpdateTargetSelection() {
        AstDocument document = parser.parse(
            """
            Node 0
            type: DataUpdate
            childRoles:
            - targetSelection: 1
            - valueExpression: 2
            params:
            - objectName: people
            - targetNames: [Age]

            Node 1
            type: RowOperation
            children: [3]
            params:
            - operationKind: ADD
            - sourceNames: [people.age]
            - outputColumnName: people.updated_age

            Node 2
            type: Selection
            params:
            - objectName: people
            - attributes: [people.age]

            Node 3
            type: Selection
            params:
            - objectName: people
            - attributes: [people.age]
            """
        );

        assertThrows(IllegalArgumentException.class, () -> binder.bind(document));
    }

    @Test
    void bindsDataCreateLiteralRowsTree() {
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

        Node root = binder.bind(document);

        assertInstanceOf(DataCreateNode.class, root);
        assertInstanceOf(LiteralRowsNode.class, ((DataCreateNode) root).literalRowsChild);
        assertEquals(List.of("name", "age", "bonus"), ((DataCreateNode) root).literalRowsChild.columnNames);
    }
}
