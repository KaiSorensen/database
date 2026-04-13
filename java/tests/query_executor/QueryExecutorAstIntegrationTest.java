package query_executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import crud_engine.CrudEngine;
import crud_engine.DatabaseSchema;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class QueryExecutorAstIntegrationTest {
    @TempDir
    Path tempDir;

    @Test
    void executesReadSelectionAst() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db-read-selection"))) {
            seedPeople(engine);
            QueryExecutionResult result = new QueryExecutor(engine).executeAstText(readSelectionAst());

            assertTrue(result.success());
            assertNotNull(result.returnedData());
            assertEquals(2, result.returnedData().getRowCount());
            assertEquals(java.util.List.of("Bob", "Cara"), result.returnedData().getColumnValues("people.name"));
        }
    }

    @Test
    void executesReadRowOperationAst() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db-read-row-op"))) {
            seedPeople(engine);
            QueryExecutionResult result = new QueryExecutor(engine).executeAstText(readRowOperationAst());

            assertTrue(result.success());
            assertEquals(java.util.List.of(25, 37, 34), result.returnedData().getColumnValues("people.total"));
        }
    }

    @Test
    void executesReadColumnOperationAst() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db-read-column-op"))) {
            seedPeople(engine);
            QueryExecutionResult result = new QueryExecutor(engine).executeAstText(readColumnOperationAst());

            assertTrue(result.success());
            assertEquals(java.util.List.of(75), result.returnedData().getColumnValues("people.age_sum"));
        }
    }

    @Test
    void executesJoinReadAst() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db-join-read"))) {
            seedPeople(engine);
            seedPets(engine);
            QueryExecutionResult result = new QueryExecutor(engine).executeAstText(joinReadAst());

            assertTrue(result.success());
            assertEquals(2, result.returnedData().getRowCount());
            assertTrue(result.returnedData().hasColumn("pets.pet_name"));
        }
    }

    @Test
    void executesCreateRowAst() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db-create-row"))) {
            seedPeople(engine);
            QueryExecutionResult result = new QueryExecutor(engine).executeAstText(createRowAst());

            assertTrue(result.success());
            assertEquals(5, engine.getRowCount("People"));
            assertEquals("Bob", engine.readString("People", "Name", 3));
            assertEquals("Cara", engine.readString("People", "Name", 4));
        }
    }

    @Test
    void executesCreateLiteralRowsAst() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db-create-literal-row"))) {
            seedPeople(engine);
            QueryExecutionResult result = new QueryExecutor(engine).executeAstText(createLiteralRowsAst());

            assertTrue(result.success());
            assertEquals(5, engine.getRowCount("People"));
            assertEquals("Dora", engine.readString("People", "Name", 3));
            assertEquals(40, engine.readInt("People", "Age", 3));
            assertEquals(11, engine.readInt("People", "Bonus", 3));
            assertEquals("Evan", engine.readString("People", "Name", 4));
            assertEquals(21, engine.readInt("People", "Age", 4));
            assertEquals(3, engine.readInt("People", "Bonus", 4));
        }
    }

    @Test
    void executesCreateDerivedColumnAst() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db-create-col"))) {
            seedPeople(engine);
            QueryExecutionResult result = new QueryExecutor(engine).executeAstText(createDerivedColumnAst());
            DatabaseSchema schema = engine.readSchema();

            assertTrue(result.success());
            assertTrue(schema.getObjects().get("people").getAttributes().containsKey("total"));
            assertEquals(25, engine.readInt("People", "Total", 0));
            assertEquals(37, engine.readInt("People", "Total", 1));
            assertEquals(34, engine.readInt("People", "Total", 2));
        }
    }

    @Test
    void executesRowAlignedUpdateAst() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db-update-row"))) {
            seedPeople(engine);
            QueryExecutionResult result = new QueryExecutor(engine).executeAstText(updateRowAlignedAst());

            assertTrue(result.success());
            assertEquals(25, engine.readInt("People", "Age", 0));
            assertEquals(37, engine.readInt("People", "Age", 1));
            assertEquals(34, engine.readInt("People", "Age", 2));
        }
    }

    @Test
    void executesScalarBroadcastUpdateAst() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db-update-scalar"))) {
            seedPeople(engine);
            QueryExecutionResult result = new QueryExecutor(engine).executeAstText(updateScalarBroadcastAst());

            assertTrue(result.success());
            assertEquals(5, engine.readInt("People", "Bonus", 0));
            assertEquals(30, engine.readInt("People", "Bonus", 1));
            assertEquals(30, engine.readInt("People", "Bonus", 2));
        }
    }

    @Test
    void executesRowDeleteAst() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db-delete-row"))) {
            seedPeople(engine);
            QueryExecutionResult result = new QueryExecutor(engine).executeAstText(deleteRowAst());

            assertTrue(result.success());
            assertEquals(1, engine.getRowCount("People"));
            assertEquals("Ada", engine.readString("People", "Name", 0));
        }
    }

    @Test
    void executesColumnDeleteAst() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db-delete-col"))) {
            seedPeople(engine);
            QueryExecutionResult result = new QueryExecutor(engine).executeAstText(deleteColumnAst());
            DatabaseSchema schema = engine.readSchema();

            assertTrue(result.success());
            assertFalse(schema.getObjects().get("people").getAttributes().containsKey("bonus"));
        }
    }

    @Test
    void failsInvalidUpdateFitBeforeAnyWriteBegins() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db-update-fail"))) {
            seedPeople(engine);
            QueryExecutionResult result = new QueryExecutor(engine).executeAstText(invalidUpdateFitAst());

            assertFalse(result.success());
            // The failed update should leave stored data untouched because writes are staged first.
            assertEquals(20, engine.readInt("People", "Age", 0));
            assertEquals(30, engine.readInt("People", "Age", 1));
            assertEquals(25, engine.readInt("People", "Age", 2));
        }
    }

    private static CrudEngine newEngine(Path path) throws Exception {
        CrudEngine engine = new CrudEngine(path);
        engine.initialize();
        return engine;
    }

    private static void seedPeople(CrudEngine engine) throws Exception {
        engine.createObject("People", null);
        engine.createAttribute("People", "Name", crud_engine.CrudEngineInterface.AttributeType.STRING);
        engine.createAttribute("People", "Age", crud_engine.CrudEngineInterface.AttributeType.INT);
        engine.createAttribute("People", "Bonus", crud_engine.CrudEngineInterface.AttributeType.INT);

        int row0 = engine.insertRow("People");
        engine.writeString("People", "Name", row0, "Ada");
        engine.writeInt("People", "Age", row0, 20);
        engine.writeInt("People", "Bonus", row0, 5);

        int row1 = engine.insertRow("People");
        engine.writeString("People", "Name", row1, "Bob");
        engine.writeInt("People", "Age", row1, 30);
        engine.writeInt("People", "Bonus", row1, 7);

        int row2 = engine.insertRow("People");
        engine.writeString("People", "Name", row2, "Cara");
        engine.writeInt("People", "Age", row2, 25);
        engine.writeInt("People", "Bonus", row2, 9);
    }

    private static void seedPets(CrudEngine engine) throws Exception {
        engine.createObject("Pets", null);
        engine.createAttribute("Pets", "Owner_Name", crud_engine.CrudEngineInterface.AttributeType.STRING);
        engine.createAttribute("Pets", "Pet_Name", crud_engine.CrudEngineInterface.AttributeType.STRING);

        int row0 = engine.insertRow("Pets");
        engine.writeString("Pets", "Owner_Name", row0, "Ada");
        engine.writeString("Pets", "Pet_Name", row0, "Milo");

        int row1 = engine.insertRow("Pets");
        engine.writeString("Pets", "Owner_Name", row1, "Cara");
        engine.writeString("Pets", "Pet_Name", row1, "Nora");
    }

    private static String readSelectionAst() {
        return """
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
            """;
    }

    private static String readRowOperationAst() {
        return """
            Node 0
            type: DataRead
            childRoles:
            - source: 1
            params:

            Node 1
            type: RowOperation
            children: [2, 3]
            params:
            - operationKind: ADD
            - sourceNames: [people.age, people.bonus]
            - outputColumnName: people.total

            Node 2
            type: Selection
            params:
            - objectName: people
            - attributes: [people.age]

            Node 3
            type: Selection
            params:
            - objectName: people
            - attributes: [people.bonus]
            """;
    }

    private static String readColumnOperationAst() {
        return """
            Node 0
            type: DataRead
            childRoles:
            - source: 1
            params:

            Node 1
            type: ColumnOperation
            children: [2]
            params:
            - operationKind: SUM
            - sourceColumnName: people.age
            - outputValueName: people.age_sum

            Node 2
            type: Selection
            params:
            - objectName: people
            - attributes: [people.age]
            """;
    }

    private static String joinReadAst() {
        return """
            Node 0
            type: DataRead
            childRoles:
            - source: 1
            params:

            Node 1
            type: Join
            childRoles:
            - left: 2
            - right: 3
            params:
            - joinKind: INNER
            - leftObjectName: people
            - rightObjectName: pets
            - leftColumnName: people.name
            - rightColumnName: pets.owner_name

            Node 2
            type: DataRead
            childRoles:
            - source: 4
            params:

            Node 3
            type: DataRead
            childRoles:
            - source: 5
            params:

            Node 4
            type: Selection
            params:
            - objectName: people
            - attributes: [people.name]

            Node 5
            type: Selection
            params:
            - objectName: pets
            - attributes: [pets.owner_name, pets.pet_name]
            """;
    }

    private static String createRowAst() {
        return """
            Node 0
            type: DataCreate
            childRoles:
            - valueExpression: 1
            params:
            - objectName: people
            - targetNames: [people.name, people.age, people.bonus]

            Node 1
            type: Selection
            params:
            - objectName: people
            - attributes: [people.name, people.age, people.bonus]
            - filter: (GREATER_THAN age 20)
            """;
    }

    private static String createDerivedColumnAst() {
        return """
            Node 0
            type: DataCreate
            childRoles:
            - valueExpression: 1
            params:
            - objectName: people
            - targetNames: [people.total]
            - createdColumnName: Total

            Node 1
            type: RowOperation
            children: [2, 3]
            params:
            - operationKind: ADD
            - sourceNames: [people.age, people.bonus]
            - outputColumnName: people.total

            Node 2
            type: Selection
            params:
            - objectName: people
            - attributes: [people.age]

            Node 3
            type: Selection
            params:
            - objectName: people
            - attributes: [people.bonus]
            """;
    }

    private static String createLiteralRowsAst() {
        return """
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
            - rows: [["Dora", 40, 11], ["Evan", 21, 3]]
            """;
    }

    private static String updateRowAlignedAst() {
        return """
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
            """;
    }

    private static String updateScalarBroadcastAst() {
        return """
            Node 0
            type: DataUpdate
            childRoles:
            - targetSelection: 1
            - valueExpression: 2
            params:
            - objectName: people
            - targetNames: [Bonus]

            Node 1
            type: Selection
            params:
            - role: targetSelection
            - objectName: people
            - attributes: [people.bonus, people.age]
            - filter: (GREATER_THAN age 20)

            Node 2
            type: ColumnOperation
            children: [3]
            params:
            - operationKind: MAX
            - sourceColumnName: people.age
            - outputValueName: people.max_age

            Node 3
            type: Selection
            params:
            - objectName: people
            - attributes: [people.age]
            """;
    }

    private static String deleteRowAst() {
        return """
            Node 0
            type: DataDelete
            childRoles:
            - targetSelection: 1
            params:
            - objectName: people
            - targetNames: [people.name, people.age, people.bonus]

            Node 1
            type: Selection
            params:
            - objectName: people
            - attributes: [people.name, people.age, people.bonus]
            - filter: (GREATER_THAN age 20)
            """;
    }

    private static String deleteColumnAst() {
        return """
            Node 0
            type: DataDelete
            childRoles:
            - targetSelection: 1
            params:
            - objectName: people
            - targetNames: [people.bonus]

            Node 1
            type: Selection
            params:
            - objectName: people
            - attributes: [people.bonus]
            """;
    }

    private static String invalidUpdateFitAst() {
        return """
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
            type: Selection
            params:
            - objectName: people
            - attributes: [people.bonus]
            - filter: (GREATER_THAN age 20)
            """;
    }
}
