package query_executor.nodes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import crud_engine.CrudEngine;
import crud_engine.DatabaseSchema;
import crud_engine.CrudEngineInterface.AttributeType;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import query_executor.ComparisonOperator;
import query_executor.Data;
import query_executor.DataInterface;
import query_executor.FilterCondition;

public class QueryExecutorIntegrationTest {
    @TempDir
    Path tempDir;

    @Test
    void readSelectionTreeUsesPersistedCrudData() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db"))) {
            seedPeople(engine);

            DataReadNode root = new DataReadNode();
            root.selectionChild = new SelectionNode();
            root.selectionChild.objectName = "people";
            root.selectionChild.attributeNames = List.of("people.name", "people.age");
            root.selectionChild.filterExpression = new FilterCondition("age", ComparisonOperator.GREATER_THAN, 20);
            root.crudEngine = engine;
            root.dataContext = Data.fromCrudEngine(engine, "People");

            DataInterface result = root.execute();

            assertEquals(2, result.getRowCount());
            assertEquals(List.of("Bob", "Cara"), result.getColumnValues("people.name"));
            assertEquals(List.of(30, 25), result.getColumnValues("people.age"));
        }
    }

    @Test
    void readRowOperationTreeUsesPersistedCrudData() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db"))) {
            seedPeople(engine);

            DataReadNode root = new DataReadNode();
            root.rowOperationChild = new RowOperationNode();
            root.rowOperationChild.primarySelectionChild = new SelectionNode();
            root.rowOperationChild.primarySelectionChild.objectName = "people";
            root.rowOperationChild.primarySelectionChild.attributeNames = List.of("people.age");

            root.rowOperationChild.secondarySelectionChild = new SelectionNode();
            root.rowOperationChild.secondarySelectionChild.objectName = "people";
            root.rowOperationChild.secondarySelectionChild.attributeNames = List.of("people.bonus");

            root.rowOperationChild.operationKind = RowOperationKind.ADD;
            root.rowOperationChild.sourceNames = List.of("people.age", "people.bonus");
            root.rowOperationChild.outputColumnName = "people.total";
            root.crudEngine = engine;
            root.dataContext = Data.fromCrudEngine(engine, "People");

            DataInterface result = root.execute();

            assertEquals(List.of(25, 37, 34), result.getColumnValues("people.total"));
        }
    }

    @Test
    void readColumnOperationTreeUsesPersistedCrudData() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db"))) {
            seedPeople(engine);

            DataReadNode root = new DataReadNode();
            root.columnOperationChild = new ColumnOperationNode();
            root.columnOperationChild.primarySelectionChild = new SelectionNode();
            root.columnOperationChild.primarySelectionChild.objectName = "people";
            root.columnOperationChild.primarySelectionChild.attributeNames = List.of("people.age");
            root.columnOperationChild.operationKind = ColumnOperationKind.SUM;
            root.columnOperationChild.sourceColumnName = "people.age";
            root.columnOperationChild.outputValueName = "people.age_sum";
            root.crudEngine = engine;
            root.dataContext = Data.fromCrudEngine(engine, "People");

            DataInterface result = root.execute();

            assertEquals(1, result.getRowCount());
            assertEquals(List.of(75), result.getColumnValues("people.age_sum"));
        }
    }

    @Test
    void deleteValidationTreeUsesPersistedCrudData() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db"))) {
            seedPeople(engine);

            DataDeleteNode root = new DataDeleteNode();
            root.objectName = "people";
            root.targetNames = List.of("people.name", "people.age");
            root.selectionChild = new SelectionNode();
            root.selectionChild.objectName = "people";
            root.selectionChild.attributeNames = List.of("people.name", "people.age");
            root.selectionChild.filterExpression = new FilterCondition("age", ComparisonOperator.GREATER_THAN, 20);
            root.crudEngine = engine;
            root.dataContext = Data.fromCrudEngine(engine, "People");

            IllegalStateException exception = assertThrows(IllegalStateException.class, root::execute);
            assertTrue(exception.getMessage().contains("deletable"));
        }
    }

    @Test
    void createRowTreeAppendsPersistedRows() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db"))) {
            seedPeople(engine);

            DataCreateNode root = new DataCreateNode();
            root.objectName = "people";
            root.targetNames = List.of("people.name", "people.age", "people.bonus");
            root.selectionChild = new SelectionNode();
            root.selectionChild.objectName = "people";
            root.selectionChild.attributeNames = List.of("people.name", "people.age", "people.bonus");
            root.selectionChild.filterExpression = new FilterCondition("age", ComparisonOperator.GREATER_THAN, 20);
            root.crudEngine = engine;
            root.dataContext = Data.fromCrudEngine(engine, "People");

            DataInterface result = root.execute();

            assertEquals(5, engine.getRowCount("People"));
            assertEquals("Bob", engine.readString("People", "Name", 3));
            assertEquals(30, engine.readInt("People", "Age", 3));
            assertEquals("Cara", engine.readString("People", "Name", 4));
            assertEquals(25, engine.readInt("People", "Age", 4));
            assertEquals(5, result.getRowCount());
        }
    }

    @Test
    void createColumnTreePersistsDerivedAttribute() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db"))) {
            seedPeople(engine);

            DataCreateNode root = new DataCreateNode();
            root.objectName = "people";
            root.targetNames = List.of("people.total");
            root.createdColumnName = "Total";
            root.rowOperationChild = new RowOperationNode();
            root.rowOperationChild.primarySelectionChild = new SelectionNode();
            root.rowOperationChild.primarySelectionChild.objectName = "people";
            root.rowOperationChild.primarySelectionChild.attributeNames = List.of("people.age");
            root.rowOperationChild.secondarySelectionChild = new SelectionNode();
            root.rowOperationChild.secondarySelectionChild.objectName = "people";
            root.rowOperationChild.secondarySelectionChild.attributeNames = List.of("people.bonus");
            root.rowOperationChild.operationKind = RowOperationKind.ADD;
            root.rowOperationChild.sourceNames = List.of("people.age", "people.bonus");
            root.rowOperationChild.outputColumnName = "people.total";
            root.crudEngine = engine;
            root.dataContext = Data.fromCrudEngine(engine, "People");

            DataInterface result = root.execute();
            DatabaseSchema schema = engine.readSchema();

            assertTrue(schema.getObjects().get("people").getAttributes().containsKey("total"));
            assertEquals(25, engine.readInt("People", "Total", 0));
            assertEquals(37, engine.readInt("People", "Total", 1));
            assertEquals(34, engine.readInt("People", "Total", 2));
            assertTrue(result.hasColumn("people.total"));
        }
    }

    @Test
    void updateTreePersistsDerivedValuesIntoTargetColumn() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db"))) {
            seedPeople(engine);

            DataUpdateNode root = new DataUpdateNode();
            root.objectName = "people";
            root.targetNames = List.of("Age");
            root.targetSelectionChild = new SelectionNode();
            root.targetSelectionChild.objectName = "people";
            root.targetSelectionChild.attributeNames = List.of("people.age");
            root.valueRowOperationChild = new RowOperationNode();
            root.valueRowOperationChild.primarySelectionChild = new SelectionNode();
            root.valueRowOperationChild.primarySelectionChild.objectName = "people";
            root.valueRowOperationChild.primarySelectionChild.attributeNames = List.of("people.age");
            root.valueRowOperationChild.secondarySelectionChild = new SelectionNode();
            root.valueRowOperationChild.secondarySelectionChild.objectName = "people";
            root.valueRowOperationChild.secondarySelectionChild.attributeNames = List.of("people.bonus");
            root.valueRowOperationChild.operationKind = RowOperationKind.ADD;
            root.valueRowOperationChild.sourceNames = List.of("people.age", "people.bonus");
            root.valueRowOperationChild.outputColumnName = "people.updated_age";
            root.crudEngine = engine;
            root.dataContext = Data.fromCrudEngine(engine, "People");

            DataInterface result = root.execute();

            assertEquals(25, engine.readInt("People", "Age", 0));
            assertEquals(37, engine.readInt("People", "Age", 1));
            assertEquals(34, engine.readInt("People", "Age", 2));
            assertEquals(List.of(25, 37, 34), result.getColumnValues("people.age"));
        }
    }

    @Test
    void updateTreeBroadcastsScalarAcrossTargetRows() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db"))) {
            seedPeople(engine);

            DataUpdateNode root = new DataUpdateNode();
            root.objectName = "people";
            root.targetNames = List.of("Bonus");
            root.targetSelectionChild = new SelectionNode();
            root.targetSelectionChild.objectName = "people";
            root.targetSelectionChild.attributeNames = List.of("people.bonus", "people.age");
            root.targetSelectionChild.filterExpression = new FilterCondition("age", ComparisonOperator.GREATER_THAN, 20);
            root.valueColumnOperationChild = new ColumnOperationNode();
            root.valueColumnOperationChild.primarySelectionChild = new SelectionNode();
            root.valueColumnOperationChild.primarySelectionChild.objectName = "people";
            root.valueColumnOperationChild.primarySelectionChild.attributeNames = List.of("people.age");
            root.valueColumnOperationChild.operationKind = ColumnOperationKind.MAX;
            root.valueColumnOperationChild.sourceColumnName = "people.age";
            root.valueColumnOperationChild.outputValueName = "people.max_age";
            root.crudEngine = engine;
            root.dataContext = Data.fromCrudEngine(engine, "People");

            DataInterface result = root.execute();

            assertEquals(5, engine.readInt("People", "Bonus", 0));
            assertEquals(30, engine.readInt("People", "Bonus", 1));
            assertEquals(30, engine.readInt("People", "Bonus", 2));
            assertEquals(List.of(5, 30, 30), result.getColumnValues("people.bonus"));
        }
    }

    @Test
    void deleteRowTreeRemovesPersistedRows() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db"))) {
            seedPeople(engine);

            DataDeleteNode root = new DataDeleteNode();
            root.objectName = "people";
            root.targetNames = List.of("people.name", "people.age", "people.bonus");
            root.selectionChild = new SelectionNode();
            root.selectionChild.objectName = "people";
            root.selectionChild.attributeNames = List.of("people.name", "people.age", "people.bonus");
            root.selectionChild.filterExpression = new FilterCondition("age", ComparisonOperator.GREATER_THAN, 20);
            root.crudEngine = engine;
            root.dataContext = Data.fromCrudEngine(engine, "People");

            DataInterface result = root.execute();

            assertEquals(1, engine.getRowCount("People"));
            assertEquals("Ada", engine.readString("People", "Name", 0));
            assertEquals(1, result.getRowCount());
        }
    }

    @Test
    void deleteColumnTreeRemovesPersistedAttribute() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db"))) {
            seedPeople(engine);

            DataDeleteNode root = new DataDeleteNode();
            root.objectName = "people";
            root.targetNames = List.of("Bonus");
            root.selectionChild = new SelectionNode();
            root.selectionChild.objectName = "people";
            root.selectionChild.attributeNames = List.of("people.bonus");
            root.crudEngine = engine;
            root.dataContext = Data.fromCrudEngine(engine, "People");

            DataInterface result = root.execute();
            DatabaseSchema schema = engine.readSchema();

            assertTrue(!schema.getObjects().get("people").getAttributes().containsKey("bonus"));
            assertEquals(List.of("people.name", "people.age"), result.getColumnNames());
        }
    }

    private CrudEngine newEngine(Path dbRoot) throws Exception {
        CrudEngine engine = new CrudEngine(dbRoot);
        engine.initialize();
        return engine;
    }

    private void seedPeople(CrudEngine engine) throws Exception {
        engine.createObject("People", null);
        engine.createAttribute("People", "Name", AttributeType.STRING);
        engine.createAttribute("People", "Age", AttributeType.INT);
        engine.createAttribute("People", "Bonus", AttributeType.INT);

        int first = engine.insertRow("People");
        // Row 0 becomes Ada.
        engine.writeString("People", "Name", first, "Ada");
        engine.writeInt("People", "Age", first, 20);
        engine.writeInt("People", "Bonus", first, 5);

        int second = engine.insertRow("People");
        // Row 1 becomes Bob.
        engine.writeString("People", "Name", second, "Bob");
        engine.writeInt("People", "Age", second, 30);
        engine.writeInt("People", "Bonus", second, 7);

        int third = engine.insertRow("People");
        // Row 2 becomes Cara.
        engine.writeString("People", "Name", third, "Cara");
        engine.writeInt("People", "Age", third, 25);
        engine.writeInt("People", "Bonus", third, 9);
    }
}
