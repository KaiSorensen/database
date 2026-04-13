package query_executor.nodes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import crud_engine.CrudEngineInterface;
import crud_engine.CrudEngineInterface.AttributeType;
import crud_engine.DatabaseSchema;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import query_executor.CellLocation;
import query_executor.CellValue;
import query_executor.ColumnData;
import query_executor.ComparisonOperator;
import query_executor.Data;
import query_executor.DataInterface;
import query_executor.FilterCondition;
import query_executor.JoinKind;
import query_executor.JoinSpec;
import query_executor.RowBinding;
import query_executor.ValueOrigin;
import org.junit.jupiter.api.Test;

public class QueryExecutorTreeTest {

    @Test
    void rootReadNodeExecutesSelectionTree() {
        DataReadNode root = new DataReadNode();
        root.selectionChild = new SelectionNode();
        root.selectionChild.objectName = "people";
        root.selectionChild.attributeNames = List.of("people.name", "people.age");
        root.selectionChild.filterExpression = new FilterCondition("age", ComparisonOperator.GREATER_THAN, 20);
        root.dataContext = samplePeopleData();

        DataInterface result = root.execute();

        assertEquals(2, result.getRowCount());
        assertEquals(List.of("Bob", "Cara"), result.getColumnValues("people.name"));
        assertEquals(List.of(30, 25), result.getColumnValues("people.age"));
    }

    @Test
    void rootReadNodeExecutesBinaryRowOperationTree() {
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
        root.dataContext = samplePeopleData();

        DataInterface result = root.execute();

        assertEquals(3, result.getRowCount());
        assertEquals(List.of("people.total"), result.getColumnNames());
        assertEquals(List.of(25, 37, 34), result.getColumnValues("people.total"));
    }

    @Test
    void rootReadNodeExecutesColumnAggregationTree() {
        DataReadNode root = new DataReadNode();
        root.columnOperationChild = new ColumnOperationNode();
        root.columnOperationChild.primarySelectionChild = new SelectionNode();
        root.columnOperationChild.primarySelectionChild.objectName = "people";
        root.columnOperationChild.primarySelectionChild.attributeNames = List.of("people.age");
        root.columnOperationChild.operationKind = ColumnOperationKind.SUM;
        root.columnOperationChild.sourceColumnName = "people.age";
        root.columnOperationChild.outputValueName = "people.age_sum";
        root.dataContext = samplePeopleData();

        DataInterface result = root.execute();

        assertEquals(1, result.getRowCount());
        assertEquals(1, result.getColumnCount());
        assertEquals(List.of(75), result.getColumnValues("people.age_sum"));
    }

    @Test
    void rootReadNodeExecutesJoinTree() {
        DataReadNode leftRead = new DataReadNode();
        leftRead.selectionChild = new SelectionNode();
        leftRead.selectionChild.objectName = "people";
        leftRead.selectionChild.attributeNames = List.of("people.id", "people.name");

        DataReadNode rightRead = new DataReadNode();
        rightRead.selectionChild = new SelectionNode();
        rightRead.selectionChild.objectName = "pets";
        rightRead.selectionChild.attributeNames = List.of("pets.owner_id", "pets.pet_name");

        DataReadNode root = new DataReadNode();
        root.joinChild = new JoinNode();
        root.joinChild.leftChild = leftRead;
        root.joinChild.rightChild = rightRead;
        root.joinChild.joinSpec = new JoinSpec(JoinKind.INNER, "people", "pets", "people.id", "pets.owner_id");
        root.dataContext = sampleJoinedSeedData();

        DataInterface result = root.execute();

        assertEquals(2, result.getRowCount());
        assertEquals(List.of("Ada", "Cara"), result.getColumnValues("people.name"));
        assertEquals(List.of("Milo", "Nora"), result.getColumnValues("pets.pet_name"));
    }

    @Test
    void rootDeleteNodeRejectsPartialRowShape() {
        DataDeleteNode root = new DataDeleteNode();
        root.objectName = "people";
        root.targetNames = List.of("people.name", "people.age");
        root.selectionChild = new SelectionNode();
        root.selectionChild.objectName = "people";
        root.selectionChild.attributeNames = List.of("people.name", "people.age");
        root.selectionChild.filterExpression = new FilterCondition("age", ComparisonOperator.GREATER_THAN, 20);
        root.crudEngine = inertCrudEngine();
        root.dataContext = samplePeopleData();

        IllegalStateException exception = assertThrows(IllegalStateException.class, root::execute);
        assertTrue(exception.getMessage().contains("deletable"));
    }

    private static Data samplePeopleData() {
        LinkedHashMap<String, AttributeType> columnTypes = new LinkedHashMap<>();
        columnTypes.put("id", AttributeType.INT);
        columnTypes.put("name", AttributeType.STRING);
        columnTypes.put("age", AttributeType.INT);
        columnTypes.put("bonus", AttributeType.INT);

        LinkedHashMap<String, List<Object>> columnValues = new LinkedHashMap<>();
        columnValues.put("id", List.of(1, 2, 3));
        columnValues.put("name", List.of("Ada", "Bob", "Cara"));
        columnValues.put("age", List.of(20, 30, 25));
        columnValues.put("bonus", List.of(5, 7, 9));

        return Data.fromStoredObject("people", columnTypes, columnValues);
    }

    private static CrudEngineInterface inertCrudEngine() {
        return new CrudEngineInterface() {
            @Override public void createObject(String objectName, String parentObjectNameNullable) {}
            @Override public void deleteObject(String objectName) {}
            @Override public void renameObject(String oldObjectName, String newObjectName) {}
            @Override public void removeParent(String objectName) {}
            @Override public void addParent(String objectName, String parentObjectName) {}
            @Override public void createAttribute(String objectName, String attributeName, AttributeType attributeType) {}
            @Override public void deleteAttribute(String objectName, String attributeName) {}
            @Override public void renameAttribute(String objectName, String oldAttributeName, String newAttributeName) {}
            @Override public DatabaseSchema readSchema() { return new DatabaseSchema(); }
            @Override public int insertRow(String objectName) { return 0; }
            @Override public int getRowCount(String objectName) { return 0; }
            @Override public void writeInt(String objectName, String attributeName, int rowIndex, Integer value) {}
            @Override public Integer readInt(String objectName, String attributeName, int rowIndex) { return null; }
            @Override public void writeString(String objectName, String attributeName, int rowIndex, String value) {}
            @Override public String readString(String objectName, String attributeName, int rowIndex) { return null; }
            @Override public void writeBool(String objectName, String attributeName, int rowIndex, Boolean value) {}
            @Override public Boolean readBool(String objectName, String attributeName, int rowIndex) { return null; }
            @Override public void writeId(String objectName, String attributeName, int rowIndex, UUID value) {}
            @Override public UUID readId(String objectName, String attributeName, int rowIndex) { return null; }
            @Override public void deleteRow(String objectName, int rowIndex) {}
            @Override public void close() {}
        };
    }

    private static Data sampleJoinedSeedData() {
        List<RowBinding> rows = List.of(
            new RowBinding(0, Map.of("people", 0, "pets", 0)),
            new RowBinding(1, Map.of("people", 1, "pets", 1)),
            new RowBinding(2, Map.of("people", 2, "pets", 2))
        );

        LinkedHashMap<String, ColumnData> columns = new LinkedHashMap<>();
        columns.put(
            "people.id",
            new ColumnData(
                "people.id",
                "people",
                "id",
                AttributeType.INT,
                ValueOrigin.STORED,
                List.of(
                    new CellValue(1, new CellLocation("people", "id", 0)),
                    new CellValue(2, new CellLocation("people", "id", 1)),
                    new CellValue(3, new CellLocation("people", "id", 2))
                )
            )
        );
        columns.put(
            "people.name",
            new ColumnData(
                "people.name",
                "people",
                "name",
                AttributeType.STRING,
                ValueOrigin.STORED,
                List.of(
                    new CellValue("Ada", new CellLocation("people", "name", 0)),
                    new CellValue("Bob", new CellLocation("people", "name", 1)),
                    new CellValue("Cara", new CellLocation("people", "name", 2))
                )
            )
        );
        columns.put(
            "pets.owner_id",
            new ColumnData(
                "pets.owner_id",
                "pets",
                "owner_id",
                AttributeType.INT,
                ValueOrigin.STORED,
                List.of(
                    new CellValue(1, new CellLocation("pets", "owner_id", 0)),
                    new CellValue(3, new CellLocation("pets", "owner_id", 1)),
                    new CellValue(4, new CellLocation("pets", "owner_id", 2))
                )
            )
        );
        columns.put(
            "pets.pet_name",
            new ColumnData(
                "pets.pet_name",
                "pets",
                "pet_name",
                AttributeType.STRING,
                ValueOrigin.STORED,
                List.of(
                    new CellValue("Milo", new CellLocation("pets", "pet_name", 0)),
                    new CellValue("Nora", new CellLocation("pets", "pet_name", 1)),
                    new CellValue("Otis", new CellLocation("pets", "pet_name", 2))
                )
            )
        );

        return new Data(
            Set.of("people", "pets"),
            rows,
            columns,
            false,
            Map.of(
                "people", List.of("id", "name"),
                "pets", List.of("owner_id", "pet_name")
            ),
            Map.of(
                "people", List.of(0, 1, 2),
                "pets", List.of(0, 1, 2)
            )
        );
    }
}
