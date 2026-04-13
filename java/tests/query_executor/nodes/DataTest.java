package query_executor.nodes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import crud_engine.CrudEngineInterface.AttributeType;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import query_executor.BooleanOperator;
import query_executor.CellLocation;
import query_executor.CellValue;
import query_executor.ColumnData;
import query_executor.ComparisonOperator;
import query_executor.Data;
import query_executor.DataInterface;
import query_executor.FilterCondition;
import query_executor.FilterExpression;
import query_executor.FilterGroup;
import query_executor.JoinKind;
import query_executor.JoinSpec;
import query_executor.RowBinding;
import query_executor.ValueOrigin;
import org.junit.jupiter.api.Test;

public class DataTest {

    @Test
    void applySelectionSupportsNestedBooleanFilters() {
        Data people = samplePeopleData();
        FilterExpression filter = new FilterGroup(
            BooleanOperator.AND,
            List.of(
                new FilterCondition("age", ComparisonOperator.GREATER_THAN, 20),
                new FilterGroup(
                    BooleanOperator.NOT,
                    List.of(new FilterCondition("name", ComparisonOperator.CONTAINS, "o"))
                )
            )
        );

        DataInterface selected = people.applySelection(List.of("people.name", "people.age"), filter);

        assertEquals(1, selected.getRowCount());
        assertEquals(List.of("people.name", "people.age"), selected.getColumnNames());
    }

    @Test
    void rowOperationProducesSingleDerivedColumn() {
        Data people = samplePeopleData();

        DataInterface result = people.applyRowOperation(
            RowOperationKind.ADD,
            List.of("age", "bonus"),
            "people.total"
        );

        assertEquals(1, result.getColumnCount());
        assertTrue(result.hasColumn("people.total"));
        assertTrue(result.isSingleColumn());
    }

    @Test
    void columnOperationProducesScalarAsOneRowOneColumnTable() {
        Data people = samplePeopleData();

        DataInterface result = people.applyColumnOperation(
            ColumnOperationKind.SUM,
            "age",
            "people.age_sum"
        );

        assertEquals(1, result.getRowCount());
        assertEquals(1, result.getColumnCount());
        assertTrue(result.isSingleValue());
    }

    @Test
    void exactRowSetMatchDetectsDifferentFilteredResults() {
        Data people = samplePeopleData();
        DataInterface older = people.filterRows(new FilterCondition("age", ComparisonOperator.GREATER_THAN, 20));
        DataInterface named = people.filterRows(new FilterCondition("name", ComparisonOperator.CONTAINS, "a"));

        assertFalse(older.hasExactRowSetMatch(named));
        assertThrows(IllegalArgumentException.class, () -> older.mergeColumnsFromExactMatch(named));
    }

    @Test
    void innerJoinProducesMergedRowsAndColumns() {
        Data people = samplePeopleData();
        Data pets = samplePetsData();

        DataInterface joined = people.join(
            pets,
            new JoinSpec(JoinKind.INNER, "people", "pets", "people.id", "pets.owner_id")
        );

        assertEquals(2, joined.getRowCount());
        assertTrue(joined.hasColumn("people.name"));
        assertTrue(joined.hasColumn("pets.pet_name"));
    }

    @Test
    void rowOperationNodeRejectsMismatchedDualInputsWithoutJoin() {
        SelectionNode firstSelection = new SelectionNode();
        firstSelection.objectName = "people";
        firstSelection.attributeNames = List.of("people.age");
        firstSelection.filterExpression = new FilterCondition("age", ComparisonOperator.GREATER_THAN, 20);

        SelectionNode secondSelection = new SelectionNode();
        secondSelection.objectName = "people";
        secondSelection.attributeNames = List.of("people.bonus", "people.name");
        secondSelection.filterExpression = new FilterCondition("name", ComparisonOperator.CONTAINS, "a");

        RowOperationNode operationNode = new RowOperationNode();
        operationNode.primarySelectionChild = firstSelection;
        operationNode.secondarySelectionChild = secondSelection;
        operationNode.operationKind = RowOperationKind.ADD;
        operationNode.sourceNames = List.of("people.age", "people.bonus");
        operationNode.outputColumnName = "people.total";
        operationNode.dataContext = samplePeopleData();

        IllegalStateException exception = assertThrows(IllegalStateException.class, operationNode::execute);
        assertTrue(exception.getMessage().contains("exact row-set match"));
    }

    @Test
    void joinNodeMakesAmbiguousDualInputBehaviorExplicit() {
        DataReadNode leftRead = new DataReadNode();
        leftRead.selectionChild = new SelectionNode();
        leftRead.selectionChild.objectName = "people";
        leftRead.selectionChild.attributeNames = List.of("people.id", "people.name");

        DataReadNode rightRead = new DataReadNode();
        rightRead.selectionChild = new SelectionNode();
        rightRead.selectionChild.objectName = "pets";
        rightRead.selectionChild.attributeNames = List.of("pets.owner_id", "pets.pet_name");

        JoinNode joinNode = new JoinNode();
        joinNode.leftChild = leftRead;
        joinNode.rightChild = rightRead;
        joinNode.joinSpec = new JoinSpec(JoinKind.INNER, "people", "pets", "people.id", "pets.owner_id");
        joinNode.dataContext = sampleJoinedSeedData();

        DataInterface joined = joinNode.execute();

        assertEquals(2, joined.getRowCount());
        assertTrue(joined.hasColumn("pets.pet_name"));
    }

    @Test
    void fullRowAndColumnValidationsRespectObjectUniverse() {
        Data people = samplePeopleData();
        DataInterface selectedColumns = people.selectColumns(List.of("people.id", "people.name"));

        assertTrue(people.isFullRowSetForObject("people"));
        assertTrue(people.isFullColumnSetForObject("people"));
        assertFalse(selectedColumns.isFullRowSetForObject("people"));
        assertTrue(selectedColumns.isFullColumnSetForObject("people"));
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

    private static Data samplePetsData() {
        LinkedHashMap<String, AttributeType> columnTypes = new LinkedHashMap<>();
        columnTypes.put("owner_id", AttributeType.INT);
        columnTypes.put("pet_name", AttributeType.STRING);

        LinkedHashMap<String, List<Object>> columnValues = new LinkedHashMap<>();
        columnValues.put("owner_id", List.of(1, 3, 4));
        columnValues.put("pet_name", List.of("Milo", "Nora", "Otis"));

        return Data.fromStoredObject("pets", columnTypes, columnValues);
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
