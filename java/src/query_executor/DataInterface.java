package query_executor;

import java.util.List;
import query_executor.nodes.ColumnOperationKind;
import query_executor.nodes.RowOperationKind;

public interface DataInterface {

    String getObjectName();

    int getRowCount();

    int getColumnCount();

    List<String> getColumnNames();

    List<Object> getColumnValues(String columnName);

    boolean hasColumn(String columnName);

    boolean isEmpty();

    DataInterface selectColumns(List<String> columnNames);

    DataInterface filterRows(FilterExpression filter);

    default DataInterface applySelection(List<String> columnNames, FilterExpression filter) {
        DataInterface filtered = filter == null ? this : filterRows(filter);
        return filtered.selectColumns(columnNames);
    }

    DataInterface applyRowOperation(
        RowOperationKind operationKind,
        List<String> sourceColumns,
        String outputColumnName
    );

    DataInterface applyColumnOperation(
        ColumnOperationKind operationKind,
        String sourceColumn,
        String outputValueName
    );

    boolean hasExactRowSetMatch(DataInterface other);

    DataInterface mergeColumnsFromExactMatch(DataInterface other);

    DataInterface join(DataInterface other, JoinSpec joinSpec);

    boolean isReadable();

    boolean isSingleColumn();

    boolean isSingleValue();

    boolean isFullRowSetForObject(String objectName);

    boolean isFullColumnSetForObject(String objectName);

    boolean hasExistingLocationsForObject(String objectName);

    boolean isCreatableAsRows(String objectName);

    boolean isCreatableAsColumns(String objectName);

    boolean isUpdatableForObject(String objectName);

    boolean fitsUpdateTarget(DataInterface targetData, String objectName, List<String> targetNames);

    boolean isDeletableAsRows(String objectName);

    boolean isDeletableAsColumns(String objectName);

    List<RowCreateRequest> toRowCreateRequests(String objectName);

    List<CellUpdateRequest> toCellUpdateRequests(String objectName);

    List<CellUpdateRequest> toCellUpdateRequests(String objectName, List<String> targetNames);

    List<CellUpdateRequest> toCellUpdateRequests(String objectName, List<String> targetNames, DataInterface targetData);

    List<RowDeleteRequest> toRowDeleteRequests(String objectName);

    List<ColumnCreateRequest> toColumnCreateRequests(String objectName);
}
