package query_executor;

import crud_engine.AttributeSchema;
import crud_engine.CrudEngineInterface;
import crud_engine.DatabaseSchema;
import crud_engine.ObjectSchema;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import query_executor.nodes.ColumnOperationKind;
import query_executor.nodes.RowOperationKind;

/**
 * This class is the universal runtime data structure for the query executor layer.
 * It stores query results in a tabular format, even when a query is concerned with a
 * single value. It owns filtering, row-wise and column-wise calculations, join behavior,
 * CRUD-shape validation, and export into CRUD-ready mutation requests.
 *
 * The nodes own query structure and intent, while this class owns the truth about
 * rows, columns, values, and storage locations.
 *
 * @author Written by Codex, designed and documented by Kai
 */
public final class Data implements DataInterface {
    private final Set<String> objectNames;
    private final List<RowBinding> rows;
    private final LinkedHashMap<String, ColumnData> columns;
    private final boolean scalarLike;
    private final LinkedHashMap<String, List<String>> storedColumnUniverseByObject;
    private final LinkedHashMap<String, List<Integer>> rowUniverseByObject;

    public Data(
        Set<String> objectNames,
        List<RowBinding> rows,
        LinkedHashMap<String, ColumnData> columns,
        boolean scalarLike,
        Map<String, List<String>> storedColumnUniverseByObject,
        Map<String, List<Integer>> rowUniverseByObject
    ) {
        this.objectNames = Collections.unmodifiableSet(new LinkedHashSet<>(objectNames));
        this.rows = List.copyOf(rows);
        this.columns = new LinkedHashMap<>(columns);
        this.scalarLike = scalarLike;
        this.storedColumnUniverseByObject = deepCopyListMap(storedColumnUniverseByObject);
        this.rowUniverseByObject = deepCopyIntListMap(rowUniverseByObject);
        validateShape();
    }

    /**
     * Builds a tabular runtime view directly from stored object data.
     * Each stored attribute becomes a qualified column, and each row keeps
     * a binding back to its original row index under that object.
     */
    public static Data fromStoredObject(
        String objectName,
        LinkedHashMap<String, CrudEngineInterface.AttributeType> columnTypes,
        LinkedHashMap<String, List<Object>> columnValues
    ) {
        // The widest column determines how many stored rows the runtime view must expose.
        List<Integer> rowUniverse = inferRowUniverse(columnValues);
        LinkedHashMap<String, ColumnData> columns = new LinkedHashMap<>();
        // Build one qualified runtime column for each stored attribute.
        for (Map.Entry<String, CrudEngineInterface.AttributeType> entry : columnTypes.entrySet()) {
            String attributeName = entry.getKey();
            String qualifiedColumnName = qualifyColumnName(objectName, attributeName);
            List<Object> values = columnValues.getOrDefault(attributeName, List.of());
            List<CellValue> cellValues = new ArrayList<>();
            // Missing tail values are padded with null so every column shares one row count.
            for (int i = 0; i < rowUniverse.size(); i++) {
                Object value = i < values.size() ? values.get(i) : null;
                cellValues.add(new CellValue(value, new CellLocation(objectName, attributeName, rowUniverse.get(i))));
            }
            columns.put(
                qualifiedColumnName,
                new ColumnData(
                    qualifiedColumnName,
                    objectName,
                    attributeName,
                    entry.getValue(),
                    ValueOrigin.STORED,
                    cellValues
                )
            );
        }

        List<RowBinding> rows = new ArrayList<>();
        // Each result row initially maps back to one stored row under the source object.
        for (int i = 0; i < rowUniverse.size(); i++) {
            rows.add(new RowBinding(i, Map.of(objectName, rowUniverse.get(i))));
        }

        return new Data(
            Set.of(objectName),
            rows,
            columns,
            false,
            Map.of(objectName, new ArrayList<>(columnTypes.keySet())),
            Map.of(objectName, rowUniverse)
        );
    }

    /**
     * Wraps a scalar result into the same tabular structure used everywhere else.
     * This keeps node input/output shapes uniform across the executor.
     */
    public static Data fromScalar(
        String outputValueName,
        CrudEngineInterface.AttributeType attributeType,
        Object value
    ) {
        LinkedHashMap<String, ColumnData> columns = new LinkedHashMap<>();
        columns.put(
            outputValueName,
            new ColumnData(
                outputValueName,
                null,
                null,
                attributeType,
                ValueOrigin.DERIVED,
                List.of(new CellValue(value, null))
            )
        );
        return new Data(
            Set.of(),
            List.of(new RowBinding(0, Map.of())),
            columns,
            true,
            Map.of(),
            Map.of()
        );
    }

    /**
     * Builds a row-creatable runtime view from inline literal values.
     * The provided row maps may specify a subset of the object's stored attributes;
     * any omitted attributes are filled with null in schema order so row creation can
     * still emit complete CRUD-ready rows.
     */
    public static Data fromLiteralRows(
        String objectName,
        LinkedHashMap<String, CrudEngineInterface.AttributeType> schemaColumnTypes,
        List<LinkedHashMap<String, Object>> rowValues
    ) {
        LinkedHashMap<String, ColumnData> columns = new LinkedHashMap<>();
        List<RowBinding> rows = new ArrayList<>();
        for (int rowIndex = 0; rowIndex < rowValues.size(); rowIndex++) {
            // Literal rows have result-row identity only; they do not point at stored rows yet.
            rows.add(new RowBinding(rowIndex, Map.of()));
        }

        for (Map.Entry<String, CrudEngineInterface.AttributeType> entry : schemaColumnTypes.entrySet()) {
            String attributeName = entry.getKey();
            String qualifiedColumnName = qualifyColumnName(objectName, attributeName);
            List<CellValue> values = new ArrayList<>();
            for (LinkedHashMap<String, Object> rowValue : rowValues) {
                values.add(new CellValue(rowValue.get(attributeName), null));
            }
            columns.put(
                qualifiedColumnName,
                new ColumnData(
                    qualifiedColumnName,
                    objectName,
                    attributeName,
                    entry.getValue(),
                    ValueOrigin.DERIVED,
                    values
                )
            );
        }

        return new Data(
            Set.of(objectName),
            rows,
            columns,
            false,
            Map.of(objectName, new ArrayList<>(schemaColumnTypes.keySet())),
            Map.of(objectName, List.of())
        );
    }

    /**
     * Loads one stored object through the CRUD layer and converts it into the executor's
     * tabular runtime representation. This gives query-executor tests a direct path from
     * persisted database state into Data without bypassing the CRUD engine.
     */
    public static Data fromCrudEngine(CrudEngineInterface crudEngine, String objectName) throws IOException {
        DatabaseSchema schema = crudEngine.readSchema();
        String objectSlug = normalizeName(objectName);
        ObjectSchema objectSchema = schema.getObjects().get(objectSlug);
        if (objectSchema == null) {
            throw new IOException("Unknown object: " + objectName);
        }

        int rowCount = crudEngine.getRowCount(objectName);
        LinkedHashMap<String, CrudEngineInterface.AttributeType> columnTypes = new LinkedHashMap<>();
        LinkedHashMap<String, List<Object>> columnValues = new LinkedHashMap<>();

        // Preserve schema order so the runtime view matches the stored attribute layout.
        for (var entry : objectSchema.getAttributes().entrySet()) {
            String attributeSlug = entry.getKey();
            AttributeSchema attributeSchema = entry.getValue();
            columnTypes.put(attributeSlug, attributeSchema.getAttributeType());

            List<Object> values = new ArrayList<>();
            // Read every stored row for the current attribute through the typed CRUD API.
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                values.add(readValue(crudEngine, objectName, attributeSlug, attributeSchema.getAttributeType(), rowIndex));
            }
            columnValues.put(attributeSlug, values);
        }

        return fromStoredObject(objectSlug, columnTypes, columnValues);
    }

    @Override
    public String getObjectName() {
        // Empty object sets happen for scalar derived results.
        if (objectNames.isEmpty()) {
            return "";
        }
        // Single-object results can still report a plain object name.
        if (objectNames.size() == 1) {
            return objectNames.iterator().next();
        }
        // Multi-object results only show a summary name because joins have more than one owner.
        return String.join(",", objectNames);
    }

    @Override
    public int getRowCount() {
        return rows.size();
    }

    @Override
    public int getColumnCount() {
        return columns.size();
    }

    @Override
    public List<String> getColumnNames() {
        return new ArrayList<>(columns.keySet());
    }

    @Override
    public List<Object> getColumnValues(String columnName) {
        String resolvedColumn = requireResolvedColumnName(columnName);
        return columns.get(resolvedColumn).values().stream()
            .map(CellValue::value)
            .toList();
    }

    @Override
    public boolean hasColumn(String columnName) {
        return resolveColumnName(columnName) != null;
    }

    @Override
    public boolean isEmpty() {
        return rows.isEmpty() || columns.isEmpty();
    }

    /*
     * Selection is performed in two steps:
     * 1. keep only the requested columns
     * 2. filter the surviving rows with the parsed filter tree
     *
     * This mirrors the current SelectionNode behavior.
     */
    @Override
    public DataInterface selectColumns(List<String> columnNames) {
        if (columnNames == null || columnNames.isEmpty()) {
            throw new IllegalArgumentException("selectColumns requires at least one column name.");
        }
        LinkedHashMap<String, ColumnData> selectedColumns = new LinkedHashMap<>();
        // Preserve the caller's requested order in the projected result.
        for (String requestedName : columnNames) {
            String resolvedName = requireResolvedColumnName(requestedName);
            selectedColumns.put(resolvedName, columns.get(resolvedName));
        }
        return copyWith(rows, selectedColumns, scalarLike);
    }

    @Override
    public DataInterface filterRows(FilterExpression filter) {
        if (filter == null) {
            return this;
        }
        // Fail fast if the filter tree mentions columns that are not present here.
        validateFilterReferences(filter);
        List<RowBinding> filteredRows = new ArrayList<>();
        LinkedHashMap<String, List<CellValue>> filteredValuesByColumn = initializeFilteredValues();

        // Evaluate the filter row by row so the output stays aligned across all columns.
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            if (evaluateFilter(filter, rowIndex)) {
                // Filtering keeps the original row binding for each surviving result row.
                // That is what later allows exact-rowset matching across sibling branches.
                filteredRows.add(rows.get(rowIndex));
                // Copy the row's cell from every active column into the filtered result.
                for (Map.Entry<String, ColumnData> entry : columns.entrySet()) {
                    filteredValuesByColumn.get(entry.getKey()).add(entry.getValue().values().get(rowIndex));
                }
            }
        }

        LinkedHashMap<String, ColumnData> filteredColumns = rebuildColumns(filteredValuesByColumn);
        return copyWith(filteredRows, filteredColumns, scalarLike && filteredRows.size() == 1 && filteredColumns.size() == 1);
    }

    /*
     * Row operations consume one or more columns and produce a single derived column.
     * The row bindings are preserved because each output value still corresponds to a
     * specific result row.
     */
    @Override
    public DataInterface applyRowOperation(
        RowOperationKind operationKind,
        List<String> sourceColumns,
        String outputColumnName
    ) {
        if (sourceColumns == null || sourceColumns.isEmpty()) {
            throw new IllegalArgumentException("Row operation requires source columns.");
        }
        List<String> resolvedColumns = resolveColumnNames(sourceColumns);
        List<CellValue> outputValues = new ArrayList<>();
        CrudEngineInterface.AttributeType outputType = inferRowOperationType(operationKind);

        // Produce one derived value for each existing result row.
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            List<Object> inputValues = new ArrayList<>();
            // Gather the requested source-column values from the current row.
            for (String columnName : resolvedColumns) {
                inputValues.add(columns.get(columnName).values().get(rowIndex).value());
            }
            // Derived values do not point at a stored cell location.
            outputValues.add(new CellValue(applyRowOperationToValues(operationKind, inputValues), null));
        }

        LinkedHashMap<String, ColumnData> outputColumns = new LinkedHashMap<>();
        // Row operations intentionally collapse the working set to the new derived column.
        outputColumns.put(
            outputColumnName,
            new ColumnData(
                outputColumnName,
                objectNames.size() == 1 ? getObjectName() : null,
                null,
                outputType,
                ValueOrigin.DERIVED,
                outputValues
            )
        );

        return new Data(
            objectNames,
            rows,
            outputColumns,
            false,
            storedColumnUniverseByObject,
            rowUniverseByObject
        );
    }

    /*
     * Column operations reduce a single column to a scalar result.
     * The scalar is represented as a one-row, one-column Data instance.
     */
    @Override
    public DataInterface applyColumnOperation(
        ColumnOperationKind operationKind,
        String sourceColumn,
        String outputValueName
    ) {
        String resolvedColumn = requireResolvedColumnName(sourceColumn);
        List<Object> values = columns.get(resolvedColumn).values().stream()
            .map(CellValue::value)
            .toList();
        Object result = applyColumnOperationToValues(operationKind, values);
        CrudEngineInterface.AttributeType outputType = inferColumnOperationType(operationKind, columns.get(resolvedColumn).attributeType());
        return fromScalar(outputValueName, outputType, result);
    }

    @Override
    public boolean hasExactRowSetMatch(DataInterface other) {
        Data otherData = requireData(other);
        // Different row counts cannot describe the same row set.
        if (rows.size() != otherData.rows.size()) {
            return false;
        }
        // Exact match means each result row points at the same source-row bindings in order.
        for (int i = 0; i < rows.size(); i++) {
            if (!Objects.equals(rows.get(i).sourceRowIndexByObject(), otherData.rows.get(i).sourceRowIndexByObject())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public DataInterface mergeColumnsFromExactMatch(DataInterface other) {
        Data otherData = requireData(other);
        if (!hasExactRowSetMatch(otherData)) {
            throw new IllegalArgumentException("Cannot merge data with non-matching row sets.");
        }
        // Exact-match merge keeps the left-side row set intact and only appends columns.
        // If two branches produced the same column name, the tree is ambiguous.
        LinkedHashMap<String, ColumnData> mergedColumns = new LinkedHashMap<>(columns);
        for (Map.Entry<String, ColumnData> entry : otherData.columns.entrySet()) {
            if (mergedColumns.containsKey(entry.getKey())) {
                throw new IllegalArgumentException("Duplicate column during exact-match merge: " + entry.getKey());
            }
            mergedColumns.put(entry.getKey(), entry.getValue());
        }
        LinkedHashSet<String> mergedObjects = new LinkedHashSet<>(objectNames);
        mergedObjects.addAll(otherData.objectNames);
        LinkedHashMap<String, List<String>> mergedUniverse = mergeListMaps(storedColumnUniverseByObject, otherData.storedColumnUniverseByObject);
        LinkedHashMap<String, List<Integer>> mergedRows = mergeIntListMaps(rowUniverseByObject, otherData.rowUniverseByObject);
        return new Data(mergedObjects, rows, mergedColumns, scalarLike && otherData.scalarLike, mergedUniverse, mergedRows);
    }

    /*
     * Joins create a new result-row set that combines row bindings from both sides.
     * v1 supports INNER JOIN only, with a single equality key per side.
     */
    @Override
    public DataInterface join(DataInterface other, JoinSpec joinSpec) {
        Data rightData = requireData(other);
        // v1 only supports INNER joins, so reject everything else explicitly.
        if (joinSpec.joinKind() != JoinKind.INNER) {
            throw new IllegalArgumentException("Only INNER joins are supported.");
        }

        // Resolve the key columns inside each side's own data context.
        String leftJoinColumn = requireResolvedColumnName(joinSpec.leftColumnName());
        String rightJoinColumn = rightData.requireResolvedColumnName(joinSpec.rightColumnName());

        // Join key types must match before any row comparison happens.
        CrudEngineInterface.AttributeType leftType = columns.get(leftJoinColumn).attributeType();
        CrudEngineInterface.AttributeType rightType = rightData.columns.get(rightJoinColumn).attributeType();
        if (leftType != rightType) {
            throw new IllegalArgumentException("Join column types must match.");
        }

        List<RowBinding> joinedRows = new ArrayList<>();
        LinkedHashMap<String, List<CellValue>> joinedValuesByColumn = initializeFilteredValues();
        // Seed the result with right-side columns after checking for naming collisions.
        for (String columnName : rightData.columns.keySet()) {
            if (joinedValuesByColumn.containsKey(columnName)) {
                throw new IllegalArgumentException("Duplicate column during join: " + columnName);
            }
            joinedValuesByColumn.put(columnName, new ArrayList<>());
        }

        int resultIndex = 0;
        // Compare every left row against every right row for the join predicate.
        for (int leftIndex = 0; leftIndex < rows.size(); leftIndex++) {
            Object leftValue = columns.get(leftJoinColumn).values().get(leftIndex).value();
            // The inner loop looks for right-side matches for the current left row.
            for (int rightIndex = 0; rightIndex < rightData.rows.size(); rightIndex++) {
                Object rightValue = rightData.columns.get(rightJoinColumn).values().get(rightIndex).value();
                if (Objects.equals(leftValue, rightValue)) {
                    // A joined result row carries source-row bindings from both sides so
                    // downstream validation can still reason about existing locations.
                    LinkedHashMap<String, Integer> bindingMap = rows.get(leftIndex).sourceRowIndexByObject();
                    bindingMap.putAll(rightData.rows.get(rightIndex).sourceRowIndexByObject());
                    joinedRows.add(new RowBinding(resultIndex++, bindingMap));

                    // Copy the full left row into the joined result.
                    for (Map.Entry<String, ColumnData> entry : columns.entrySet()) {
                        joinedValuesByColumn.get(entry.getKey()).add(entry.getValue().values().get(leftIndex));
                    }
                    // Copy the full matching right row into the joined result.
                    for (Map.Entry<String, ColumnData> entry : rightData.columns.entrySet()) {
                        joinedValuesByColumn.get(entry.getKey()).add(entry.getValue().values().get(rightIndex));
                    }
                }
            }
        }

        LinkedHashMap<String, ColumnData> joinedColumns = rebuildColumnsFrom(joinedValuesByColumn, columns, rightData.columns);
        LinkedHashSet<String> mergedObjects = new LinkedHashSet<>(objectNames);
        mergedObjects.addAll(rightData.objectNames);
        LinkedHashMap<String, List<String>> mergedUniverse = mergeListMaps(storedColumnUniverseByObject, rightData.storedColumnUniverseByObject);
        LinkedHashMap<String, List<Integer>> mergedRows = mergeIntListMaps(rowUniverseByObject, rightData.rowUniverseByObject);
        return new Data(mergedObjects, joinedRows, joinedColumns, false, mergedUniverse, mergedRows);
    }

    /*
     * These validation helpers tell the CRUD root nodes whether the current shape is
     * legal for create, read, update, or delete. They do not perform the mutation.
     */
    @Override
    public boolean isReadable() {
        return !columns.isEmpty();
    }

    @Override
    public boolean isSingleColumn() {
        return columns.size() == 1;
    }

    @Override
    public boolean isSingleValue() {
        return scalarLike || (rows.size() == 1 && columns.size() == 1);
    }

    @Override
    public boolean isFullRowSetForObject(String objectName) {
        List<String> universe = storedColumnUniverseByObject.get(objectName);
        // Full-row validation needs both a known schema universe and at least one row.
        if (universe == null || universe.isEmpty() || rows.isEmpty()) {
            return false;
        }
        // Every stored attribute for the object must be present in the working set.
        for (String attributeName : universe) {
            if (!columns.containsKey(qualifyColumnName(objectName, attributeName))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isFullColumnSetForObject(String objectName) {
        List<Integer> universeRows = rowUniverseByObject.get(objectName);
        // Full-column validation needs a known row universe for the target object.
        if (universeRows == null || universeRows.isEmpty()) {
            return false;
        }
        // Collect the active stored row indexes for just the target object.
        List<Integer> activeRows = rows.stream()
            .map(row -> row.sourceRowIndexForObject(objectName))
            .filter(Objects::nonNull)
            .toList();
        // A full column means the working set still spans every stored row in order.
        return activeRows.equals(universeRows);
    }

    @Override
    public boolean hasExistingLocationsForObject(String objectName) {
        boolean foundColumn = false;
        // Check every active column that belongs to the target object.
        for (ColumnData column : columns.values()) {
            if (!objectName.equals(column.objectNameNullable())) {
                continue;
            }
            foundColumn = true;
            // Every cell must still point at a stored location for update/delete scenarios.
            for (CellValue value : column.values()) {
                if (value.locationNullable() == null) {
                    return false;
                }
            }
        }
        return foundColumn;
    }

    @Override
    public boolean isCreatableAsRows(String objectName) {
        return isFullRowSetForObject(objectName);
    }

    @Override
    public boolean isCreatableAsColumns(String objectName) {
        return isFullColumnSetForObject(objectName);
    }

    @Override
    public boolean isUpdatableForObject(String objectName) {
        if (rows.isEmpty() || columns.isEmpty()) {
            return false;
        }
        // Updates only require that each result row still points at a stored row under the target object.
        for (RowBinding row : rows) {
            if (row.sourceRowIndexForObject(objectName) == null) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean fitsUpdateTarget(DataInterface targetData, String objectName, List<String> targetNames) {
        Data target = requireData(targetData);
        if (targetNames == null || targetNames.isEmpty()) {
            return false;
        }
        // The target side must identify concrete stored rows under the object being updated.
        if (!target.isUpdatableForObject(objectName)) {
            return false;
        }
        // Scalar updates are allowed only for one target attribute and broadcast to every target row.
        if (isSingleValue()) {
            return targetNames.size() == 1 && !target.rows.isEmpty();
        }
        // Non-scalar updates must provide one active column per target attribute.
        if (columns.size() != targetNames.size()) {
            return false;
        }
        // Per-row updates must line up exactly with the selected target rows.
        return hasExactRowSetMatch(target);
    }

    @Override
    public boolean isDeletableAsRows(String objectName) {
        return isFullRowSetForObject(objectName) && hasExistingLocationsForObject(objectName);
    }

    @Override
    public boolean isDeletableAsColumns(String objectName) {
        return isFullColumnSetForObject(objectName) && hasExistingLocationsForObject(objectName);
    }

    @Override
    public List<RowCreateRequest> toRowCreateRequests(String objectName) {
        if (!isCreatableAsRows(objectName)) {
            throw new IllegalStateException("Data is not creatable as rows for object: " + objectName);
        }
        List<RowCreateRequest> requests = new ArrayList<>();
        List<String> objectColumns = storedColumnUniverseByObject.getOrDefault(objectName, List.of());
        // Build one create request per result row.
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            LinkedHashMap<String, Object> values = new LinkedHashMap<>();
            // Fill the row request in schema order so downstream CRUD stays predictable.
            for (String attributeName : objectColumns) {
                String qualifiedColumn = qualifyColumnName(objectName, attributeName);
                values.put(attributeName, columns.get(qualifiedColumn).values().get(rowIndex).value());
            }
            requests.add(new RowCreateRequest(objectName, values));
        }
        return requests;
    }

    @Override
    public List<CellUpdateRequest> toCellUpdateRequests(String objectName) {
        // Default update export uses the active stored columns' own attribute names as targets.
        List<String> targetNames = new ArrayList<>();
        for (ColumnData column : columns.values()) {
            if (!objectName.equals(column.objectNameNullable()) || column.attributeNameNullable() == null) {
                throw new IllegalStateException("Data does not expose direct stored-column targets for object: " + objectName);
            }
            targetNames.add(column.attributeNameNullable());
        }
        return toCellUpdateRequests(objectName, targetNames, this);
    }

    @Override
    public List<CellUpdateRequest> toCellUpdateRequests(String objectName, List<String> targetNames) {
        return toCellUpdateRequests(objectName, targetNames, this);
    }

    @Override
    public List<CellUpdateRequest> toCellUpdateRequests(String objectName, List<String> targetNames, DataInterface targetData) {
        Data target = requireData(targetData);
        if (!fitsUpdateTarget(target, objectName, targetNames)) {
            throw new IllegalStateException("Data does not fit the requested update target for object: " + objectName);
        }
        // Scalar updates broadcast one computed value across every selected target row.
        if (isSingleValue()) {
            Object scalarValue = columns.values().iterator().next().values().get(0).value();
            List<CellUpdateRequest> requests = new ArrayList<>();
            for (RowBinding targetRow : target.rows) {
                Integer storedRowIndex = targetRow.sourceRowIndexForObject(objectName);
                requests.add(new CellUpdateRequest(objectName, extractAttributeName(targetNames.get(0)), storedRowIndex, scalarValue));
            }
            return requests;
        }

        if (!isUpdatableForObject(objectName)) {
            throw new IllegalStateException("Data is not updatable for object: " + objectName);
        }
        List<CellUpdateRequest> requests = new ArrayList<>();
        List<ColumnData> activeColumns = new ArrayList<>(columns.values());
        // Pair each active column with the caller-provided target attribute it should update.
        for (int columnIndex = 0; columnIndex < activeColumns.size(); columnIndex++) {
            ColumnData column = activeColumns.get(columnIndex);
            String targetName = extractAttributeName(targetNames.get(columnIndex));
            // Emit one update request per result row using the row binding as the stored location.
            for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
                Integer storedRowIndex = target.rows.get(rowIndex).sourceRowIndexForObject(objectName);
                requests.add(new CellUpdateRequest(objectName, targetName, storedRowIndex, column.values().get(rowIndex).value()));
            }
        }
        return requests;
    }

    @Override
    public List<RowDeleteRequest> toRowDeleteRequests(String objectName) {
        if (!isDeletableAsRows(objectName)) {
            throw new IllegalStateException("Data is not deletable as rows for object: " + objectName);
        }
        return rows.stream()
            .map(row -> new RowDeleteRequest(objectName, row.sourceRowIndexForObject(objectName)))
            .toList();
    }

    @Override
    public List<ColumnCreateRequest> toColumnCreateRequests(String objectName) {
        if (!isCreatableAsColumns(objectName)) {
            throw new IllegalStateException("Data is not creatable as columns for object: " + objectName);
        }
        List<ColumnCreateRequest> requests = new ArrayList<>();
        // Only derived columns are candidates for column creation.
        for (ColumnData column : columns.values()) {
            if (column.origin() != ValueOrigin.DERIVED) {
                continue;
            }
            // Preserve the full derived column payload so CRUD can append it in one step.
            List<Object> values = column.values().stream().map(CellValue::value).toList();
            requests.add(new ColumnCreateRequest(objectName, column.columnName(), column.attributeType(), values));
        }
        return requests;
    }

    private LinkedHashMap<String, List<CellValue>> initializeFilteredValues() {
        LinkedHashMap<String, List<CellValue>> values = new LinkedHashMap<>();
        // Pre-create an output list for every active column so row filtering can append in place.
        for (String columnName : columns.keySet()) {
            values.put(columnName, new ArrayList<>());
        }
        return values;
    }

    private LinkedHashMap<String, ColumnData> rebuildColumns(LinkedHashMap<String, List<CellValue>> valuesByColumn) {
        return rebuildColumnsFrom(valuesByColumn, columns, Map.of());
    }

    private LinkedHashMap<String, ColumnData> rebuildColumnsFrom(
        LinkedHashMap<String, List<CellValue>> valuesByColumn,
        Map<String, ColumnData> primaryColumns,
        Map<String, ColumnData> secondaryColumns
    ) {
        LinkedHashMap<String, ColumnData> rebuilt = new LinkedHashMap<>();
        // Rebuild each column with the filtered or joined cell sequence that was accumulated.
        for (Map.Entry<String, List<CellValue>> entry : valuesByColumn.entrySet()) {
            ColumnData source = primaryColumns.containsKey(entry.getKey())
                ? primaryColumns.get(entry.getKey())
                : secondaryColumns.get(entry.getKey());
            rebuilt.put(
                entry.getKey(),
                new ColumnData(
                    source.columnName(),
                    source.objectNameNullable(),
                    source.attributeNameNullable(),
                    source.attributeType(),
                    source.origin(),
                    entry.getValue()
                )
            );
        }
        return rebuilt;
    }

    private Data copyWith(
        List<RowBinding> newRows,
        LinkedHashMap<String, ColumnData> newColumns,
        boolean newScalarLike
    ) {
        return new Data(
            objectNames,
            newRows,
            newColumns,
            newScalarLike,
            storedColumnUniverseByObject,
            rowUniverseByObject
        );
    }

    private void validateShape() {
        // Every column must stay aligned to the shared result-row count.
        for (ColumnData column : columns.values()) {
            if (column.values().size() != rows.size()) {
                throw new IllegalArgumentException("Column " + column.columnName() + " has mismatched row count.");
            }
        }
    }

    private void validateFilterReferences(FilterExpression filter) {
        // Force every referenced column through the normal resolution rules up front.
        for (String columnName : filter.referencedColumns()) {
            requireResolvedColumnName(columnName);
        }
    }

    private boolean evaluateFilter(FilterExpression expression, int rowIndex) {
        // Leaf conditions compare one row's value against one comparison value.
        if (expression instanceof FilterCondition condition) {
            return evaluateCondition(condition, rowIndex);
        }
        if (!(expression instanceof FilterGroup group)) {
            throw new IllegalArgumentException("Unsupported filter expression type: " + expression.getClass().getName());
        }
        // NOT is unary, so it is handled as a dedicated case before generic group folding.
        if (group.operator() == BooleanOperator.NOT) {
            if (group.children() == null || group.children().size() != 1) {
                throw new IllegalArgumentException("NOT filter groups must have exactly one child.");
            }
            return !evaluateFilter(group.children().get(0), rowIndex);
        }
        // AND and OR require at least one child to seed the fold.
        if (group.children() == null || group.children().isEmpty()) {
            throw new IllegalArgumentException("Boolean filter groups require at least one child.");
        }
        boolean result = evaluateFilter(group.children().get(0), rowIndex);
        // Fold the remaining child expressions left-to-right.
        for (int i = 1; i < group.children().size(); i++) {
            boolean childResult = evaluateFilter(group.children().get(i), rowIndex);
            result = switch (group.operator()) {
                case AND -> result && childResult;
                case OR -> result || childResult;
                case NOT -> throw new IllegalStateException("NOT handled above.");
            };
        }
        return result;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private boolean evaluateCondition(FilterCondition condition, int rowIndex) {
        String resolvedColumn = requireResolvedColumnName(condition.columnName());
        Object leftValue = columns.get(resolvedColumn).values().get(rowIndex).value();
        Object rightValue = condition.comparisonValue();
        // Compare the current row's cell value against the filter's comparison value.
        return switch (condition.operator()) {
            case EQUALS -> Objects.equals(leftValue, rightValue);
            case NOT_EQUALS -> !Objects.equals(leftValue, rightValue);
            case CONTAINS -> leftValue instanceof String && rightValue instanceof String &&
                ((String) leftValue).contains((String) rightValue);
            case GREATER_THAN -> compareComparable((Comparable) leftValue, (Comparable) rightValue) > 0;
            case LESS_THAN -> compareComparable((Comparable) leftValue, (Comparable) rightValue) < 0;
            case GREATER_THAN_OR_EQUALS -> compareComparable((Comparable) leftValue, (Comparable) rightValue) >= 0;
            case LESS_THAN_OR_EQUALS -> compareComparable((Comparable) leftValue, (Comparable) rightValue) <= 0;
        };
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private int compareComparable(Comparable left, Comparable right) {
        if (left == null || right == null) {
            throw new IllegalArgumentException("Comparison filters do not support null operands.");
        }
        return left.compareTo(right);
    }

    private CrudEngineInterface.AttributeType inferRowOperationType(RowOperationKind operationKind) {
        return switch (operationKind) {
            case ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULO -> CrudEngineInterface.AttributeType.INT;
            case EQUALS, NOT_EQUALS, GREATER_THAN, LESS_THAN, CONTAINS -> CrudEngineInterface.AttributeType.BOOL;
        };
    }

    private Object applyRowOperationToValues(RowOperationKind operationKind, List<Object> inputValues) {
        return switch (operationKind) {
            case ADD -> inputValues.stream().mapToInt(this::asInteger).sum();
            case SUBTRACT -> subtractIntegers(inputValues);
            case MULTIPLY -> multiplyIntegers(inputValues);
            case DIVIDE -> divideIntegers(inputValues);
            case MODULO -> moduloIntegers(inputValues);
            case EQUALS -> compareAllEqual(inputValues);
            case NOT_EQUALS -> !compareAllEqual(inputValues);
            case GREATER_THAN -> asInteger(inputValues.get(0)) > asInteger(inputValues.get(1));
            case LESS_THAN -> asInteger(inputValues.get(0)) < asInteger(inputValues.get(1));
            case CONTAINS -> String.valueOf(inputValues.get(0)).contains(String.valueOf(inputValues.get(1)));
        };
    }

    private CrudEngineInterface.AttributeType inferColumnOperationType(
        ColumnOperationKind operationKind,
        CrudEngineInterface.AttributeType sourceType
    ) {
        return switch (operationKind) {
            case COUNT, SUM, AVERAGE, MEDIAN -> CrudEngineInterface.AttributeType.INT;
            case MIN, MAX, MODE -> sourceType;
        };
    }

    private Object applyColumnOperationToValues(ColumnOperationKind operationKind, List<Object> values) {
        List<Object> nonNullValues = values.stream().filter(Objects::nonNull).toList();
        return switch (operationKind) {
            case COUNT -> nonNullValues.size();
            case SUM -> nonNullValues.stream().mapToInt(this::asInteger).sum();
            case AVERAGE -> nonNullValues.isEmpty() ? 0 : nonNullValues.stream().mapToInt(this::asInteger).sum() / nonNullValues.size();
            case MIN -> nonNullValues.stream().min(this::compareObjects).orElse(null);
            case MAX -> nonNullValues.stream().max(this::compareObjects).orElse(null);
            case MEDIAN -> median(nonNullValues);
            case MODE -> mode(nonNullValues);
        };
    }

    private int compareObjects(Object left, Object right) {
        if (left instanceof Comparable<?> comparableLeft && right instanceof Comparable<?>) {
            @SuppressWarnings("unchecked")
            Comparable<Object> typedLeft = (Comparable<Object>) comparableLeft;
            return typedLeft.compareTo(right);
        }
        throw new IllegalArgumentException("Values are not comparable: " + left + ", " + right);
    }

    private int asInteger(Object value) {
        if (!(value instanceof Integer integerValue)) {
            throw new IllegalArgumentException("Expected integer value but got: " + value);
        }
        return integerValue;
    }

    private Integer subtractIntegers(List<Object> values) {
        int result = asInteger(values.get(0));
        // Subtraction is left-associative across the remaining operands.
        for (int i = 1; i < values.size(); i++) {
            result -= asInteger(values.get(i));
        }
        return result;
    }

    private Integer multiplyIntegers(List<Object> values) {
        int result = 1;
        // Multiplication folds across all operands in order.
        for (Object value : values) {
            result *= asInteger(value);
        }
        return result;
    }

    private Integer divideIntegers(List<Object> values) {
        int result = asInteger(values.get(0));
        // Division is left-associative across the remaining operands.
        for (int i = 1; i < values.size(); i++) {
            result /= asInteger(values.get(i));
        }
        return result;
    }

    private Integer moduloIntegers(List<Object> values) {
        if (values.size() != 2) {
            throw new IllegalArgumentException("Modulo requires exactly two integer operands.");
        }
        return asInteger(values.get(0)) % asInteger(values.get(1));
    }

    private boolean compareAllEqual(List<Object> values) {
        Object first = values.get(0);
        // All later operands must match the first operand exactly.
        for (int i = 1; i < values.size(); i++) {
            if (!Objects.equals(first, values.get(i))) {
                return false;
            }
        }
        return true;
    }

    private Object median(List<Object> values) {
        if (values.isEmpty()) {
            return null;
        }
        List<Integer> sorted = values.stream().map(this::asInteger).sorted().toList();
        return sorted.get(sorted.size() / 2);
    }

    private Object mode(List<Object> values) {
        if (values.isEmpty()) {
            return null;
        }
        return values.stream()
            .collect(Collectors.groupingBy(value -> value, LinkedHashMap::new, Collectors.counting()))
            .entrySet()
            .stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse(null);
    }

    private String resolveColumnName(String columnName) {
        if (columns.containsKey(columnName)) {
            return columnName;
        }
        // Unqualified names are allowed only when they identify one column uniquely.
        // Once multiple objects are present, callers need to qualify the reference.
        List<String> matches = columns.keySet().stream()
            .filter(existing -> existing.equals(columnName) || existing.endsWith("." + columnName))
            .toList();
        if (matches.size() == 1) {
            return matches.get(0);
        }
        if (matches.size() > 1) {
            throw new IllegalArgumentException("Ambiguous column reference: " + columnName);
        }
        return null;
    }

    private String requireResolvedColumnName(String columnName) {
        String resolved = resolveColumnName(columnName);
        if (resolved == null) {
            throw new IllegalArgumentException("Unknown column: " + columnName);
        }
        return resolved;
    }

    private List<String> resolveColumnNames(Collection<String> columnNames) {
        return columnNames.stream().map(this::requireResolvedColumnName).toList();
    }

    private static List<Integer> inferRowUniverse(LinkedHashMap<String, List<Object>> columnValues) {
        int rowCount = columnValues.values().stream().mapToInt(List::size).max().orElse(0);
        List<Integer> rowUniverse = new ArrayList<>();
        // Stored rows are indexed contiguously from zero in the current engine model.
        for (int i = 0; i < rowCount; i++) {
            rowUniverse.add(i);
        }
        return rowUniverse;
    }

    private static String qualifyColumnName(String objectName, String attributeName) {
        return objectName + "." + attributeName;
    }

    private static String extractAttributeName(String rawName) {
        int separatorIndex = rawName.lastIndexOf('.');
        return separatorIndex >= 0 ? rawName.substring(separatorIndex + 1) : rawName;
    }

    private static String normalizeName(String rawName) {
        return rawName.trim().toLowerCase().replaceAll("[^a-z0-9]+", "_").replaceAll("^_+|_+$", "");
    }

    private static Object readValue(
        CrudEngineInterface crudEngine,
        String objectName,
        String attributeName,
        CrudEngineInterface.AttributeType attributeType,
        int rowIndex
    ) throws IOException {
        // Dispatch to the typed CRUD read that matches the schema-declared attribute type.
        return switch (attributeType) {
            case INT -> crudEngine.readInt(objectName, attributeName, rowIndex);
            case STRING -> crudEngine.readString(objectName, attributeName, rowIndex);
            case BOOL -> crudEngine.readBool(objectName, attributeName, rowIndex);
            case ID -> crudEngine.readId(objectName, attributeName, rowIndex);
        };
    }

    private static LinkedHashMap<String, List<String>> deepCopyListMap(Map<String, List<String>> source) {
        LinkedHashMap<String, List<String>> copy = new LinkedHashMap<>();
        // Copy each list so this Data instance stays immutable from the caller's perspective.
        for (Map.Entry<String, List<String>> entry : source.entrySet()) {
            copy.put(entry.getKey(), List.copyOf(entry.getValue()));
        }
        return copy;
    }

    private static LinkedHashMap<String, List<Integer>> deepCopyIntListMap(Map<String, List<Integer>> source) {
        LinkedHashMap<String, List<Integer>> copy = new LinkedHashMap<>();
        // Copy each row-universe list so later mutations cannot leak into this instance.
        for (Map.Entry<String, List<Integer>> entry : source.entrySet()) {
            copy.put(entry.getKey(), List.copyOf(entry.getValue()));
        }
        return copy;
    }

    private static LinkedHashMap<String, List<String>> mergeListMaps(
        Map<String, List<String>> left,
        Map<String, List<String>> right
    ) {
        LinkedHashMap<String, List<String>> merged = deepCopyListMap(left);
        // Preserve the left map's order and only add object universes that are missing.
        for (Map.Entry<String, List<String>> entry : right.entrySet()) {
            merged.putIfAbsent(entry.getKey(), List.copyOf(entry.getValue()));
        }
        return merged;
    }

    private static LinkedHashMap<String, List<Integer>> mergeIntListMaps(
        Map<String, List<Integer>> left,
        Map<String, List<Integer>> right
    ) {
        LinkedHashMap<String, List<Integer>> merged = deepCopyIntListMap(left);
        // Preserve the left map's order and only add row universes that are missing.
        for (Map.Entry<String, List<Integer>> entry : right.entrySet()) {
            merged.putIfAbsent(entry.getKey(), List.copyOf(entry.getValue()));
        }
        return merged;
    }

    private Data requireData(DataInterface other) {
        if (!(other instanceof Data otherData)) {
            throw new IllegalArgumentException("Expected Data implementation but received: " + other.getClass().getName());
        }
        return otherData;
    }
}
