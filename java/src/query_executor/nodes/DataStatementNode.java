package query_executor.nodes;

import crud_engine.AttributeSchema;
import crud_engine.CrudEngineInterface;
import crud_engine.DatabaseSchema;
import crud_engine.ObjectSchema;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import query_executor.CellUpdateRequest;
import query_executor.ColumnCreateRequest;
import query_executor.Data;
import query_executor.RowCreateRequest;
import query_executor.RowDeleteRequest;
import query_executor.ValidationResult;

public abstract class DataStatementNode extends Node {
    protected String objectName;

    protected void commitRowCreates() throws IOException {
        LinkedHashMap<String, CrudEngineInterface.AttributeType> attributeTypes = attributeTypesByName();
        // Insert each requested row and then write one typed value per attribute.
        for (RowCreateRequest request : ((Data) dataContext).toRowCreateRequests(objectName)) {
            int rowIndex = crudEngine.insertRow(objectName);
            for (Map.Entry<String, Object> valueEntry : request.values().entrySet()) {
                String attributeName = valueEntry.getKey();
                writeTypedValue(attributeName, rowIndex, valueEntry.getValue(), attributeTypes.get(attributeName));
            }
        }
    }

    protected void commitColumnCreates() throws IOException {
        // Each derived column becomes one new stored attribute.
        for (ColumnCreateRequest request : ((Data) dataContext).toColumnCreateRequests(objectName)) {
            String attributeName = createdAttributeName(request);
            crudEngine.createAttribute(objectName, attributeName, request.attributeType());
            // Fill the newly created attribute row-by-row through the typed CRUD API.
            for (int rowIndex = 0; rowIndex < request.values().size(); rowIndex++) {
                writeTypedValue(attributeName, rowIndex, request.values().get(rowIndex), request.attributeType());
            }
        }
    }

    protected void commitUpdates(Iterable<CellUpdateRequest> requests) throws IOException {
        LinkedHashMap<String, CrudEngineInterface.AttributeType> attributeTypes = attributeTypesByName();
        List<CellUpdateRequest> stagedRequests = new ArrayList<>();
        // Stage and validate every update request before the first persistent write happens.
        for (CellUpdateRequest request : requests) {
            CrudEngineInterface.AttributeType attributeType = attributeTypes.get(normalizeName(request.columnName()));
            validateValueCompatibility(objectName, request.columnName(), request.value(), attributeType);
            stagedRequests.add(request);
        }
        // Each update request targets one stored cell address identified by object/attribute/row.
        for (CellUpdateRequest request : stagedRequests) {
            writeTypedValue(
                request.columnName(),
                request.rowIndex(),
                request.value(),
                attributeTypes.get(normalizeName(request.columnName()))
            );
        }
    }

    protected void commitRowDeletes() throws IOException {
        // Delete in descending row order so later row indexes do not shift underneath earlier deletes.
        ((Data) dataContext).toRowDeleteRequests(objectName).stream()
            .map(RowDeleteRequest::rowIndex)
            .sorted((left, right) -> Integer.compare(right, left))
            .forEach(rowIndex -> {
                try {
                    crudEngine.deleteRow(objectName, rowIndex);
                } catch (IOException exception) {
                    throw new IllegalStateException("Failed to delete row " + rowIndex + " from object " + objectName, exception);
                }
            });
    }

    protected void commitColumnDeletes() throws IOException {
        // Delete the selected stored attributes by name.
        for (String targetName : targetNames()) {
            crudEngine.deleteAttribute(objectName, extractAttributeName(targetName));
        }
    }

    protected Data refreshDataContext() throws IOException {
        return Data.fromCrudEngine(crudEngine, objectName);
    }

    protected Iterable<String> targetNames() {
        return java.util.List.of();
    }

    private LinkedHashMap<String, CrudEngineInterface.AttributeType> attributeTypesByName() throws IOException {
        DatabaseSchema schema = crudEngine.readSchema();
        ObjectSchema objectSchema = schema.getObjects().get(normalizeName(objectName));
        if (objectSchema == null) {
            throw new IOException("Unknown object: " + objectName);
        }
        LinkedHashMap<String, CrudEngineInterface.AttributeType> attributeTypes = new LinkedHashMap<>();
        // Preserve the object's schema order while building the typed write map.
        for (AttributeSchema attributeSchema : objectSchema.getAttributes().values()) {
            attributeTypes.put(attributeSchema.getAttributeName(), attributeSchema.getAttributeType());
        }
        return attributeTypes;
    }

    private void writeTypedValue(
        String attributeName,
        int rowIndex,
        Object value,
        CrudEngineInterface.AttributeType attributeType
    ) throws IOException {
        if (attributeType == null) {
            throw new IOException("Unknown attribute type for " + objectName + "." + attributeName);
        }
        // Dispatch each write through the CRUD engine's typed API so persistence rules stay centralized there.
        switch (attributeType) {
            case INT -> crudEngine.writeInt(objectName, attributeName, rowIndex, (Integer) value);
            case STRING -> crudEngine.writeString(objectName, attributeName, rowIndex, (String) value);
            case BOOL -> crudEngine.writeBool(objectName, attributeName, rowIndex, (Boolean) value);
            case ID -> crudEngine.writeId(objectName, attributeName, rowIndex, (java.util.UUID) value);
        }
    }

    private void validateValueCompatibility(
        String objectName,
        String attributeName,
        Object value,
        CrudEngineInterface.AttributeType attributeType
    ) throws IOException {
        if (attributeType == null) {
            throw new IOException("Unknown attribute type for " + objectName + "." + attributeName);
        }
        if (value == null) {
            return;
        }
        // Check Java-side type compatibility before persistent writes begin.
        boolean compatible = switch (attributeType) {
            case INT -> value instanceof Integer;
            case STRING -> value instanceof String;
            case BOOL -> value instanceof Boolean;
            case ID -> value instanceof java.util.UUID;
        };
        if (!compatible) {
            throw new IOException("Value type does not fit target attribute " + objectName + "." + attributeName);
        }
    }

    private String createdAttributeName(ColumnCreateRequest request) {
        if (request.columnName() == null || request.columnName().isBlank()) {
            throw new IllegalStateException("Column create request requires a non-blank column name.");
        }
        return extractAttributeName(request.columnName());
    }

    protected String extractAttributeName(String rawName) {
        int separatorIndex = rawName.lastIndexOf('.');
        return separatorIndex >= 0 ? rawName.substring(separatorIndex + 1) : rawName;
    }

    private String normalizeName(String rawName) {
        return rawName.trim().toLowerCase().replaceAll("[^a-z0-9]+", "_").replaceAll("^_+|_+$", "");
    }
}
