package query_executor.nodes;

import crud_engine.AttributeSchema;
import crud_engine.DatabaseSchema;
import crud_engine.ObjectSchema;
import crud_engine.CrudEngineInterface.AttributeType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import query_executor.Data;
import query_executor.DataInterface;
import query_executor.ValidationResult;

public class LiteralRowsNode extends Node {
    protected String objectName;
    protected List<String> columnNames;
    protected List<List<Object>> rows;

    @Override
    public ValidationResult validate() {
        ValidationResult crudValidation = requireCrudEngine("LiteralRowsNode");
        if (!crudValidation.isValid()) {
            return crudValidation;
        }
        if (objectName == null || objectName.isBlank()) {
            return ValidationResult.invalid("LiteralRowsNode requires an object name.");
        }
        if (columnNames == null || columnNames.isEmpty()) {
            return ValidationResult.invalid("LiteralRowsNode requires at least one column name.");
        }
        if (rows == null || rows.isEmpty()) {
            return ValidationResult.invalid("LiteralRowsNode requires at least one literal row.");
        }
        if (new LinkedHashSet<>(normalizedColumnNames()).size() != columnNames.size()) {
            return ValidationResult.invalid("LiteralRowsNode column names must be unique.");
        }
        for (List<Object> row : rows) {
            if (row == null || row.size() != columnNames.size()) {
                return ValidationResult.invalid("LiteralRowsNode requires each row to match the declared column count.");
            }
        }
        try {
            ObjectSchema objectSchema = requireObjectSchema();
            for (String columnName : normalizedColumnNames()) {
                if (!objectSchema.getAttributes().containsKey(columnName)) {
                    return ValidationResult.invalid("LiteralRowsNode references unknown column: " + columnName);
                }
            }
        } catch (IOException exception) {
            return ValidationResult.invalid(exception.getMessage());
        }
        return ValidationResult.valid();
    }

    @Override
    public DataInterface execute() {
        ValidationResult validation = validate();
        if (!validation.isValid()) {
            throw new IllegalStateException(validation.message());
        }
        try {
            ObjectSchema objectSchema = requireObjectSchema();
            LinkedHashMap<String, AttributeType> schemaColumnTypes = new LinkedHashMap<>();
            for (AttributeSchema attributeSchema : objectSchema.getAttributes().values()) {
                schemaColumnTypes.put(normalizeName(attributeSchema.getAttributeName()), attributeSchema.getAttributeType());
            }

            List<String> normalizedColumnNames = normalizedColumnNames();
            List<LinkedHashMap<String, Object>> rowValues = new ArrayList<>();
            for (List<Object> row : rows) {
                LinkedHashMap<String, Object> rowValueMap = new LinkedHashMap<>();
                for (int index = 0; index < normalizedColumnNames.size(); index++) {
                    rowValueMap.put(normalizedColumnNames.get(index), row.get(index));
                }
                rowValues.add(rowValueMap);
            }

            dataContext = Data.fromLiteralRows(normalizeName(objectName), schemaColumnTypes, rowValues);
            return dataContext;
        } catch (IOException exception) {
            throw new IllegalStateException("LiteralRowsNode failed to load schema for object: " + objectName, exception);
        }
    }

    private ObjectSchema requireObjectSchema() throws IOException {
        DatabaseSchema schema = crudEngine.readSchema();
        ObjectSchema objectSchema = schema.getObjects().get(normalizeName(objectName));
        if (objectSchema == null) {
            throw new IOException("Unknown object: " + objectName);
        }
        return objectSchema;
    }

    private List<String> normalizedColumnNames() {
        return columnNames.stream().map(this::extractAttributeName).map(this::normalizeName).toList();
    }

    private String extractAttributeName(String rawName) {
        int separatorIndex = rawName.lastIndexOf('.');
        return separatorIndex >= 0 ? rawName.substring(separatorIndex + 1) : rawName;
    }

    private String normalizeName(String rawName) {
        return rawName.trim().toLowerCase().replaceAll("[^a-z0-9]+", "_").replaceAll("^_+|_+$", "");
    }
}
