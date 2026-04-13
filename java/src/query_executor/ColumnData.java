package query_executor;

import crud_engine.CrudEngineInterface;
import java.util.ArrayList;
import java.util.List;

public final class ColumnData {
    private final String columnName;
    private final String objectNameNullable;
    private final String attributeNameNullable;
    private final CrudEngineInterface.AttributeType attributeType;
    private final ValueOrigin origin;
    private final List<CellValue> values;

    public ColumnData(
        String columnName,
        String objectNameNullable,
        String attributeNameNullable,
        CrudEngineInterface.AttributeType attributeType,
        ValueOrigin origin,
        List<CellValue> values
    ) {
        this.columnName = columnName;
        this.objectNameNullable = objectNameNullable;
        this.attributeNameNullable = attributeNameNullable;
        this.attributeType = attributeType;
        this.origin = origin;
        this.values = List.copyOf(values);
    }

    public String columnName() {
        return columnName;
    }

    public String objectNameNullable() {
        return objectNameNullable;
    }

    public String attributeNameNullable() {
        return attributeNameNullable;
    }

    public CrudEngineInterface.AttributeType attributeType() {
        return attributeType;
    }

    public ValueOrigin origin() {
        return origin;
    }

    public List<CellValue> values() {
        return new ArrayList<>(values);
    }
}
