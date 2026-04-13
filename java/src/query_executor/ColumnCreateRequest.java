package query_executor;

import crud_engine.CrudEngineInterface;
import java.util.List;

public record ColumnCreateRequest(
    String objectName,
    String columnName,
    CrudEngineInterface.AttributeType attributeType,
    List<Object> values
) {
    public ColumnCreateRequest {
        values = List.copyOf(values);
    }
}
