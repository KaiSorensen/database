package query_executor;

import java.util.LinkedHashMap;
import java.util.Map;

public record RowCreateRequest(
    String objectName,
    LinkedHashMap<String, Object> values
) {
    public RowCreateRequest {
        values = new LinkedHashMap<>(values);
    }
}
