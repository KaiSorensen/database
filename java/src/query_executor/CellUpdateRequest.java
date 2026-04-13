package query_executor;

public record CellUpdateRequest(
    String objectName,
    String columnName,
    int rowIndex,
    Object value
) {
}
