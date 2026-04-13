package query_executor;

public record RowDeleteRequest(
    String objectName,
    int rowIndex
) {
}
