package query_executor;

public record CellLocation(
    String objectName,
    String attributeName,
    int rowIndex
) {
}
