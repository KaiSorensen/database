package query_executor;

public record CellValue(
    Object value,
    CellLocation locationNullable
) {
}
