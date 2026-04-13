package query_executor;

public record QueryExecutionResult(
    boolean success,
    String message,
    DataInterface returnedData
) {
    public static QueryExecutionResult success(String message, DataInterface returnedData) {
        return new QueryExecutionResult(true, message, returnedData);
    }

    public static QueryExecutionResult failure(String message) {
        return new QueryExecutionResult(false, message, null);
    }
}
