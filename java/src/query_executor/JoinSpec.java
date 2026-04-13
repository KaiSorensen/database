package query_executor;

public record JoinSpec(
    JoinKind joinKind,
    String leftObjectName,
    String rightObjectName,
    String leftColumnName,
    String rightColumnName
) {
}
