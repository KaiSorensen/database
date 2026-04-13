package query_executor;

import java.util.Set;

public record FilterCondition(
    String columnName,
    ComparisonOperator operator,
    Object comparisonValue
) implements FilterExpression {

    @Override
    public Set<String> referencedColumns() {
        return Set.of(columnName);
    }
}
