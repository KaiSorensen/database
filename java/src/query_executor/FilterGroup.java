package query_executor;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public record FilterGroup(
    BooleanOperator operator,
    List<FilterExpression> children
) implements FilterExpression {

    @Override
    public Set<String> referencedColumns() {
        LinkedHashSet<String> columns = new LinkedHashSet<>();
        if (children == null) {
            return columns;
        }
        for (FilterExpression child : children) {
            if (child != null) {
                columns.addAll(child.referencedColumns());
            }
        }
        return columns;
    }
}
