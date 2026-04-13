package query_executor;

import java.util.Set;

public interface FilterExpression {
    Set<String> referencedColumns();
}
