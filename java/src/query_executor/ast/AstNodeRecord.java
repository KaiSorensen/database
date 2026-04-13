package query_executor.ast;

import java.util.List;
import java.util.Map;

public record AstNodeRecord(
    int nodeId,
    AstNodeType nodeType,
    List<Integer> childIds,
    Map<String, Integer> childRoles,
    Map<String, Object> params
) {
}
