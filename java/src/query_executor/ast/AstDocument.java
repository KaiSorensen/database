package query_executor.ast;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class AstDocument {
    private final LinkedHashMap<Integer, AstNodeRecord> nodesById;

    public AstDocument(List<AstNodeRecord> nodes) {
        this.nodesById = new LinkedHashMap<>();
        for (AstNodeRecord node : nodes) {
            if (nodesById.putIfAbsent(node.nodeId(), node) != null) {
                throw new IllegalArgumentException("Duplicate AST node id: " + node.nodeId());
            }
        }
        if (nodesById.isEmpty()) {
            throw new IllegalArgumentException("AST document must contain at least one node.");
        }
    }

    public List<AstNodeRecord> nodes() {
        return new ArrayList<>(nodesById.values());
    }

    public AstNodeRecord node(int nodeId) {
        AstNodeRecord node = nodesById.get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("Unknown AST node id: " + nodeId);
        }
        return node;
    }

    public AstNodeRecord root() {
        Set<Integer> referencedIds = new LinkedHashSet<>();
        // Child lists contribute one set of inbound references for root detection.
        for (AstNodeRecord node : nodesById.values()) {
            referencedIds.addAll(node.childIds());
            // Named child roles contribute the same kind of inbound references.
            referencedIds.addAll(node.childRoles().values());
        }
        List<AstNodeRecord> roots = new ArrayList<>();
        // Any node never referenced as a child is a root candidate.
        for (AstNodeRecord node : nodesById.values()) {
            if (!referencedIds.contains(node.nodeId())) {
                roots.add(node);
            }
        }
        if (roots.size() != 1) {
            throw new IllegalArgumentException("AST document must have exactly one root node.");
        }
        return roots.getFirst();
    }

    public void validateReferences() {
        for (AstNodeRecord node : nodesById.values()) {
            // Every child id in the document must resolve to a known node record.
            for (Integer childId : node.childIds()) {
                if (!nodesById.containsKey(childId)) {
                    throw new IllegalArgumentException("Unknown child id " + childId + " referenced by node " + node.nodeId());
                }
            }
            // Named child-role references follow the same rule as positional child ids.
            for (Map.Entry<String, Integer> childRole : node.childRoles().entrySet()) {
                if (!nodesById.containsKey(childRole.getValue())) {
                    throw new IllegalArgumentException(
                        "Unknown child id " + childRole.getValue() + " referenced by role " + childRole.getKey()
                            + " on node " + node.nodeId()
                    );
                }
            }
        }
    }
}
