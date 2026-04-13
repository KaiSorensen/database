package query_executor.nodes;

import crud_engine.CrudEngineInterface;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import query_executor.FilterExpression;
import query_executor.JoinKind;
import query_executor.JoinSpec;
import query_executor.ast.AstDocument;
import query_executor.ast.AstNodeRecord;
import query_executor.ast.AstNodeType;

public final class IndexedAstBinder {
    public Node bind(AstDocument document) {
        LinkedHashMap<Integer, Node> nodesById = new LinkedHashMap<>();
        for (AstNodeRecord record : document.nodes()) {
            nodesById.put(record.nodeId(), instantiate(record.nodeType()));
        }
        for (AstNodeRecord record : document.nodes()) {
            populateParams(nodesById.get(record.nodeId()), record);
        }
        for (AstNodeRecord record : document.nodes()) {
            wireChildren(nodesById, record);
        }
        AstNodeRecord rootRecord = document.root();
        return nodesById.get(rootRecord.nodeId());
    }

    private Node instantiate(AstNodeType nodeType) {
        return switch (nodeType) {
            case ObjectCreate -> new ObjectCreateNode();
            case ObjectRead -> new ObjectReadNode();
            case ObjectUpdate -> new ObjectUpdateNode();
            case ObjectDelete -> new ObjectDeleteNode();
            case AttributeCreate -> new AttributeCreateNode();
            case AttributeRead -> new AttributeReadNode();
            case AttributeUpdate -> new AttributeUpdateNode();
            case AttributeDelete -> new AttributeDeleteNode();
            case DataRead -> new DataReadNode();
            case DataCreate -> new DataCreateNode();
            case DataUpdate -> new DataUpdateNode();
            case DataDelete -> new DataDeleteNode();
            case Selection -> new SelectionNode();
            case RowOperation -> new RowOperationNode();
            case ColumnOperation -> new ColumnOperationNode();
            case Join -> new JoinNode();
        };
    }

    private void populateParams(Node node, AstNodeRecord record) {
        node.nodeId = String.valueOf(record.nodeId());
        node.nodeLabel = record.nodeType().name();
        Map<String, Object> params = record.params();
        switch (record.nodeType()) {
            case ObjectCreate -> populateObjectCreate((ObjectCreateNode) node, params);
            case ObjectRead -> populateObjectRead((ObjectReadNode) node, params);
            case ObjectUpdate -> populateObjectUpdate((ObjectUpdateNode) node, params);
            case ObjectDelete -> populateObjectDelete((ObjectDeleteNode) node, params);
            case AttributeCreate -> populateAttributeCreate((AttributeCreateNode) node, params);
            case AttributeRead -> populateAttributeRead((AttributeReadNode) node, params);
            case AttributeUpdate -> populateAttributeUpdate((AttributeUpdateNode) node, params);
            case AttributeDelete -> populateAttributeDelete((AttributeDeleteNode) node, params);
            case DataRead -> populateDataRead((DataReadNode) node, params);
            case DataCreate -> populateDataCreate((DataCreateNode) node, params);
            case DataUpdate -> populateDataUpdate((DataUpdateNode) node, params);
            case DataDelete -> populateDataDelete((DataDeleteNode) node, params);
            case Selection -> populateSelection((SelectionNode) node, params);
            case RowOperation -> populateRowOperation((RowOperationNode) node, params);
            case ColumnOperation -> populateColumnOperation((ColumnOperationNode) node, params);
            case Join -> populateJoin((JoinNode) node, params);
        }
    }

    private void wireChildren(Map<Integer, Node> nodesById, AstNodeRecord record) {
        Node node = nodesById.get(record.nodeId());
        // Named child roles are used where child meaning matters more than position.
        if (!record.childRoles().isEmpty()) {
            wireChildRoles(node, nodesById, record);
        } else if (!record.childIds().isEmpty()) {
            // Positional child arrays are used where primary/secondary ordering is enough.
            wirePositionalChildren(node, nodesById, record);
        }
    }

    private void wireChildRoles(Node node, Map<Integer, Node> nodesById, AstNodeRecord record) {
        if (node instanceof DataReadNode dataReadNode) {
            Node sourceChild = requireChild(nodesById, record.childRoles(), "source", record.nodeId());
            assignDataReadSource(dataReadNode, sourceChild);
            return;
        }
        if (node instanceof DataCreateNode dataCreateNode) {
            Node valueExpression = requireChild(nodesById, record.childRoles(), "valueExpression", record.nodeId());
            assignDataCreateSource(dataCreateNode, valueExpression);
            return;
        }
        if (node instanceof DataDeleteNode dataDeleteNode) {
            Node targetSelection = requireChild(nodesById, record.childRoles(), "targetSelection", record.nodeId());
            assignDataDeleteSource(dataDeleteNode, targetSelection);
            return;
        }
        if (node instanceof DataUpdateNode dataUpdateNode) {
            Node targetSelection = requireChild(nodesById, record.childRoles(), "targetSelection", record.nodeId());
            Node valueExpression = requireChild(nodesById, record.childRoles(), "valueExpression", record.nodeId());
            if (!(targetSelection instanceof SelectionNode selectionNode)) {
                throw new IllegalArgumentException("DataUpdate targetSelection must be a Selection node.");
            }
            dataUpdateNode.targetSelectionChild = selectionNode;
            assignDataUpdateValue(dataUpdateNode, valueExpression);
            return;
        }
        if (node instanceof JoinNode joinNode) {
            joinNode.leftChild = requireChild(nodesById, record.childRoles(), "left", record.nodeId());
            joinNode.rightChild = requireChild(nodesById, record.childRoles(), "right", record.nodeId());
            return;
        }
        throw new IllegalArgumentException("Node type " + record.nodeType() + " does not support childRoles.");
    }

    private void wirePositionalChildren(Node node, Map<Integer, Node> nodesById, AstNodeRecord record) {
        List<Node> children = new ArrayList<>();
        for (Integer childId : record.childIds()) {
            children.add(nodesById.get(childId));
        }
        if (node instanceof RowOperationNode rowOperationNode) {
            assignRowOperationChildren(rowOperationNode, children);
            return;
        }
        if (node instanceof ColumnOperationNode columnOperationNode) {
            assignColumnOperationChildren(columnOperationNode, children);
            return;
        }
        throw new IllegalArgumentException("Node type " + record.nodeType() + " does not support positional children.");
    }

    private Node requireChild(Map<Integer, Node> nodesById, Map<String, Integer> childRoles, String role, int nodeId) {
        Integer childId = childRoles.get(role);
        if (childId == null) {
            throw new IllegalArgumentException("Node " + nodeId + " is missing required child role: " + role);
        }
        return nodesById.get(childId);
    }

    private void assignDataReadSource(DataReadNode node, Node child) {
        if (child instanceof SelectionNode selectionNode) {
            node.selectionChild = selectionNode;
        } else if (child instanceof RowOperationNode rowOperationNode) {
            node.rowOperationChild = rowOperationNode;
        } else if (child instanceof ColumnOperationNode columnOperationNode) {
            node.columnOperationChild = columnOperationNode;
        } else if (child instanceof JoinNode joinNode) {
            node.joinChild = joinNode;
        } else {
            throw new IllegalArgumentException("DataRead source must be Selection, RowOperation, ColumnOperation, or Join.");
        }
    }

    private void assignDataCreateSource(DataCreateNode node, Node child) {
        if (child instanceof SelectionNode selectionNode) {
            node.selectionChild = selectionNode;
        } else if (child instanceof RowOperationNode rowOperationNode) {
            node.rowOperationChild = rowOperationNode;
        } else {
            throw new IllegalArgumentException("DataCreate valueExpression must be Selection or RowOperation.");
        }
    }

    private void assignDataDeleteSource(DataDeleteNode node, Node child) {
        if (child instanceof SelectionNode selectionNode) {
            node.selectionChild = selectionNode;
        } else if (child instanceof RowOperationNode rowOperationNode) {
            node.rowOperationChild = rowOperationNode;
        } else {
            throw new IllegalArgumentException("DataDelete targetSelection must be Selection or RowOperation.");
        }
    }

    private void assignDataUpdateValue(DataUpdateNode node, Node child) {
        if (child instanceof SelectionNode selectionNode) {
            node.valueSelectionChild = selectionNode;
        } else if (child instanceof RowOperationNode rowOperationNode) {
            node.valueRowOperationChild = rowOperationNode;
        } else if (child instanceof ColumnOperationNode columnOperationNode) {
            node.valueColumnOperationChild = columnOperationNode;
        } else if (child instanceof JoinNode joinNode) {
            node.valueJoinChild = joinNode;
        } else {
            throw new IllegalArgumentException("DataUpdate valueExpression must be Selection, RowOperation, ColumnOperation, or Join.");
        }
    }

    private void assignRowOperationChildren(RowOperationNode node, List<Node> children) {
        if (children.isEmpty() || children.size() > 2) {
            throw new IllegalArgumentException("RowOperation requires one or two children.");
        }
        Node primary = children.getFirst();
        Node secondary = children.size() == 2 ? children.get(1) : null;
        if (primary instanceof DataReadNode primaryRead) {
            node.primaryDataReadChild = primaryRead;
            node.secondaryDataReadChild = castNullable(secondary, DataReadNode.class, "RowOperation secondary child");
        } else if (primary instanceof JoinNode primaryJoin) {
            node.primaryJoinChild = primaryJoin;
            node.secondaryJoinChild = castNullable(secondary, JoinNode.class, "RowOperation secondary child");
        } else if (primary instanceof SelectionNode primarySelection) {
            node.primarySelectionChild = primarySelection;
            node.secondarySelectionChild = castNullable(secondary, SelectionNode.class, "RowOperation secondary child");
        } else if (primary instanceof RowOperationNode primaryRowOperation) {
            node.primaryRowOperationChild = primaryRowOperation;
            node.secondaryRowOperationChild = castNullable(secondary, RowOperationNode.class, "RowOperation secondary child");
        } else if (primary instanceof ColumnOperationNode primaryColumnOperation) {
            node.primaryColumnOperationChild = primaryColumnOperation;
            node.secondaryColumnOperationChild = castNullable(secondary, ColumnOperationNode.class, "RowOperation secondary child");
        } else {
            throw new IllegalArgumentException("RowOperation children must be one supported family.");
        }
    }

    private void assignColumnOperationChildren(ColumnOperationNode node, List<Node> children) {
        if (children.isEmpty() || children.size() > 2) {
            throw new IllegalArgumentException("ColumnOperation requires one or two children.");
        }
        Node primary = children.getFirst();
        Node secondary = children.size() == 2 ? children.get(1) : null;
        if (primary instanceof DataReadNode primaryRead) {
            node.primaryDataReadChild = primaryRead;
            node.secondaryDataReadChild = castNullable(secondary, DataReadNode.class, "ColumnOperation secondary child");
        } else if (primary instanceof JoinNode primaryJoin) {
            node.primaryJoinChild = primaryJoin;
            node.secondaryJoinChild = castNullable(secondary, JoinNode.class, "ColumnOperation secondary child");
        } else if (primary instanceof SelectionNode primarySelection) {
            node.primarySelectionChild = primarySelection;
            node.secondarySelectionChild = castNullable(secondary, SelectionNode.class, "ColumnOperation secondary child");
        } else if (primary instanceof RowOperationNode primaryRowOperation) {
            node.primaryRowOperationChild = primaryRowOperation;
            node.secondaryRowOperationChild = castNullable(secondary, RowOperationNode.class, "ColumnOperation secondary child");
        } else {
            throw new IllegalArgumentException("ColumnOperation children must be one supported family.");
        }
    }

    private <T> T castNullable(Node node, Class<T> expectedType, String label) {
        if (node == null) {
            return null;
        }
        if (!expectedType.isInstance(node)) {
            throw new IllegalArgumentException(label + " must also be a " + expectedType.getSimpleName() + ".");
        }
        return expectedType.cast(node);
    }

    private void populateObjectCreate(ObjectCreateNode node, Map<String, Object> params) {
        node.objectName = requiredString(params, "objectName");
        node.parentObjectName = optionalString(params, "parentObjectName");
        node.extendParentAttributes = optionalBoolean(params, "extendParentAttributes", false);
    }

    private void populateObjectRead(ObjectReadNode node, Map<String, Object> params) {
        node.objectName = requiredString(params, "objectName");
        node.includeAttributeMetadata = optionalBoolean(params, "includeAttributeMetadata", false);
        node.includeRowCount = optionalBoolean(params, "includeRowCount", false);
    }

    private void populateObjectUpdate(ObjectUpdateNode node, Map<String, Object> params) {
        node.objectName = requiredString(params, "objectName");
        node.newObjectName = requiredString(params, "newObjectName");
    }

    private void populateObjectDelete(ObjectDeleteNode node, Map<String, Object> params) {
        node.objectName = requiredString(params, "objectName");
    }

    private void populateAttributeCreate(AttributeCreateNode node, Map<String, Object> params) {
        node.objectName = requiredString(params, "objectName");
        node.attributeName = requiredString(params, "attributeName");
        node.attributeType = CrudEngineInterface.AttributeType.valueOf(requiredString(params, "attributeType"));
    }

    private void populateAttributeRead(AttributeReadNode node, Map<String, Object> params) {
        node.objectName = requiredString(params, "objectName");
        node.attributeName = requiredString(params, "attributeName");
        node.includeAttributeType = optionalBoolean(params, "includeAttributeType", false);
    }

    private void populateAttributeUpdate(AttributeUpdateNode node, Map<String, Object> params) {
        node.objectName = requiredString(params, "objectName");
        node.attributeName = requiredString(params, "attributeName");
        node.newAttributeName = requiredString(params, "newAttributeName");
    }

    private void populateAttributeDelete(AttributeDeleteNode node, Map<String, Object> params) {
        node.objectName = requiredString(params, "objectName");
        node.attributeName = requiredString(params, "attributeName");
    }

    private void populateDataRead(DataReadNode node, Map<String, Object> params) {
        node.objectName = optionalString(params, "objectName");
        node.requestedAttributeNames = optionalStringList(params, "requestedAttributeNames");
    }

    private void populateDataCreate(DataCreateNode node, Map<String, Object> params) {
        node.objectName = requiredString(params, "objectName");
        node.targetNames = requiredStringList(params, "targetNames");
        node.createdColumnName = optionalString(params, "createdColumnName");
    }

    private void populateDataUpdate(DataUpdateNode node, Map<String, Object> params) {
        node.objectName = requiredString(params, "objectName");
        node.targetNames = requiredStringList(params, "targetNames");
    }

    private void populateDataDelete(DataDeleteNode node, Map<String, Object> params) {
        node.objectName = requiredString(params, "objectName");
        node.targetNames = requiredStringList(params, "targetNames");
    }

    private void populateSelection(SelectionNode node, Map<String, Object> params) {
        node.objectName = requiredString(params, "objectName");
        node.attributeNames = requiredStringList(params, "attributes");
        Object filter = params.get("filter");
        if (filter != null && !(filter instanceof FilterExpression filterExpression)) {
            throw new IllegalArgumentException("Selection filter must be a parsed FilterExpression.");
        } else if (filter instanceof FilterExpression filterExpression) {
            node.filterExpression = filterExpression;
        }
        Object role = params.get("role");
        if (role instanceof String roleLabel) {
            node.nodeLabel = roleLabel;
        }
    }

    private void populateRowOperation(RowOperationNode node, Map<String, Object> params) {
        node.operationKind = RowOperationKind.valueOf(requiredString(params, "operationKind"));
        node.sourceNames = requiredStringList(params, "sourceNames");
        node.outputColumnName = requiredString(params, "outputColumnName");
    }

    private void populateColumnOperation(ColumnOperationNode node, Map<String, Object> params) {
        node.operationKind = ColumnOperationKind.valueOf(requiredString(params, "operationKind"));
        node.sourceColumnName = requiredString(params, "sourceColumnName");
        node.outputValueName = requiredString(params, "outputValueName");
    }

    private void populateJoin(JoinNode node, Map<String, Object> params) {
        node.joinSpec = new JoinSpec(
            JoinKind.valueOf(requiredString(params, "joinKind")),
            requiredString(params, "leftObjectName"),
            requiredString(params, "rightObjectName"),
            requiredString(params, "leftColumnName"),
            requiredString(params, "rightColumnName")
        );
    }

    private String requiredString(Map<String, Object> params, String key) {
        Object value = params.get(key);
        if (!(value instanceof String stringValue) || stringValue.isBlank()) {
            throw new IllegalArgumentException("Missing required string param: " + key);
        }
        return stringValue;
    }

    private String optionalString(Map<String, Object> params, String key) {
        Object value = params.get(key);
        return value instanceof String stringValue ? stringValue : null;
    }

    private boolean optionalBoolean(Map<String, Object> params, String key, boolean defaultValue) {
        Object value = params.get(key);
        return value instanceof Boolean booleanValue ? booleanValue : defaultValue;
    }

    @SuppressWarnings("unchecked")
    private List<String> requiredStringList(Map<String, Object> params, String key) {
        Object value = params.get(key);
        if (!(value instanceof List<?> rawList) || rawList.isEmpty()) {
            throw new IllegalArgumentException("Missing required list param: " + key);
        }
        return (List<String>) ensureStringList(rawList, key);
    }

    @SuppressWarnings("unchecked")
    private List<String> optionalStringList(Map<String, Object> params, String key) {
        Object value = params.get(key);
        if (value == null) {
            return null;
        }
        if (!(value instanceof List<?> rawList)) {
            throw new IllegalArgumentException("Param " + key + " must be a list.");
        }
        return (List<String>) ensureStringList(rawList, key);
    }

    private List<String> ensureStringList(List<?> rawList, String key) {
        List<String> values = new ArrayList<>();
        for (Object entry : rawList) {
            if (!(entry instanceof String stringEntry)) {
                throw new IllegalArgumentException("Param " + key + " must contain only string values.");
            }
            values.add(stringEntry);
        }
        return List.copyOf(values);
    }
}
