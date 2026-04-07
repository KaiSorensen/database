package query_executor.nodes;

import java.util.List;

public class DataReadNode extends DataStatementNode {
    // One-source read path.
    protected SelectionNode selectionChild;

    // One-source read path.
    protected RowOperationNode rowOperationChild;

    // One-source read path.
    protected ColumnOperationNode columnOperationChild;

    protected List<String> requestedAttributeNames;
}
