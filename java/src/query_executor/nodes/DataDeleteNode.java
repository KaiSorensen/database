package query_executor.nodes;

import java.util.List;

public class DataDeleteNode extends DataStatementNode {
    // One-source delete path.
    protected SelectionNode selectionChild;

    // One-source delete path.
    protected RowOperationNode rowOperationChild;

    protected List<String> targetNames;
}
