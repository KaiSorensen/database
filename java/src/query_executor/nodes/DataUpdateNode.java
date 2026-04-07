package query_executor.nodes;

import java.util.List;

public class DataUpdateNode extends DataStatementNode {
    // One-source update path.
    protected SelectionNode selectionChild;

    // One-source update path.
    protected RowOperationNode rowOperationChild;

    // One-source update path.
    protected ColumnOperationNode columnOperationChild;

    protected List<String> targetNames;
}
