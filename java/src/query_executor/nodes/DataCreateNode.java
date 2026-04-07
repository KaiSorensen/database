package query_executor.nodes;

import java.util.List;

public class DataCreateNode extends DataStatementNode {
    // One-source create path.
    protected SelectionNode selectionChild;

    // One-source create path.
    protected RowOperationNode rowOperationChild;

    protected List<String> targetNames;
    protected String createdColumnName;
}
