package query_executor.nodes;

import java.util.List;

public class ColumnOperationNode extends Node {
    // Combination: DataRead (single or pair).
    protected DataReadNode primaryDataReadChild;
    protected DataReadNode secondaryDataReadChild;

    // Combination: Selection (single or pair).
    protected SelectionNode primarySelectionChild;
    protected SelectionNode secondarySelectionChild;

    // Combination: RowOperation (single or pair).
    protected RowOperationNode primaryRowOperationChild;
    protected RowOperationNode secondaryRowOperationChild;

    protected ColumnOperationKind operationKind;
    protected String sourceColumnName;
    protected String outputValueName;
}
