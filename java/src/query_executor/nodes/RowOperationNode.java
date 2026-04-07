package query_executor.nodes;

import java.util.List;

public class RowOperationNode extends Node {
    // Combination: DataRead (single or pair).
    protected DataReadNode primaryDataReadChild;
    protected DataReadNode secondaryDataReadChild;

    // Combination: Selection (single or pair).
    protected SelectionNode primarySelectionChild;
    protected SelectionNode secondarySelectionChild;

    // Combination: RowOperation (single or pair).
    protected RowOperationNode primaryRowOperationChild;
    protected RowOperationNode secondaryRowOperationChild;

    // Combination: ColumnOperation (single or pair).
    protected ColumnOperationNode primaryColumnOperationChild;
    protected ColumnOperationNode secondaryColumnOperationChild;

    protected RowOperationKind operationKind;
    protected List<String> sourceNames;
    protected String outputColumnName;
}
