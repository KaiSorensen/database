package query_executor.nodes;

import java.util.List;
import query_executor.DataInterface;
import query_executor.ValidationResult;

public class DataReadNode extends DataStatementNode {
    // One-source read path.
    protected SelectionNode selectionChild;

    // One-source read path.
    protected RowOperationNode rowOperationChild;

    // One-source read path.
    protected ColumnOperationNode columnOperationChild;

    // One-source read path.
    protected JoinNode joinChild;

    protected List<String> requestedAttributeNames;

    @Override
    public ValidationResult validate() {
        // Read nodes only allow one active child path so the source of the read result
        // is always structurally unambiguous.
        ValidationResult childValidation = requireExactlyOne(
            "DataReadNode",
            selectionChild,
            rowOperationChild,
            columnOperationChild,
            joinChild
        );
        if (!childValidation.isValid()) {
            return childValidation;
        }
        // Delegate validation to whichever subtree is active.
        if (selectionChild != null) {
            return validateChild(selectionChild);
        }
        if (rowOperationChild != null) {
            return validateChild(rowOperationChild);
        }
        if (columnOperationChild != null) {
            return validateChild(columnOperationChild);
        }
        return validateChild(joinChild);
    }

    @Override
    public DataInterface execute() {
        ValidationResult validation = validate();
        if (!validation.isValid()) {
            throw new IllegalStateException(validation.message());
        }
        // Execute the one active child path to produce the read result.
        if (selectionChild != null) {
            dataContext = executeChild(selectionChild);
        } else if (rowOperationChild != null) {
            dataContext = executeChild(rowOperationChild);
        } else if (columnOperationChild != null) {
            dataContext = executeChild(columnOperationChild);
        } else {
            dataContext = executeChild(joinChild);
        }
        // Reads are valid as long as the resulting data shape is readable at all.
        if (!dataContext.isReadable()) {
            throw new IllegalStateException("DataReadNode requires readable data.");
        }
        return dataContext;
    }
}
