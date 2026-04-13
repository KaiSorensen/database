package query_executor.nodes;

import java.util.List;
import query_executor.DataInterface;
import query_executor.ValidationResult;

public class DataDeleteNode extends DataStatementNode {
    // One-source delete path.
    protected SelectionNode selectionChild;

    // One-source delete path.
    protected RowOperationNode rowOperationChild;

    protected List<String> targetNames;

    @Override
    public ValidationResult validate() {
        ValidationResult crudValidation = requireCrudEngine("DataDeleteNode");
        if (!crudValidation.isValid()) {
            return crudValidation;
        }
        ValidationResult childValidation = requireExactlyOne(
            "DataDeleteNode",
            selectionChild,
            rowOperationChild
        );
        if (!childValidation.isValid()) {
            return childValidation;
        }
        if (targetNames == null || targetNames.isEmpty()) {
            return ValidationResult.invalid("DataDeleteNode requires at least one target name.");
        }
        if (selectionChild != null) {
            return validateChild(selectionChild);
        }
        return validateChild(rowOperationChild);
    }

    @Override
    public DataInterface execute() {
        ValidationResult validation = validate();
        if (!validation.isValid()) {
            throw new IllegalStateException(validation.message());
        }
        dataContext = selectionChild != null ? executeChild(selectionChild) : executeChild(rowOperationChild);
        boolean deletableAsRows = dataContext.isDeletableAsRows(objectName);
        boolean deletableAsColumns = dataContext.isDeletableAsColumns(objectName);
        if (!deletableAsRows && !deletableAsColumns) {
            throw new IllegalStateException("DataDeleteNode requires deletable row or column data for object: " + objectName);
        }
        try {
            // Row-shaped deletes remove stored rows; column-shaped deletes remove stored attributes.
            if (deletableAsRows && !deletableAsColumns) {
                commitRowDeletes();
            } else if (deletableAsColumns && !deletableAsRows) {
                commitColumnDeletes();
            } else {
                // When the shape is both row-valid and column-valid, default to row deletion.
                commitRowDeletes();
            }
            dataContext = refreshDataContext();
            return dataContext;
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to delete data for object: " + objectName, exception);
        }
    }

    @Override
    protected Iterable<String> targetNames() {
        return targetNames;
    }
}
