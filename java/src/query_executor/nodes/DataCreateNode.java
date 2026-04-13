package query_executor.nodes;

import java.util.List;
import query_executor.ColumnCreateRequest;
import query_executor.DataInterface;
import query_executor.ValidationResult;

public class DataCreateNode extends DataStatementNode {
    // One-source create path.
    protected SelectionNode selectionChild;

    // One-source create path.
    protected RowOperationNode rowOperationChild;

    protected List<String> targetNames;
    protected String createdColumnName;

    @Override
    public ValidationResult validate() {
        ValidationResult crudValidation = requireCrudEngine("DataCreateNode");
        if (!crudValidation.isValid()) {
            return crudValidation;
        }
        ValidationResult childValidation = requireExactlyOne(
            "DataCreateNode",
            selectionChild,
            rowOperationChild
        );
        if (!childValidation.isValid()) {
            return childValidation;
        }
        if (targetNames == null || targetNames.isEmpty()) {
            return ValidationResult.invalid("DataCreateNode requires at least one target name.");
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
        boolean creatableAsRows = dataContext.isCreatableAsRows(objectName);
        boolean creatableAsColumns = dataContext.isCreatableAsColumns(objectName);
        if (!creatableAsRows && !creatableAsColumns) {
            throw new IllegalStateException("DataCreateNode requires data creatable as rows or columns for object: " + objectName);
        }
        try {
            // A row-operation create path is meant to materialize a new stored column.
            if (creatableAsColumns && (rowOperationChild != null || createdColumnName != null)) {
                commitColumnCreates();
            } else {
                // Otherwise treat the result as one or more full rows to append.
                commitRowCreates();
            }
            dataContext = refreshDataContext();
            return dataContext;
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to create data for object: " + objectName, exception);
        }
    }

    @Override
    protected Iterable<String> targetNames() {
        return targetNames;
    }
}
