package query_executor.nodes;

import java.util.List;
import query_executor.DataInterface;
import query_executor.ValidationResult;

public class DataUpdateNode extends DataStatementNode {
    // Target selection path.
    protected SelectionNode targetSelectionChild;

    // Value expression path.
    protected SelectionNode valueSelectionChild;

    // Value expression path.
    protected RowOperationNode valueRowOperationChild;

    // Value expression path.
    protected ColumnOperationNode valueColumnOperationChild;

    // Value expression path.
    protected JoinNode valueJoinChild;

    protected List<String> targetNames;

    @Override
    public ValidationResult validate() {
        ValidationResult crudValidation = requireCrudEngine("DataUpdateNode");
        if (!crudValidation.isValid()) {
            return crudValidation;
        }
        // Update requires one subtree to identify target rows and one subtree to produce replacement values.
        if (targetSelectionChild == null) {
            return ValidationResult.invalid("DataUpdateNode requires a target selection child.");
        }
        if (targetNames == null || targetNames.isEmpty()) {
            return ValidationResult.invalid("DataUpdateNode requires at least one target name.");
        }
        ValidationResult valueValidation = requireExactlyOne(
            "DataUpdateNode",
            valueSelectionChild,
            valueRowOperationChild,
            valueColumnOperationChild,
            valueJoinChild
        );
        if (!valueValidation.isValid()) {
            return valueValidation;
        }
        ValidationResult targetValidation = validateChild(targetSelectionChild);
        if (!targetValidation.isValid()) {
            return targetValidation;
        }
        if (valueSelectionChild != null) {
            return validateChild(valueSelectionChild);
        }
        if (valueRowOperationChild != null) {
            return validateChild(valueRowOperationChild);
        }
        if (valueColumnOperationChild != null) {
            return validateChild(valueColumnOperationChild);
        }
        return validateChild(valueJoinChild);
    }

    @Override
    public DataInterface execute() {
        ValidationResult validation = validate();
        if (!validation.isValid()) {
            throw new IllegalStateException(validation.message());
        }
        DataInterface sourceData = dataContext;
        // Evaluate the target row set from the original source data.
        DataInterface targetData = executeChild(targetSelectionChild);
        dataContext = sourceData;
        // Evaluate the replacement values independently from the same original source data.
        DataInterface valueData;
        if (valueSelectionChild != null) {
            valueData = executeChild(valueSelectionChild);
        } else if (valueRowOperationChild != null) {
            valueData = executeChild(valueRowOperationChild);
        } else if (valueColumnOperationChild != null) {
            valueData = executeChild(valueColumnOperationChild);
        } else {
            valueData = executeChild(valueJoinChild);
        }
        // Replacement values must either line up row-for-row with the targets or be a single broadcast scalar.
        if (!valueData.fitsUpdateTarget(targetData, objectName, targetNames)) {
            throw new IllegalStateException("DataUpdateNode requires replacement values that fit the selected target rows.");
        }
        try {
            // Build and validate all writes before the first persistent mutation occurs.
            commitUpdates(((query_executor.Data) valueData).toCellUpdateRequests(objectName, targetNames, targetData));
            dataContext = refreshDataContext();
            return dataContext;
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to update data for object: " + objectName, exception);
        }
    }

    @Override
    protected Iterable<String> targetNames() {
        return targetNames;
    }
}
