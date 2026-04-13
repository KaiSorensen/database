package query_executor.nodes;

import java.util.List;
import query_executor.DataInterface;
import query_executor.ValidationResult;

public class ColumnOperationNode extends Node {
    // Combination: DataRead (single or pair).
    protected DataReadNode primaryDataReadChild;
    protected DataReadNode secondaryDataReadChild;

    // Combination: Join (single or pair).
    protected JoinNode primaryJoinChild;
    protected JoinNode secondaryJoinChild;

    // Combination: Selection (single or pair).
    protected SelectionNode primarySelectionChild;
    protected SelectionNode secondarySelectionChild;

    // Combination: RowOperation (single or pair).
    protected RowOperationNode primaryRowOperationChild;
    protected RowOperationNode secondaryRowOperationChild;

    protected ColumnOperationKind operationKind;
    protected String sourceColumnName;
    protected String outputValueName;

    @Override
    public ValidationResult validate() {
        // Exactly one family of child inputs must be active for this operation node.
        ValidationResult pathValidation = requireExactlyOne(
            "ColumnOperationNode",
            activeDataReadPath(),
            activeJoinPath(),
            activeSelectionPath(),
            activeRowOperationPath()
        );
        if (!pathValidation.isValid()) {
            return pathValidation;
        }
        // Secondary inputs are only legal when the matching primary input exists too.
        ValidationResult secondaryValidation =
            requirePrimaryWhenSecondaryPresent("ColumnOperationNode", primaryDataReadChild, secondaryDataReadChild, "DataRead")
                .and(requirePrimaryWhenSecondaryPresent("ColumnOperationNode", primaryJoinChild, secondaryJoinChild, "Join"))
                .and(requirePrimaryWhenSecondaryPresent("ColumnOperationNode", primarySelectionChild, secondarySelectionChild, "Selection"))
                .and(requirePrimaryWhenSecondaryPresent("ColumnOperationNode", primaryRowOperationChild, secondaryRowOperationChild, "RowOperation"));
        if (!secondaryValidation.isValid()) {
            return secondaryValidation;
        }
        // The operation kind determines which aggregation or reduction is applied.
        if (operationKind == null) {
            return ValidationResult.invalid("ColumnOperationNode requires an operation kind.");
        }
        // The source column identifies which single column is being reduced.
        if (sourceColumnName == null || sourceColumnName.isBlank()) {
            return ValidationResult.invalid("ColumnOperationNode requires a source column name.");
        }
        return validateActiveChildren();
    }

    @Override
    public DataInterface execute() {
        ValidationResult validation = validate();
        if (!validation.isValid()) {
            throw new IllegalStateException(validation.message());
        }

        DataInterface primaryData = resolvePrimaryData();
        DataInterface secondaryData = resolveSecondaryData();
        DataInterface workingData = primaryData;

        if (secondaryData != null) {
            // Binary column operations follow the same merge rule as row operations:
            // the two child results must already describe the same result-row set.
            if (!primaryData.hasExactRowSetMatch(secondaryData)) {
                throw new IllegalStateException("ColumnOperationNode requires exact row-set match between child results.");
            }
            workingData = primaryData.mergeColumnsFromExactMatch(secondaryData);
        }

        // Column operations collapse one column into one scalar, so the input shape
        // must already have been narrowed to a single source column.
        if (!workingData.isSingleColumn()) {
            throw new IllegalStateException("ColumnOperationNode requires a single-column input.");
        }
        dataContext = workingData.applyColumnOperation(operationKind, sourceColumnName, outputValueName);
        return dataContext;
    }

    private Object activeDataReadPath() {
        return primaryDataReadChild != null ? primaryDataReadChild : secondaryDataReadChild;
    }

    private Object activeSelectionPath() {
        return primarySelectionChild != null ? primarySelectionChild : secondarySelectionChild;
    }

    private Object activeJoinPath() {
        return primaryJoinChild != null ? primaryJoinChild : secondaryJoinChild;
    }

    private Object activeRowOperationPath() {
        return primaryRowOperationChild != null ? primaryRowOperationChild : secondaryRowOperationChild;
    }

    private ValidationResult validateActiveChildren() {
        // Validate both members of the active pair when the inputs come from reads.
        if (primaryDataReadChild != null) {
            ValidationResult result = validateChild(primaryDataReadChild);
            if (!result.isValid()) {
                return result;
            }
            return secondaryDataReadChild == null ? ValidationResult.valid() : validateChild(secondaryDataReadChild);
        }
        // Validate both members of the active pair when the inputs come from selections.
        if (primarySelectionChild != null) {
            ValidationResult result = validateChild(primarySelectionChild);
            if (!result.isValid()) {
                return result;
            }
            return secondarySelectionChild == null ? ValidationResult.valid() : validateChild(secondarySelectionChild);
        }
        // Validate both members of the active pair when the inputs come from joins.
        if (primaryJoinChild != null) {
            ValidationResult result = validateChild(primaryJoinChild);
            if (!result.isValid()) {
                return result;
            }
            return secondaryJoinChild == null ? ValidationResult.valid() : validateChild(secondaryJoinChild);
        }
        // Otherwise the active pair comes from row-operation children.
        ValidationResult result = validateChild(primaryRowOperationChild);
        if (!result.isValid()) {
            return result;
        }
        return secondaryRowOperationChild == null ? ValidationResult.valid() : validateChild(secondaryRowOperationChild);
    }

    private DataInterface resolvePrimaryData() {
        // Resolve the primary input from whichever child family is active.
        if (primaryDataReadChild != null) {
            return executeChild(primaryDataReadChild);
        }
        if (primarySelectionChild != null) {
            return executeChild(primarySelectionChild);
        }
        if (primaryJoinChild != null) {
            return executeChild(primaryJoinChild);
        }
        return executeChild(primaryRowOperationChild);
    }

    private DataInterface resolveSecondaryData() {
        // Resolve the optional secondary input from the same active child family.
        if (secondaryDataReadChild != null) {
            return executeChild(secondaryDataReadChild);
        }
        if (secondarySelectionChild != null) {
            return executeChild(secondarySelectionChild);
        }
        if (secondaryJoinChild != null) {
            return executeChild(secondaryJoinChild);
        }
        return secondaryRowOperationChild == null ? null : executeChild(secondaryRowOperationChild);
    }
}
