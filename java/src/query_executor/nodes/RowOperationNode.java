package query_executor.nodes;

import java.util.List;
import query_executor.DataInterface;
import query_executor.ValidationResult;

public class RowOperationNode extends Node {
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

    // Combination: ColumnOperation (single or pair).
    protected ColumnOperationNode primaryColumnOperationChild;
    protected ColumnOperationNode secondaryColumnOperationChild;

    protected RowOperationKind operationKind;
    protected List<String> sourceNames;
    protected String outputColumnName;

    @Override
    public ValidationResult validate() {
        // Exactly one family of child inputs must be active for this operation node.
        ValidationResult pathValidation = requireExactlyOne(
            "RowOperationNode",
            activeDataReadPath(),
            activeJoinPath(),
            activeSelectionPath(),
            activeRowOperationPath(),
            activeColumnOperationPath()
        );
        if (!pathValidation.isValid()) {
            return pathValidation;
        }
        // Secondary inputs are only legal when the matching primary input exists too.
        ValidationResult secondaryValidation =
            requirePrimaryWhenSecondaryPresent("RowOperationNode", primaryDataReadChild, secondaryDataReadChild, "DataRead")
                .and(requirePrimaryWhenSecondaryPresent("RowOperationNode", primaryJoinChild, secondaryJoinChild, "Join"))
                .and(requirePrimaryWhenSecondaryPresent("RowOperationNode", primarySelectionChild, secondarySelectionChild, "Selection"))
                .and(requirePrimaryWhenSecondaryPresent("RowOperationNode", primaryRowOperationChild, secondaryRowOperationChild, "RowOperation"))
                .and(requirePrimaryWhenSecondaryPresent("RowOperationNode", primaryColumnOperationChild, secondaryColumnOperationChild, "ColumnOperation"));
        if (!secondaryValidation.isValid()) {
            return secondaryValidation;
        }
        // The operation kind determines which row-wise calculation is applied.
        if (operationKind == null) {
            return ValidationResult.invalid("RowOperationNode requires an operation kind.");
        }
        // The source names tell Data which columns to feed into the row-wise calculation.
        if (sourceNames == null || sourceNames.isEmpty()) {
            return ValidationResult.invalid("RowOperationNode requires source names.");
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
            // Two-input row operations are only allowed to combine results that still
            // refer to the exact same ordered result-row bindings. JoinNode exists for
            // the cases where the caller wants explicit row-shape reconciliation.
            if (!primaryData.hasExactRowSetMatch(secondaryData)) {
                throw new IllegalStateException("RowOperationNode requires exact row-set match between child results.");
            }
            workingData = primaryData.mergeColumnsFromExactMatch(secondaryData);
        }

        dataContext = workingData.applyRowOperation(operationKind, sourceNames, outputColumnName);
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

    private Object activeColumnOperationPath() {
        return primaryColumnOperationChild != null ? primaryColumnOperationChild : secondaryColumnOperationChild;
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
        // Validate both members of the active pair when the inputs come from row operations.
        if (primaryRowOperationChild != null) {
            ValidationResult result = validateChild(primaryRowOperationChild);
            if (!result.isValid()) {
                return result;
            }
            return secondaryRowOperationChild == null ? ValidationResult.valid() : validateChild(secondaryRowOperationChild);
        }
        // Otherwise the active pair comes from column-operation children.
        ValidationResult result = validateChild(primaryColumnOperationChild);
        if (!result.isValid()) {
            return result;
        }
        return secondaryColumnOperationChild == null ? ValidationResult.valid() : validateChild(secondaryColumnOperationChild);
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
        if (primaryRowOperationChild != null) {
            return executeChild(primaryRowOperationChild);
        }
        return executeChild(primaryColumnOperationChild);
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
        if (secondaryRowOperationChild != null) {
            return executeChild(secondaryRowOperationChild);
        }
        return secondaryColumnOperationChild == null ? null : executeChild(secondaryColumnOperationChild);
    }
}
