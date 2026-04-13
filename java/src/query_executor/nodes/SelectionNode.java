package query_executor.nodes;

import java.io.IOException;
import query_executor.DataInterface;
import query_executor.Data;
import query_executor.FilterExpression;
import query_executor.ValidationResult;
import java.util.List;

public class SelectionNode extends Node {
    protected String objectName;
    protected List<String> attributeNames;
    protected FilterExpression filterExpression;

    @Override
    public ValidationResult validate() {
        // The selection must know which object it conceptually targets.
        if (objectName == null || objectName.isBlank()) {
            return ValidationResult.invalid("SelectionNode requires an object name.");
        }
        // Source selections can lazily load their base object view from the CRUD layer.
        if (dataContext == null) {
            ValidationResult crudValidation = requireCrudEngine("SelectionNode");
            if (!crudValidation.isValid()) {
                return crudValidation;
            }
            try {
                dataContext = Data.fromCrudEngine(crudEngine, objectName);
            } catch (IOException exception) {
                return ValidationResult.invalid("SelectionNode failed to load source data for object: " + objectName);
            }
        }
        // A selection with no requested attributes has no output shape.
        if (attributeNames == null || attributeNames.isEmpty()) {
            return ValidationResult.invalid("SelectionNode requires at least one attribute name.");
        }
        // Every requested attribute must exist in the current data context.
        for (String attributeName : attributeNames) {
            if (!dataContext.hasColumn(attributeName)) {
                return ValidationResult.invalid("SelectionNode references unknown column: " + attributeName);
            }
        }
        // Filter references are validated separately because they may mention columns
        // that are not part of the returned projection.
        if (filterExpression != null) {
            for (String referencedColumn : filterExpression.referencedColumns()) {
                if (!dataContext.hasColumn(referencedColumn)) {
                    return ValidationResult.invalid("SelectionNode filter references unknown column: " + referencedColumn);
                }
            }
        }
        return ValidationResult.valid();
    }

    @Override
    public DataInterface execute() {
        ValidationResult validation = validate();
        if (!validation.isValid()) {
            throw new IllegalStateException(validation.message());
        }
        // Selection applies filter first and projection second so filter-only columns do not
        // have to appear in the returned attribute list.
        dataContext = dataContext.applySelection(attributeNames, filterExpression);
        return dataContext;
    }
}
