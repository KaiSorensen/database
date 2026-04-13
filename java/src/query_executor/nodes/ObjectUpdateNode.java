package query_executor.nodes;

import query_executor.DataInterface;
import query_executor.ValidationResult;

public class ObjectUpdateNode extends ObjectStatementNode {
    protected String newObjectName;

    @Override
    public ValidationResult validate() {
        ValidationResult crudValidation = requireCrudEngine("ObjectUpdateNode");
        if (!crudValidation.isValid()) {
            return crudValidation;
        }
        if (objectName == null || objectName.isBlank()) {
            return ValidationResult.invalid("ObjectUpdateNode requires an object name.");
        }
        if (newObjectName == null || newObjectName.isBlank()) {
            return ValidationResult.invalid("ObjectUpdateNode requires a new object name.");
        }
        return ValidationResult.valid();
    }

    @Override
    public DataInterface execute() {
        ValidationResult validation = validate();
        if (!validation.isValid()) {
            throw new IllegalStateException(validation.message());
        }
        try {
            crudEngine.renameObject(objectName, newObjectName);
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to update object: " + objectName, exception);
        }
        return dataContext;
    }
}
