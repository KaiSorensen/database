package query_executor.nodes;

import query_executor.DataInterface;
import query_executor.ValidationResult;

public class ObjectDeleteNode extends ObjectStatementNode {
    @Override
    public ValidationResult validate() {
        ValidationResult crudValidation = requireCrudEngine("ObjectDeleteNode");
        if (!crudValidation.isValid()) {
            return crudValidation;
        }
        if (objectName == null || objectName.isBlank()) {
            return ValidationResult.invalid("ObjectDeleteNode requires an object name.");
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
            crudEngine.deleteObject(objectName);
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to delete object: " + objectName, exception);
        }
        return dataContext;
    }
}
