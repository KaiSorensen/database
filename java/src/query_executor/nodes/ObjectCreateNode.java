package query_executor.nodes;

import query_executor.DataInterface;
import query_executor.ValidationResult;

public class ObjectCreateNode extends ObjectStatementNode {
    protected String parentObjectName;
    protected boolean extendParentAttributes;

    @Override
    public ValidationResult validate() {
        ValidationResult crudValidation = requireCrudEngine("ObjectCreateNode");
        if (!crudValidation.isValid()) {
            return crudValidation;
        }
        if (objectName == null || objectName.isBlank()) {
            return ValidationResult.invalid("ObjectCreateNode requires an object name.");
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
            crudEngine.createObject(objectName, parentObjectName);
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to create object: " + objectName, exception);
        }
        return dataContext;
    }
}
