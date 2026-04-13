package query_executor.nodes;

import crud_engine.CrudEngineInterface;
import query_executor.DataInterface;
import query_executor.ValidationResult;

public class AttributeCreateNode extends AttributeStatementNode {
    protected CrudEngineInterface.AttributeType attributeType;

    @Override
    public ValidationResult validate() {
        ValidationResult crudValidation = requireCrudEngine("AttributeCreateNode");
        if (!crudValidation.isValid()) {
            return crudValidation;
        }
        if (objectName == null || objectName.isBlank()) {
            return ValidationResult.invalid("AttributeCreateNode requires an object name.");
        }
        if (attributeName == null || attributeName.isBlank()) {
            return ValidationResult.invalid("AttributeCreateNode requires an attribute name.");
        }
        if (attributeType == null) {
            return ValidationResult.invalid("AttributeCreateNode requires an attribute type.");
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
            crudEngine.createAttribute(objectName, attributeName, attributeType);
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to create attribute: " + objectName + "." + attributeName, exception);
        }
        return dataContext;
    }
}
