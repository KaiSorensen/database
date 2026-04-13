package query_executor.nodes;

import query_executor.DataInterface;
import query_executor.ValidationResult;

public class AttributeUpdateNode extends AttributeStatementNode {
    protected String newAttributeName;

    @Override
    public ValidationResult validate() {
        ValidationResult crudValidation = requireCrudEngine("AttributeUpdateNode");
        if (!crudValidation.isValid()) {
            return crudValidation;
        }
        if (objectName == null || objectName.isBlank()) {
            return ValidationResult.invalid("AttributeUpdateNode requires an object name.");
        }
        if (attributeName == null || attributeName.isBlank()) {
            return ValidationResult.invalid("AttributeUpdateNode requires an attribute name.");
        }
        if (newAttributeName == null || newAttributeName.isBlank()) {
            return ValidationResult.invalid("AttributeUpdateNode requires a new attribute name.");
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
            crudEngine.renameAttribute(objectName, attributeName, newAttributeName);
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to update attribute: " + objectName + "." + attributeName, exception);
        }
        return dataContext;
    }
}
