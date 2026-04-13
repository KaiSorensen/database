package query_executor.nodes;

import query_executor.DataInterface;
import query_executor.ValidationResult;

public class AttributeDeleteNode extends AttributeStatementNode {
    @Override
    public ValidationResult validate() {
        ValidationResult crudValidation = requireCrudEngine("AttributeDeleteNode");
        if (!crudValidation.isValid()) {
            return crudValidation;
        }
        if (objectName == null || objectName.isBlank()) {
            return ValidationResult.invalid("AttributeDeleteNode requires an object name.");
        }
        if (attributeName == null || attributeName.isBlank()) {
            return ValidationResult.invalid("AttributeDeleteNode requires an attribute name.");
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
            crudEngine.deleteAttribute(objectName, attributeName);
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to delete attribute: " + objectName + "." + attributeName, exception);
        }
        return dataContext;
    }
}
