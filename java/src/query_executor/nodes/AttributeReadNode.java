package query_executor.nodes;

import query_executor.DataInterface;
import query_executor.ValidationResult;

public class AttributeReadNode extends AttributeStatementNode {
    protected boolean includeAttributeType;

    @Override
    public ValidationResult validate() {
        ValidationResult crudValidation = requireCrudEngine("AttributeReadNode");
        if (!crudValidation.isValid()) {
            return crudValidation;
        }
        if (objectName == null || objectName.isBlank()) {
            return ValidationResult.invalid("AttributeReadNode requires an object name.");
        }
        if (attributeName == null || attributeName.isBlank()) {
            return ValidationResult.invalid("AttributeReadNode requires an attribute name.");
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
            var schema = crudEngine.readSchema();
            String objectSlug = objectName.trim().toLowerCase().replaceAll("[^a-z0-9]+", "_").replaceAll("^_+|_+$", "");
            String attributeSlug = attributeName.trim().toLowerCase().replaceAll("[^a-z0-9]+", "_").replaceAll("^_+|_+$", "");
            if (!schema.getObjects().containsKey(objectSlug) || !schema.getObjects().get(objectSlug).getAttributes().containsKey(attributeSlug)) {
                throw new IllegalStateException("Unknown attribute: " + objectName + "." + attributeName);
            }
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to read attribute metadata: " + objectName + "." + attributeName, exception);
        }
        return dataContext;
    }
}
