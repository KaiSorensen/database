package query_executor.nodes;

import query_executor.DataInterface;
import query_executor.ValidationResult;

public class ObjectReadNode extends ObjectStatementNode {
    protected boolean includeAttributeMetadata;
    protected boolean includeRowCount;

    @Override
    public ValidationResult validate() {
        ValidationResult crudValidation = requireCrudEngine("ObjectReadNode");
        if (!crudValidation.isValid()) {
            return crudValidation;
        }
        if (objectName == null || objectName.isBlank()) {
            return ValidationResult.invalid("ObjectReadNode requires an object name.");
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
            if (!crudEngine.readSchema().getObjects().containsKey(objectName.trim().toLowerCase().replaceAll("[^a-z0-9]+", "_").replaceAll("^_+|_+$", ""))) {
                throw new IllegalStateException("Unknown object: " + objectName);
            }
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to read object metadata: " + objectName, exception);
        }
        return dataContext;
    }
}
