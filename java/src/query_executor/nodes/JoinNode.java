package query_executor.nodes;

import query_executor.DataInterface;
import query_executor.JoinSpec;
import query_executor.ValidationResult;

public class JoinNode extends Node {
    protected Node leftChild;
    protected Node rightChild;
    protected JoinSpec joinSpec;

    @Override
    public ValidationResult validate() {
        if (leftChild == null || rightChild == null) {
            return ValidationResult.invalid("JoinNode requires both left and right child nodes.");
        }
        if (joinSpec == null) {
            return ValidationResult.invalid("JoinNode requires a join specification.");
        }
        ValidationResult leftValidation = validateChild(leftChild);
        if (!leftValidation.isValid()) {
            return leftValidation;
        }
        return validateChild(rightChild);
    }

    @Override
    public DataInterface execute() {
        ValidationResult validation = validate();
        if (!validation.isValid()) {
            throw new IllegalStateException(validation.message());
        }
        DataInterface leftData = executeChild(leftChild);
        DataInterface rightData = executeChild(rightChild);
        dataContext = leftData.join(rightData, joinSpec);
        return dataContext;
    }
}
