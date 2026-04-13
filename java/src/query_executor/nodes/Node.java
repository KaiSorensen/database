package query_executor.nodes;

import crud_engine.CrudEngineInterface;
import query_executor.DataInterface;
import query_executor.ValidationResult;

public abstract class Node {
    protected String nodeId;
    protected String nodeLabel;
    protected Node parentNode;
    protected DataInterface dataContext;
    protected CrudEngineInterface crudEngine;

    public abstract ValidationResult validate();

    public abstract DataInterface execute();

    public void setDataContext(DataInterface dataContext) {
        this.dataContext = dataContext;
    }

    public void setCrudEngine(CrudEngineInterface crudEngine) {
        this.crudEngine = crudEngine;
    }

    protected int countNonNull(Object... values) {
        int count = 0;
        for (Object value : values) {
            if (value != null) {
                count++;
            }
        }
        return count;
    }

    protected ValidationResult requireExactlyOne(String nodeName, Object... values) {
        if (countNonNull(values) != 1) {
            return ValidationResult.invalid(nodeName + " requires exactly one active child path.");
        }
        return ValidationResult.valid();
    }

    protected ValidationResult requirePrimaryWhenSecondaryPresent(
        String nodeName,
        Object primary,
        Object secondary,
        String childLabel
    ) {
        if (secondary != null && primary == null) {
            return ValidationResult.invalid(
                nodeName + " cannot use a secondary " + childLabel + " child without a primary " + childLabel + " child."
            );
        }
        return ValidationResult.valid();
    }

    protected ValidationResult requireDataContext(String nodeName) {
        if (dataContext == null) {
            return ValidationResult.invalid(nodeName + " requires a data context before execution.");
        }
        return ValidationResult.valid();
    }

    protected ValidationResult requireCrudEngine(String nodeName) {
        if (crudEngine == null) {
            return ValidationResult.invalid(nodeName + " requires a CRUD engine before execution.");
        }
        return ValidationResult.valid();
    }

    protected ValidationResult validateChild(Node child) {
        child.parentNode = this;
        // Child nodes inherit the current data view unless a test or a higher-level
        // executor has deliberately seeded them with something more specific.
        if (child.dataContext == null) {
            child.dataContext = this.dataContext;
        }
        if (child.crudEngine == null) {
            child.crudEngine = this.crudEngine;
        }
        return child.validate();
    }

    protected DataInterface executeChild(Node child) {
        child.parentNode = this;
        // Execution uses the same context-propagation rule as validation so a subtree
        // can be executed from any root without a separate executor wrapper.
        if (child.dataContext == null) {
            child.dataContext = this.dataContext;
        }
        if (child.crudEngine == null) {
            child.crudEngine = this.crudEngine;
        }
        return child.execute();
    }
}
