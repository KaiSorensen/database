package query_executor.nodes;

import query_executor.Data;

public abstract class Node {
    protected String nodeId;
    protected String nodeLabel;
    protected Node parentNode;
    protected Data dataContext;
}
