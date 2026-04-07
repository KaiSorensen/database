package query_executor.nodes;

import java.util.List;

public class SelectionNode extends Node {
    protected String objectName;
    protected List<String> attributeNames;
    protected List<String> filterExpressions;
}
