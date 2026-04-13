package query_executor.ast;

public enum AstNodeType {
    ObjectCreate,
    ObjectRead,
    ObjectUpdate,
    ObjectDelete,
    AttributeCreate,
    AttributeRead,
    AttributeUpdate,
    AttributeDelete,
    DataRead,
    DataCreate,
    DataUpdate,
    DataDelete,
    Selection,
    RowOperation,
    ColumnOperation,
    Join
}
