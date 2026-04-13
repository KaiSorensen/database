package query_executor;

import crud_engine.CrudEngineInterface;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import query_executor.ast.AstDocument;
import query_executor.ast.IndexedAstParser;
import query_executor.nodes.IndexedAstBinder;
import query_executor.nodes.Node;

public final class QueryExecutor {
    private final CrudEngineInterface crudEngine;
    private final IndexedAstParser astParser;
    private final IndexedAstBinder astBinder;

    public QueryExecutor(CrudEngineInterface crudEngine) {
        this.crudEngine = crudEngine;
        this.astParser = new IndexedAstParser();
        this.astBinder = new IndexedAstBinder();
    }

    public QueryExecutionResult executeAstText(String astText) {
        try {
            // Parse the formal indexed AST text into a structured document first.
            AstDocument document = astParser.parse(astText);
            // Bind the AST records into the real executor node hierarchy.
            Node root = astBinder.bind(document);
            root.setCrudEngine(crudEngine);
            ValidationResult validation = root.validate();
            if (!validation.isValid()) {
                return QueryExecutionResult.failure(validation.message());
            }
            DataInterface result = root.execute();
            return QueryExecutionResult.success("Execution succeeded.", result);
        } catch (Exception exception) {
            return QueryExecutionResult.failure(exception.getMessage());
        }
    }

    public QueryExecutionResult executeAstFile(Path astPath) throws IOException {
        return executeAstText(Files.readString(astPath));
    }
}
