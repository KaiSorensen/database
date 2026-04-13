package language_parser;

import crud_engine.CrudEngineInterface;
import language_parser.ast.AstNodes;
import language_parser.lowering.SemanticAstLowerer;
import language_parser.parser.Parser;
import query_executor.QueryExecutionResult;
import query_executor.QueryExecutor;

public final class LanguageExecutor {
    private final Parser parser;
    private final SemanticAstLowerer lowerer;
    private final QueryExecutor queryExecutor;

    public LanguageExecutor(CrudEngineInterface crudEngine) {
        this.parser = new Parser();
        this.lowerer = new SemanticAstLowerer(crudEngine);
        this.queryExecutor = new QueryExecutor(crudEngine);
    }

    public QueryExecutionResult execute(String source) {
        try {
            AstNodes.ScriptNode script = parser.parse(source);
            QueryExecutionResult lastResult = QueryExecutionResult.success("No statements executed.", null);
            for (AstNodes.StatementNode statement : script.statements()) {
                String astText = lowerer.lowerToIndexedAstText(statement);
                lastResult = queryExecutor.executeAstText(astText);
                if (!lastResult.success()) {
                    return lastResult;
                }
            }
            return lastResult;
        } catch (Exception exception) {
            return QueryExecutionResult.failure(exception.getMessage());
        }
    }
}
