package language_parser_tests;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import language_parser.LanguageConsoleSupport;
import language_parser.ast.AstNodes;
import language_parser.parser.Parser;
import org.junit.jupiter.api.Test;
import query_executor.Data;

import java.util.LinkedHashMap;
import java.util.List;

public class LanguageConsoleSupportTest {
    private final Parser parser = new Parser();

    @Test
    void classifiesSingleReadStatementsOnly() {
        AstNodes.ScriptNode readScript = parser.parse("read(people, attributes(name));");
        AstNodes.ScriptNode updateScript = parser.parse("update(people, set(age, add(age, age)));");

        assertTrue(LanguageConsoleSupport.shouldPrintReadResult(readScript));
        assertFalse(LanguageConsoleSupport.shouldPrintReadResult(updateScript));
    }

    @Test
    void formatsTabularReadResults() {
        LinkedHashMap<String, crud_engine.CrudEngineInterface.AttributeType> columnTypes = new LinkedHashMap<>();
        columnTypes.put("name", crud_engine.CrudEngineInterface.AttributeType.STRING);
        columnTypes.put("age", crud_engine.CrudEngineInterface.AttributeType.INT);

        LinkedHashMap<String, List<Object>> columnValues = new LinkedHashMap<>();
        columnValues.put("name", List.of("Ada", "Bob"));
        columnValues.put("age", List.of(20, 30));

        String rendered = LanguageConsoleSupport.formatQueryData(
            Data.fromStoredObject("people", columnTypes, columnValues)
        );

        assertTrue(rendered.contains("people.name"));
        assertTrue(rendered.contains("\"Ada\""));
        assertTrue(rendered.contains("(2 rows)"));
    }
}
