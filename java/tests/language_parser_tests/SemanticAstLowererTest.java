package language_parser_tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import crud_engine.CrudEngine;
import language_parser.ast.AstNodes;
import language_parser.lowering.SemanticAstLowerer;
import language_parser.parser.Parser;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SemanticAstLowererTest {
    private final Parser parser = new Parser();

    @TempDir
    Path tempDir;

    @Test
    void lowersReadWithWhereToSelectionAst() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("lower-read"))) {
            seedPeople(engine);
            SemanticAstLowerer lowerer = new SemanticAstLowerer(engine);
            AstNodes.ScriptNode script = parser.parse("read(people, attributes(name, age), where(greater_than(age, 20)));");

            String lowered = lowerer.lowerToIndexedAstText(script);

            assertEquals(
                """
                Node 0
                type: DataRead
                childRoles:
                - source: 1
                params:
                
                Node 1
                type: Selection
                params:
                - objectName: "people"
                - attributes: ["people.name", "people.age"]
                - filter: (GREATER_THAN people.age 20)""".trim(),
                lowered.trim()
            );
        }
    }

    @Test
    void lowersDeleteRowsWithSchemaExpandedAttributes() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("lower-delete-rows"))) {
            seedPeople(engine);
            SemanticAstLowerer lowerer = new SemanticAstLowerer(engine);
            AstNodes.ScriptNode script = parser.parse("delete_rows(people, where(greater_than(age, 20)));");

            String lowered = lowerer.lowerToIndexedAstText(script);

            assertTrue(lowered.contains("- targetNames: [\"people.name\", \"people.age\", \"people.bonus\"]"));
            assertTrue(lowered.contains("- attributes: [\"people.name\", \"people.age\", \"people.bonus\"]"));
        }
    }

    @Test
    void lowersCreateRowsIntoLiteralRowsAst() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("lower-create-rows"))) {
            seedPeople(engine);
            SemanticAstLowerer lowerer = new SemanticAstLowerer(engine);
            AstNodes.ScriptNode script = parser.parse(
                "create_rows(people, values(row(age(40), name(\"Dora\")), row(name(\"Evan\"), bonus(3))));"
            );

            String lowered = lowerer.lowerToIndexedAstText(script);

            assertTrue(lowered.contains("type: LiteralRows"));
            assertTrue(lowered.contains("- columnNames: [\"age\", \"name\", \"bonus\"]"));
            assertTrue(lowered.contains("- rows: [[40, \"Dora\", null], [null, \"Evan\", 3]]"));
        }
    }

    private static CrudEngine newEngine(Path path) throws Exception {
        CrudEngine engine = new CrudEngine(path);
        engine.initialize();
        return engine;
    }

    private static void seedPeople(CrudEngine engine) throws Exception {
        engine.createObject("People", null);
        engine.createAttribute("People", "Name", crud_engine.CrudEngineInterface.AttributeType.STRING);
        engine.createAttribute("People", "Age", crud_engine.CrudEngineInterface.AttributeType.INT);
        engine.createAttribute("People", "Bonus", crud_engine.CrudEngineInterface.AttributeType.INT);
    }
}
