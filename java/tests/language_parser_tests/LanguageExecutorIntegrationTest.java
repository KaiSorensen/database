package language_parser_tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import crud_engine.CrudEngine;
import crud_engine.DatabaseSchema;
import java.nio.file.Path;
import language_parser.LanguageExecutor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import query_executor.QueryExecutionResult;

public class LanguageExecutorIntegrationTest {
    @TempDir
    Path tempDir;

    @Test
    void executesObjectAndAttributeSchemaStatements() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("lang-schema"))) {
            LanguageExecutor executor = new LanguageExecutor(engine);

            QueryExecutionResult createObject = executor.execute("create_object(people);");
            QueryExecutionResult createExtendingObject = executor.execute("create_object(employees, extends(people));");
            QueryExecutionResult createAttribute = executor.execute("create_attribute(people, age, int);");
            QueryExecutionResult readObject = executor.execute("read_object(people, include_attributes(), include_row_count());");
            QueryExecutionResult readAttribute = executor.execute("read_attribute(people, age, include_type());");
            QueryExecutionResult updateAttribute = executor.execute("update_attribute(people, age, rename(years));");
            QueryExecutionResult deleteAttribute = executor.execute("delete_attribute(people, years);");

            DatabaseSchema schema = engine.readSchema();

            assertTrue(createObject.success());
            assertTrue(createExtendingObject.success());
            assertTrue(createAttribute.success());
            assertTrue(readObject.success());
            assertTrue(readAttribute.success());
            assertTrue(updateAttribute.success());
            assertTrue(deleteAttribute.success());
            assertTrue(schema.getObjects().containsKey("people"));
            assertTrue(schema.getObjects().containsKey("employees"));
            assertFalse(schema.getObjects().get("people").getAttributes().containsKey("years"));
        }
    }

    @Test
    void executesReadWithAttributesAndWhere() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("lang-read"))) {
            seedPeople(engine);
            QueryExecutionResult result = new LanguageExecutor(engine)
                .execute("read(people, attributes(name, age), where(greater_than(age, 20)));");

            assertTrue(result.success());
            assertEquals(2, result.returnedData().getRowCount());
            assertEquals(java.util.List.of("Bob", "Cara"), result.returnedData().getColumnValues("people.name"));
        }
    }

    @Test
    void executesReadWithDerive() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("lang-derive"))) {
            seedPeople(engine);
            QueryExecutionResult result = new LanguageExecutor(engine)
                .execute("read(people, derive(total, add(age, bonus)));");

            assertTrue(result.success());
            assertEquals(java.util.List.of(25, 37, 34), result.returnedData().getColumnValues("people.total"));
        }
    }

    @Test
    void executesReadWithSummarize() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("lang-summarize"))) {
            seedPeople(engine);
            QueryExecutionResult result = new LanguageExecutor(engine)
                .execute("read(people, summarize(age_sum, sum(age)));");

            assertTrue(result.success());
            assertEquals(java.util.List.of(75), result.returnedData().getColumnValues("people.age_sum"));
        }
    }

    @Test
    void executesJoinRead() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("lang-join"))) {
            seedPeople(engine);
            seedPets(engine);
            QueryExecutionResult result = new LanguageExecutor(engine)
                .execute("read(join(people, pets, on(equals(people.name, pets.owner_name))), attributes(people.name, pets.pet_name));");

            assertTrue(result.success());
            assertEquals(2, result.returnedData().getRowCount());
            assertEquals(java.util.List.of("Ada", "Cara"), result.returnedData().getColumnValues("people.name"));
        }
    }

    @Test
    void executesCreateRowsAndDerivedAttribute() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("lang-create"))) {
            seedPeople(engine);
            LanguageExecutor executor = new LanguageExecutor(engine);

            QueryExecutionResult createRows = executor.execute(
                "create_rows(people, values(row(name(\"Dora\"), age(40), bonus(11)), row(name(\"Evan\"), age(21), bonus(3))));"
            );
            QueryExecutionResult createDerivedAttribute = executor.execute(
                "create_attribute(people, total, int, derive(add(age, bonus)));"
            );

            DatabaseSchema schema = engine.readSchema();

            assertTrue(createRows.success());
            assertTrue(createDerivedAttribute.success());
            assertEquals(5, engine.getRowCount("People"));
            assertTrue(schema.getObjects().get("people").getAttributes().containsKey("total"));
            assertEquals(25, engine.readInt("People", "Total", 0));
            assertEquals(51, engine.readInt("People", "Total", 3));
        }
    }

    @Test
    void executesUpdateRowExpressionAndScalarBroadcast() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("lang-update"))) {
            seedPeople(engine);
            LanguageExecutor executor = new LanguageExecutor(engine);

            QueryExecutionResult rowAligned = executor.execute(
                "update(people, set(age, add(age, bonus)), where(greater_than(age, 20)));"
            );
            QueryExecutionResult scalarBroadcast = executor.execute(
                "update(people, set(bonus, max(age)), where(greater_than(age, 20)));"
            );

            assertTrue(rowAligned.success());
            assertTrue(scalarBroadcast.success());
            assertEquals(20, engine.readInt("People", "Age", 0));
            assertEquals(37, engine.readInt("People", "Age", 1));
            assertEquals(34, engine.readInt("People", "Age", 2));
            assertEquals(5, engine.readInt("People", "Bonus", 0));
            assertEquals(37, engine.readInt("People", "Bonus", 1));
            assertEquals(37, engine.readInt("People", "Bonus", 2));
        }
    }

    @Test
    void executesDeleteRowsAndAttributes() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("lang-delete"))) {
            seedPeople(engine);
            LanguageExecutor executor = new LanguageExecutor(engine);

            QueryExecutionResult deleteRows = executor.execute(
                "delete_rows(people, where(greater_than(age, 20)));"
            );
            QueryExecutionResult deleteAttributes = executor.execute(
                "delete_attributes(people, attributes(bonus));"
            );

            DatabaseSchema schema = engine.readSchema();

            assertTrue(deleteRows.success());
            assertTrue(deleteAttributes.success());
            assertEquals(1, engine.getRowCount("People"));
            assertFalse(schema.getObjects().get("people").getAttributes().containsKey("bonus"));
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

        int row0 = engine.insertRow("People");
        engine.writeString("People", "Name", row0, "Ada");
        engine.writeInt("People", "Age", row0, 20);
        engine.writeInt("People", "Bonus", row0, 5);

        int row1 = engine.insertRow("People");
        engine.writeString("People", "Name", row1, "Bob");
        engine.writeInt("People", "Age", row1, 30);
        engine.writeInt("People", "Bonus", row1, 7);

        int row2 = engine.insertRow("People");
        engine.writeString("People", "Name", row2, "Cara");
        engine.writeInt("People", "Age", row2, 25);
        engine.writeInt("People", "Bonus", row2, 9);
    }

    private static void seedPets(CrudEngine engine) throws Exception {
        engine.createObject("Pets", null);
        engine.createAttribute("Pets", "Owner_Name", crud_engine.CrudEngineInterface.AttributeType.STRING);
        engine.createAttribute("Pets", "Pet_Name", crud_engine.CrudEngineInterface.AttributeType.STRING);

        int row0 = engine.insertRow("Pets");
        engine.writeString("Pets", "Owner_Name", row0, "Ada");
        engine.writeString("Pets", "Pet_Name", row0, "Milo");

        int row1 = engine.insertRow("Pets");
        engine.writeString("Pets", "Owner_Name", row1, "Cara");
        engine.writeString("Pets", "Pet_Name", row1, "Nora");
    }
}
