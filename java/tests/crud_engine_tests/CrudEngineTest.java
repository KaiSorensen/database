package crud_engine_tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import crud_engine.AttributeSchema;
import crud_engine.CrudEngine;
import crud_engine.CrudEngineInterface.AttributeType;
import crud_engine.DatabaseSchema;
import crud_engine.ObjectSchema;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import memory_allocator.BitmapMemoryAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class CrudEngineTest {
    @TempDir
    Path tempDir;

    @Test
    void createObjectPersistsSchemaAcrossRestart() throws Exception {
        Path dbRoot = tempDir.resolve("db");

        try (CrudEngine engine = newEngine(dbRoot)) {
            engine.createObject("Animals", null);
            engine.createAttribute("Animals", "Name", AttributeType.STRING);
            engine.insertRow("Animals");
            engine.writeString("Animals", "Name", 0, "Giraffe");
        }

        try (CrudEngine engine = newEngine(dbRoot)) {
            DatabaseSchema schema = engine.readSchema();
            assertTrue(schema.getObjects().containsKey("animals"));
            assertEquals(AttributeType.STRING, schema.getObjects().get("animals").getAttributes().get("name").getAttributeType());
            assertEquals(1, engine.getRowCount("Animals"));
            assertEquals("Giraffe", engine.readString("Animals", "Name", 0));
        }
    }

    @Test
    void createChildObjectCopiesParentAttributesAndStoresParentLink() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db"))) {
            engine.createObject("Mammals", null);
            engine.createAttribute("Mammals", "Species", AttributeType.STRING);
            engine.createObject("Giraffes", "Mammals");

            DatabaseSchema schema = engine.readSchema();
            assertEquals("mammals", schema.getObjects().get("giraffes").getParentObjectName());
            assertTrue(schema.getObjects().get("giraffes").getAttributes().containsKey("species"));
            assertTrue(Files.exists(tempDir.resolve("db/metadata/objects/giraffes/species.txt")));
        }
    }

    @Test
    void namingIsSluggedAndCaseInsensitiveForUniquenessAndRename() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db"))) {
            engine.createObject("Zoo Animals", null);
            assertThrows(IOException.class, () -> engine.createObject("zoo_animals", null));

            engine.createAttribute("Zoo Animals", "Display Name", AttributeType.STRING);
            assertThrows(IOException.class, () -> engine.createAttribute("zoo_animals", "display_name", AttributeType.STRING));

            engine.renameObject("Zoo Animals", "Big Cats");
            engine.renameAttribute("Big Cats", "Display Name", "Label");

            assertTrue(Files.isDirectory(tempDir.resolve("db/metadata/objects/big_cats")));
            assertTrue(Files.exists(tempDir.resolve("db/metadata/objects/big_cats/label.txt")));
            assertFalse(Files.exists(tempDir.resolve("db/metadata/objects/zoo_animals")));
        }
    }

    @Test
    void typedRoundTripsAndNullSentinelWorkForAllTypes() throws Exception {
        UUID uuid = UUID.randomUUID();

        try (CrudEngine engine = newEngine(tempDir.resolve("db"))) {
            engine.createObject("People", null);
            engine.createAttribute("People", "Age", AttributeType.INT);
            engine.createAttribute("People", "Name", AttributeType.STRING);
            engine.createAttribute("People", "Active", AttributeType.BOOL);
            engine.createAttribute("People", "Person Id", AttributeType.ID);

            int rowIndex = engine.insertRow("People");
            engine.writeInt("People", "Age", rowIndex, 42);
            engine.writeString("People", "Name", rowIndex, "Kai");
            engine.writeBool("People", "Active", rowIndex, true);
            engine.writeId("People", "Person Id", rowIndex, uuid);

            assertEquals(42, engine.readInt("People", "Age", rowIndex));
            assertEquals("Kai", engine.readString("People", "Name", rowIndex));
            assertEquals(true, engine.readBool("People", "Active", rowIndex));
            assertEquals(uuid, engine.readId("People", "Person Id", rowIndex));

            engine.writeString("People", "Name", rowIndex, null);
            assertNull(engine.readString("People", "Name", rowIndex));
            assertEquals(List.of("-1"), Files.readAllLines(
                tempDir.resolve("db/metadata/objects/people/name.txt"),
                StandardCharsets.UTF_8));
        }
    }

    @Test
    void addAttributeBackfillsNullsAndDeleteRowCompactsIndexes() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db"))) {
            engine.createObject("Pets", null);
            engine.createAttribute("Pets", "Name", AttributeType.STRING);

            int firstRow = engine.insertRow("Pets");
            int secondRow = engine.insertRow("Pets");
            engine.writeString("Pets", "Name", firstRow, "Dog");
            engine.writeString("Pets", "Name", secondRow, "Cat");

            engine.createAttribute("Pets", "Age", AttributeType.INT);
            assertEquals(List.of("-1", "-1"), Files.readAllLines(
                tempDir.resolve("db/metadata/objects/pets/age.txt"),
                StandardCharsets.UTF_8));

            engine.writeInt("Pets", "Age", firstRow, 4);
            engine.writeInt("Pets", "Age", secondRow, 7);
            engine.deleteRow("Pets", 0);

            assertEquals(1, engine.getRowCount("Pets"));
            assertEquals("Cat", engine.readString("Pets", "Name", 0));
            assertEquals(7, engine.readInt("Pets", "Age", 0));
            assertEquals(1, Files.readAllLines(
                tempDir.resolve("db/metadata/objects/pets/name.txt"),
                StandardCharsets.UTF_8).size());
        }
    }

    @Test
    void initializationFailsOnInconsistentAttributeRowCounts() throws Exception {
        Path dbRoot = tempDir.resolve("db");

        try (CrudEngine engine = newEngine(dbRoot)) {
            engine.createObject("Pets", null);
            engine.createAttribute("Pets", "Name", AttributeType.STRING);
            engine.createAttribute("Pets", "Age", AttributeType.INT);
            engine.insertRow("Pets");
        }

        Files.write(
            dbRoot.resolve("metadata/objects/pets/age.txt"),
            List.of("-1", "-1"),
            StandardCharsets.UTF_8);

        CrudEngine engine = new CrudEngine(dbRoot);
        IOException exception = assertThrows(IOException.class, engine::initialize);
        assertNotNull(exception.getMessage());
    }

    @Test
    void insertRowWithoutAttributesThrows() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db"))) {
            engine.createObject("Empty", null);

            IOException exception = assertThrows(IOException.class, () -> engine.insertRow("Empty"));
            assertTrue(exception.getMessage().contains("no attributes"));
        }
    }

    @Test
    void deleteObjectWithChildThrowsAndKeepsObject() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db"))) {
            engine.createObject("Mammals", null);
            engine.createObject("Giraffes", "Mammals");

            IOException exception = assertThrows(IOException.class, () -> engine.deleteObject("Mammals"));
            assertTrue(exception.getMessage().contains("child objects"));
            assertTrue(engine.readSchema().getObjects().containsKey("mammals"));
        }
    }

    @Test
    void deleteAttributeRemovesFileSchemaAndAllocatorEntry() throws Exception {
        Path dbRoot = tempDir.resolve("db");

        try (CrudEngine engine = newEngine(dbRoot)) {
            engine.createObject("People", null);
            engine.createAttribute("People", "Name", AttributeType.STRING);
            int rowIndex = engine.insertRow("People");
            engine.writeString("People", "Name", rowIndex, "Kai");

            long address = Long.parseLong(Files.readString(
                dbRoot.resolve("metadata/objects/people/name.txt"),
                StandardCharsets.UTF_8).trim());

            engine.deleteAttribute("People", "Name");

            assertFalse(Files.exists(dbRoot.resolve("metadata/objects/people/name.txt")));
            assertFalse(engine.readSchema().getObjects().get("people").getAttributes().containsKey("name"));

            try (BitmapMemoryAllocator allocator = newAllocator(dbRoot.resolve("data"))) {
                assertFalse(allocator.isAllocated(address));
            }
        }
    }

    @Test
    void typeMismatchAndRowBoundsThrow() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db"))) {
            engine.createObject("People", null);
            engine.createAttribute("People", "Age", AttributeType.INT);
            int rowIndex = engine.insertRow("People");
            engine.writeInt("People", "Age", rowIndex, 42);

            IOException typeException =
                assertThrows(IOException.class, () -> engine.writeString("People", "Age", rowIndex, "forty-two"));
            assertTrue(typeException.getMessage().contains("not STRING"));

            IOException rowException =
                assertThrows(IOException.class, () -> engine.readInt("People", "Age", rowIndex + 1));
            assertTrue(rowException.getMessage().contains("Row index out of bounds"));
        }
    }

    @Test
    void renameObjectUpdatesChildParentReference() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db"))) {
            engine.createObject("Animals", null);
            engine.createObject("Birds", "Animals");

            engine.renameObject("Animals", "Creatures");

            DatabaseSchema schema = engine.readSchema();
            assertEquals("creatures", schema.getObjects().get("birds").getParentObjectName());
            assertTrue(schema.getObjects().containsKey("creatures"));
            assertFalse(schema.getObjects().containsKey("animals"));
        }
    }

    @Test
    void removeParentDecouplesChildSoFormerParentCanBeDeleted() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db"))) {
            engine.createObject("Mammals", null);
            engine.createAttribute("Mammals", "Species", AttributeType.STRING);
            engine.createObject("Giraffes", "Mammals");

            engine.removeParent("Giraffes");
            assertNull(engine.readSchema().getObjects().get("giraffes").getParentObjectName());

            engine.deleteObject("Mammals");
            assertFalse(engine.readSchema().getObjects().containsKey("mammals"));
            assertTrue(engine.readSchema().getObjects().containsKey("giraffes"));
        }
    }

    @Test
    void addParentReattachesWhenChildAlreadyHasParentAttributes() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db"))) {
            engine.createObject("Mammals", null);
            engine.createAttribute("Mammals", "Species", AttributeType.STRING);
            engine.createAttribute("Mammals", "Has Fur", AttributeType.BOOL);
            engine.createObject("Giraffes", "Mammals");

            engine.removeParent("Giraffes");
            engine.addParent("Giraffes", "Mammals");

            assertEquals("mammals", engine.readSchema().getObjects().get("giraffes").getParentObjectName());
        }
    }

    @Test
    void addParentFailsWhenChildDoesNotAlreadyContainParentAttributes() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db"))) {
            engine.createObject("Mammals", null);
            engine.createAttribute("Mammals", "Species", AttributeType.STRING);
            engine.createObject("Birds", null);

            IOException exception = assertThrows(IOException.class, () -> engine.addParent("Birds", "Mammals"));
            assertTrue(exception.getMessage().contains("missing parent attribute"));
        }
    }

    @Test
    void addParentFailsWhenObjectAlreadyHasParent() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db"))) {
            engine.createObject("Animals", null);
            engine.createAttribute("Animals", "Name", AttributeType.STRING);
            engine.createObject("Mammals", "Animals");
            engine.createObject("Giraffes", "Mammals");

            IOException exception = assertThrows(IOException.class, () -> engine.addParent("Giraffes", "Animals"));
            assertTrue(exception.getMessage().contains("already has a parent"));
        }
    }

    @Test
    void addParentFailsWhenItWouldCreateCycle() throws Exception {
        try (CrudEngine engine = newEngine(tempDir.resolve("db"))) {
            engine.createObject("Animals", null);
            engine.createAttribute("Animals", "Name", AttributeType.STRING);
            engine.createObject("Mammals", "Animals");
            engine.createObject("Giraffes", "Mammals");

            IOException exception = assertThrows(IOException.class, () -> engine.addParent("Animals", "Giraffes"));
            assertTrue(exception.getMessage().contains("cycle"));
        }
    }

    @Test
    void initializeFailsWhenAttributeFileIsMissing() throws Exception {
        Path dbRoot = tempDir.resolve("db");

        try (CrudEngine engine = newEngine(dbRoot)) {
            engine.createObject("Pets", null);
            engine.createAttribute("Pets", "Name", AttributeType.STRING);
        }

        Files.delete(dbRoot.resolve("metadata/objects/pets/name.txt"));

        CrudEngine engine = new CrudEngine(dbRoot);
        IOException exception = assertThrows(IOException.class, engine::initialize);
        assertTrue(exception.getMessage().contains("Missing attribute file"));
    }

    @Test
    void initializeFailsWhenSchemaVersionIsInvalid() throws Exception {
        Path dbRoot = tempDir.resolve("db");

        try (CrudEngine engine = newEngine(dbRoot)) {
            engine.createObject("Pets", null);
        }

        DatabaseSchema schema = readSchemaFile(dbRoot);
        schema.setSchemaVersion(-1);
        writeSchemaFile(dbRoot, schema);

        CrudEngine engine = new CrudEngine(dbRoot);
        IOException exception = assertThrows(IOException.class, engine::initialize);
        assertTrue(exception.getMessage().contains("Schema version must be positive"));
    }

    @Test
    void initializeFailsWhenSchemaReferencesMissingParentObject() throws Exception {
        Path dbRoot = tempDir.resolve("db");

        try (CrudEngine engine = newEngine(dbRoot)) {
            engine.createObject("Animals", null);
            engine.createAttribute("Animals", "Name", AttributeType.STRING);
            engine.createObject("Birds", "Animals");
        }

        DatabaseSchema schema = readSchemaFile(dbRoot);
        schema.getObjects().get("birds").setParentObjectName("ghost_parent");
        writeSchemaFile(dbRoot, schema);

        CrudEngine engine = new CrudEngine(dbRoot);
        IOException exception = assertThrows(IOException.class, engine::initialize);
        assertTrue(exception.getMessage().contains("Missing parent object"));
    }

    @Test
    void initializeFailsWhenObjectDirectoryIsMissing() throws Exception {
        Path dbRoot = tempDir.resolve("db");

        try (CrudEngine engine = newEngine(dbRoot)) {
            engine.createObject("Pets", null);
        }

        Files.delete(dbRoot.resolve("metadata/objects/pets"));

        CrudEngine engine = new CrudEngine(dbRoot);
        IOException exception = assertThrows(IOException.class, engine::initialize);
        assertTrue(exception.getMessage().contains("Missing object directory"));
    }

    @Test
    void initializeFailsWhenSchemaObjectNameDoesNotMatchKey() throws Exception {
        Path dbRoot = tempDir.resolve("db");

        try (CrudEngine engine = newEngine(dbRoot)) {
            engine.createObject("Pets", null);
        }

        DatabaseSchema schema = readSchemaFile(dbRoot);
        schema.getObjects().get("pets").setObjectName("animals");
        writeSchemaFile(dbRoot, schema);

        CrudEngine engine = new CrudEngine(dbRoot);
        IOException exception = assertThrows(IOException.class, engine::initialize);
        assertTrue(exception.getMessage().contains("Schema object name mismatch"));
    }

    @Test
    void initializeFailsWhenSchemaAttributeNameDoesNotMatchKey() throws Exception {
        Path dbRoot = tempDir.resolve("db");

        try (CrudEngine engine = newEngine(dbRoot)) {
            engine.createObject("Pets", null);
            engine.createAttribute("Pets", "Name", AttributeType.STRING);
        }

        DatabaseSchema schema = readSchemaFile(dbRoot);
        schema.getObjects().get("pets").getAttributes().get("name").setAttributeName("label");
        writeSchemaFile(dbRoot, schema);

        CrudEngine engine = new CrudEngine(dbRoot);
        IOException exception = assertThrows(IOException.class, engine::initialize);
        assertTrue(exception.getMessage().contains("Schema attribute name mismatch"));
    }

    @Test
    void initializeFailsWhenAttributeFileContainsInvalidAddressText() throws Exception {
        Path dbRoot = tempDir.resolve("db");

        try (CrudEngine engine = newEngine(dbRoot)) {
            engine.createObject("Pets", null);
            engine.createAttribute("Pets", "Name", AttributeType.STRING);
            engine.insertRow("Pets");
        }

        Files.write(
            dbRoot.resolve("metadata/objects/pets/name.txt"),
            List.of("not_a_number"),
            StandardCharsets.UTF_8);

        CrudEngine engine = new CrudEngine(dbRoot);
        IOException exception = assertThrows(IOException.class, engine::initialize);
        assertTrue(exception.getMessage().contains("Invalid address line"));
    }

    @Test
    void initializeFailsWhenAttributeFileContainsBlankAddressLine() throws Exception {
        Path dbRoot = tempDir.resolve("db");

        try (CrudEngine engine = newEngine(dbRoot)) {
            engine.createObject("Pets", null);
            engine.createAttribute("Pets", "Name", AttributeType.STRING);
            engine.insertRow("Pets");
        }

        Files.writeString(
            dbRoot.resolve("metadata/objects/pets/name.txt"),
            "\n",
            StandardCharsets.UTF_8);

        CrudEngine engine = new CrudEngine(dbRoot);
        IOException exception = assertThrows(IOException.class, engine::initialize);
        assertTrue(exception.getMessage().contains("Blank address line"));
    }

    @Test
    void closePreventsFurtherOperations() throws Exception {
        CrudEngine engine = newEngine(tempDir.resolve("db"));
        engine.close();

        IllegalStateException exception =
            assertThrows(IllegalStateException.class, () -> engine.createObject("People", null));
        assertInstanceOf(IllegalStateException.class, exception);
    }

    private CrudEngine newEngine(Path dbRoot) throws IOException {
        CrudEngine engine = new CrudEngine(dbRoot);
        engine.initialize();
        return engine;
    }

    private BitmapMemoryAllocator newAllocator(Path dataRoot) throws IOException {
        BitmapMemoryAllocator allocator = new BitmapMemoryAllocator(dataRoot);
        allocator.initialize();
        return allocator;
    }

    private DatabaseSchema readSchemaFile(Path dbRoot) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        return objectMapper.readValue(dbRoot.resolve("metadata/schema.json").toFile(), DatabaseSchema.class);
    }

    private void writeSchemaFile(Path dbRoot, DatabaseSchema schema) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        objectMapper.writeValue(dbRoot.resolve("metadata/schema.json").toFile(), schema);
    }
}
