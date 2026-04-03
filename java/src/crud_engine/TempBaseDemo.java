package crud_engine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public class TempBaseDemo {
    private static final Path TEMP_BASE_PATH = Path.of("TEMP_BASE");

    public static void main(String[] args) throws Exception {
        if (Files.exists(TEMP_BASE_PATH)) {
            throw new IOException("TEMP_BASE already exists. Delete it first if you want to regenerate it.");
        }

        try (CrudEngine engine = new CrudEngine(TEMP_BASE_PATH)) {
            engine.initialize();

            createAnimals(engine);
            createMammalsAndGiraffes(engine);
            createPlants(engine);
        }

        System.out.println("Created sample database at " + TEMP_BASE_PATH.toAbsolutePath());
    }

    private static void createAnimals(CrudEngine engine) throws IOException {
        engine.createObject("Animals", null);
        engine.createAttribute("Animals", "Animal Id", CrudEngineInterface.AttributeType.ID);
        engine.createAttribute("Animals", "Name", CrudEngineInterface.AttributeType.STRING);
        engine.createAttribute("Animals", "Age", CrudEngineInterface.AttributeType.INT);
        engine.createAttribute("Animals", "Is Wild", CrudEngineInterface.AttributeType.BOOL);

        int lionRow = engine.insertRow("Animals");
        engine.writeId("Animals", "Animal Id", lionRow, UUID.fromString("11111111-1111-1111-1111-111111111111"));
        engine.writeString("Animals", "Name", lionRow, "lion");
        engine.writeInt("Animals", "Age", lionRow, 8);
        engine.writeBool("Animals", "Is Wild", lionRow, true);

        int dogRow = engine.insertRow("Animals");
        engine.writeId("Animals", "Animal Id", dogRow, UUID.fromString("22222222-2222-2222-2222-222222222222"));
        engine.writeString("Animals", "Name", dogRow, "dog");
        engine.writeInt("Animals", "Age", dogRow, 4);
        engine.writeBool("Animals", "Is Wild", dogRow, false);
    }

    private static void createMammalsAndGiraffes(CrudEngine engine) throws IOException {
        engine.createObject("Mammals", null);
        engine.createAttribute("Mammals", "Mammal Id", CrudEngineInterface.AttributeType.ID);
        engine.createAttribute("Mammals", "Species", CrudEngineInterface.AttributeType.STRING);
        engine.createAttribute("Mammals", "Has Fur", CrudEngineInterface.AttributeType.BOOL);

        int whaleRow = engine.insertRow("Mammals");
        engine.writeId("Mammals", "Mammal Id", whaleRow, UUID.fromString("33333333-3333-3333-3333-333333333333"));
        engine.writeString("Mammals", "Species", whaleRow, "blue whale");
        engine.writeBool("Mammals", "Has Fur", whaleRow, false);

        engine.createObject("Giraffes", "Mammals");
        engine.createAttribute("Giraffes", "Neck Length Cm", CrudEngineInterface.AttributeType.INT);
        engine.createAttribute("Giraffes", "Favorite Tree", CrudEngineInterface.AttributeType.STRING);

        int giraffeRow = engine.insertRow("Giraffes");
        engine.writeId("Giraffes", "Mammal Id", giraffeRow, UUID.fromString("44444444-4444-4444-4444-444444444444"));
        engine.writeString("Giraffes", "Species", giraffeRow, "masai giraffe");
        engine.writeBool("Giraffes", "Has Fur", giraffeRow, true);
        engine.writeInt("Giraffes", "Neck Length Cm", giraffeRow, 205);
        engine.writeString("Giraffes", "Favorite Tree", giraffeRow, "acacia");
    }

    private static void createPlants(CrudEngine engine) throws IOException {
        engine.createObject("Plants", null);
        engine.createAttribute("Plants", "Plant Id", CrudEngineInterface.AttributeType.ID);
        engine.createAttribute("Plants", "Common Name", CrudEngineInterface.AttributeType.STRING);
        engine.createAttribute("Plants", "Height Cm", CrudEngineInterface.AttributeType.INT);
        engine.createAttribute("Plants", "Is Perennial", CrudEngineInterface.AttributeType.BOOL);

        int sunflowerRow = engine.insertRow("Plants");
        engine.writeId("Plants", "Plant Id", sunflowerRow, UUID.fromString("55555555-5555-5555-5555-555555555555"));
        engine.writeString("Plants", "Common Name", sunflowerRow, "sunflower");
        engine.writeInt("Plants", "Height Cm", sunflowerRow, 180);
        engine.writeBool("Plants", "Is Perennial", sunflowerRow, false);

        int oakRow = engine.insertRow("Plants");
        engine.writeId("Plants", "Plant Id", oakRow, UUID.fromString("66666666-6666-6666-6666-666666666666"));
        engine.writeString("Plants", "Common Name", oakRow, "oak");
        engine.writeInt("Plants", "Height Cm", oakRow, null);
        engine.writeBool("Plants", "Is Perennial", oakRow, true);
    }
}
