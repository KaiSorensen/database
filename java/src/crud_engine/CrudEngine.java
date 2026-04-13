package crud_engine;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import memory_allocator.BitmapMemoryAllocator;

/**
 * This class wraps around the memory allocator, providing the outer layer of database file interaction. It contains a BitmapMemoryAllocator as an instance variable.
 * It manages the schema, object, and attribute files. Through these files, it organizes all database data in a queryable fashion.
 * 
 * @author Written by Codex, designed and documented by Kai
 */

public class CrudEngine implements CrudEngineInterface {
    private static final long NULL_ADDRESS = -1L;
    private static final int SCHEMA_VERSION = 1;
    private static final String DEFAULT_DATABASE_FOLDER = "java/DATABASE";
    private static final String METADATA_DIRECTORY = "metadata";
    private static final String OBJECTS_DIRECTORY = "objects";
    private static final String DATA_DIRECTORY = "data";
    private static final String SCHEMA_FILE = "schema.json";
    private static final String ATTRIBUTE_FILE_EXTENSION = ".txt";

    private final Path databaseRoot;
    private final Path metadataPath;
    private final Path objectsRootPath;
    private final Path schemaPath;
    private final BitmapMemoryAllocator allocator;
    private final ObjectMapper objectMapper;
    private boolean closed;

    public CrudEngine() {
        this(Path.of(DEFAULT_DATABASE_FOLDER));
    }

    public CrudEngine(Path databaseRoot) {
        this.databaseRoot = databaseRoot;
        this.metadataPath = databaseRoot.resolve(METADATA_DIRECTORY);
        this.objectsRootPath = metadataPath.resolve(OBJECTS_DIRECTORY);
        this.schemaPath = metadataPath.resolve(SCHEMA_FILE);
        this.allocator = new BitmapMemoryAllocator(databaseRoot.resolve(DATA_DIRECTORY));
        this.objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        this.closed = false;
    }

    public void initialize() throws IOException {
        // opened by the constructor before this is called
        ensureOpen();

        // ensure that the database folders exist
        Files.createDirectories(databaseRoot);
        Files.createDirectories(metadataPath);
        Files.createDirectories(objectsRootPath);
        allocator.initialize();

        // if this is the first time, create a new database schema
        if (!Files.exists(schemaPath)) {
            writeSchemaInternal(new DatabaseSchema(SCHEMA_VERSION, new LinkedHashMap<>()));
        }

        // make sure the file structure matches what we have in the object
        validateSchemaState(loadSchemaInternal());
    }

    @Override
    public void createObject(String objectName, String parentObjectNameNullable) throws IOException {
        ensureOpen();

        // get/initialize the schema
        DatabaseSchema schema = loadSchemaInternal();
        String objectSlug = normalizeName(objectName);
        // if there's already an object of this name
        if (schema.getObjects().containsKey(objectSlug)) {
            throw new IOException("Object already exists: " + objectName);
        }
        // if this is a valid new object name, proceed to create it
        ObjectSchema objectSchema = new ObjectSchema();
        objectSchema.setObjectName(objectSlug);

        // get initial attributes (empty if no parent)
        if (parentObjectNameNullable != null) {
            // if it has a parent obejct, get the parent's attributes
            String parentSlug = normalizeName(parentObjectNameNullable);
            ObjectSchema parentSchema = requireObject(schema, parentSlug);
            objectSchema.setParentObjectName(parentSlug);
            objectSchema.setAttributes(copyAttributes(parentSchema.getAttributes()));
        } else {
            //if it has no parent, make a new space for attributes
            objectSchema.setParentObjectName(null);
            objectSchema.setAttributes(new LinkedHashMap<>());
        }

        // make the object's folder for attributes
        Files.createDirectories(getObjectDirectory(objectSlug));
        for (String attributeSlug : objectSchema.getAttributes().keySet()) {
            writeAttributeAddresses(getAttributeFilePath(objectSlug, attributeSlug), List.of());
        }

        // officially add the object to the schema
        schema.getObjects().put(objectSlug, objectSchema);
        writeSchemaInternal(schema);
    }

    @Override
    public void deleteObject(String objectName) throws IOException {
        ensureOpen();

        // load in the schema
        DatabaseSchema schema = loadSchemaInternal();
        String objectSlug = normalizeName(objectName);
        requireObject(schema, objectSlug);

        // check if there are children of this object, which can't exist without their parent
        for (ObjectSchema schemaEntry : schema.getObjects().values()) {
            if (objectSlug.equals(schemaEntry.getParentObjectName())) {
                throw new IOException("Cannot delete object with child objects: " + objectName);
            }
        }

        for (String attributeSlug : new ArrayList<>(requireObject(schema, objectSlug).getAttributes().keySet())) {
            deleteAttributeInternal(schema, objectSlug, attributeSlug);
        }

        deleteDirectory(getObjectDirectory(objectSlug));
        schema.getObjects().remove(objectSlug);
        writeSchemaInternal(schema);
        allocator.flush();
    }

    @Override
    public void renameObject(String oldObjectName, String newObjectName) throws IOException {
        ensureOpen();

        DatabaseSchema schema = loadSchemaInternal();
        String oldSlug = normalizeName(oldObjectName);
        String newSlug = normalizeName(newObjectName);
        ObjectSchema objectSchema = requireObject(schema, oldSlug);

        if (!oldSlug.equals(newSlug) && schema.getObjects().containsKey(newSlug)) {
            throw new IOException("Object already exists: " + newObjectName);
        }

        Files.move(getObjectDirectory(oldSlug), getObjectDirectory(newSlug), StandardCopyOption.ATOMIC_MOVE);

        objectSchema.setObjectName(newSlug);
        schema.getObjects().remove(oldSlug);
        schema.getObjects().put(newSlug, objectSchema);

        for (ObjectSchema childSchema : schema.getObjects().values()) {
            if (oldSlug.equals(childSchema.getParentObjectName())) {
                childSchema.setParentObjectName(newSlug);
            }
        }

        writeSchemaInternal(schema);
    }

    @Override
    public void removeParent(String objectName) throws IOException {
        ensureOpen();

        DatabaseSchema schema = loadSchemaInternal();
        ObjectSchema objectSchema = requireObject(schema, normalizeName(objectName));
        if (objectSchema.getParentObjectName() == null) {
            throw new IOException("Object does not currently have a parent: " + objectName);
        }

        objectSchema.setParentObjectName(null);
        writeSchemaInternal(schema);
    }

    @Override
    public void addParent(String objectName, String parentObjectName) throws IOException {
        ensureOpen();

        DatabaseSchema schema = loadSchemaInternal();
        String objectSlug = normalizeName(objectName);
        String parentSlug = normalizeName(parentObjectName);
        ObjectSchema objectSchema = requireObject(schema, objectSlug);
        ObjectSchema parentSchema = requireObject(schema, parentSlug);

        if (objectSlug.equals(parentSlug)) {
            throw new IOException("Object cannot be its own parent: " + objectName);
        }
        if (objectSchema.getParentObjectName() != null) {
            throw new IOException("Object already has a parent: " + objectName);
        }
        if (wouldCreateParentCycle(schema, objectSlug, parentSlug)) {
            throw new IOException("Adding parent would create an inheritance cycle");
        }

        requireAllParentAttributesPresent(objectSchema, parentSchema);
        objectSchema.setParentObjectName(parentSlug);
        writeSchemaInternal(schema);
    }

    @Override
    public void createAttribute(String objectName, String attributeName, AttributeType attributeType)
            throws IOException {
        ensureOpen();

        DatabaseSchema schema = loadSchemaInternal();
        String objectSlug = normalizeName(objectName);
        String attributeSlug = normalizeName(attributeName);
        ObjectSchema objectSchema = requireObject(schema, objectSlug);

        if (objectSchema.getAttributes().containsKey(attributeSlug)) {
            throw new IOException("Attribute already exists: " + attributeName);
        }

        int rowCount = getRowCountInternal(objectSchema);
        objectSchema.getAttributes().put(attributeSlug, new AttributeSchema(attributeSlug, attributeType));
        writeAttributeAddresses(getAttributeFilePath(objectSlug, attributeSlug), createNullAddresses(rowCount));
        writeSchemaInternal(schema);
    }

    @Override
    public void deleteAttribute(String objectName, String attributeName) throws IOException {
        ensureOpen();

        DatabaseSchema schema = loadSchemaInternal();
        String objectSlug = normalizeName(objectName);
        deleteAttributeInternal(schema, objectSlug, normalizeName(attributeName));
        writeSchemaInternal(schema);
        allocator.flush();
    }

    @Override
    public void renameAttribute(String objectName, String oldAttributeName, String newAttributeName)
            throws IOException {
        ensureOpen();

        DatabaseSchema schema = loadSchemaInternal();
        String objectSlug = normalizeName(objectName);
        String oldSlug = normalizeName(oldAttributeName);
        String newSlug = normalizeName(newAttributeName);
        ObjectSchema objectSchema = requireObject(schema, objectSlug);
        AttributeSchema attributeSchema = requireAttribute(objectSchema, oldSlug);

        if (!oldSlug.equals(newSlug) && objectSchema.getAttributes().containsKey(newSlug)) {
            throw new IOException("Attribute already exists: " + newAttributeName);
        }

        Files.move(
            getAttributeFilePath(objectSlug, oldSlug),
            getAttributeFilePath(objectSlug, newSlug),
            StandardCopyOption.ATOMIC_MOVE);

        objectSchema.getAttributes().remove(oldSlug);
        attributeSchema.setAttributeName(newSlug);
        objectSchema.getAttributes().put(newSlug, attributeSchema);
        writeSchemaInternal(schema);
    }

    @Override
    public DatabaseSchema readSchema() throws IOException {
        ensureOpen();
        return loadSchemaInternal();
    }

    /**
     * Creates placeholder null addresses under each attribute that will be modified before execution ends.
     * Since this is the class which handles type conversions, it offers a method for reading/writing each type.
     * Therefore, the read/write methods for each type need to take object/attribute pair as a parameter.
     * They could just assume the row count, but we have a function to be explicit about it because it modularizes creation versus updating. Also, it ensures that attribute row counts are equal.
     * We could have abstracted away row indexing into this class, but it's the query executor layer that is doing all the discrimination anyway, and the row indexing logic counts as row discrimination.
     * @return the row count
     */
    @Override
    public int insertRow(String objectName) throws IOException {
        ensureOpen();

        DatabaseSchema schema = loadSchemaInternal();
        ObjectSchema objectSchema = requireObject(schema, normalizeName(objectName));
        if (objectSchema.getAttributes().isEmpty()) {
            throw new IOException("Cannot insert row into object with no attributes");
        }

        int rowIndex = getRowCountInternal(objectSchema);
        for (String attributeSlug : objectSchema.getAttributes().keySet()) {
            Path attributeFilePath = getAttributeFilePath(objectSchema.getObjectName(), attributeSlug);
            List<Long> addresses = readAttributeAddresses(attributeFilePath);
            addresses.add(NULL_ADDRESS);
            writeAttributeAddresses(attributeFilePath, addresses);
        }

        return rowIndex;
    }

    @Override
    public int getRowCount(String objectName) throws IOException {
        ensureOpen();

        ObjectSchema objectSchema = requireObject(loadSchemaInternal(), normalizeName(objectName));
        return getRowCountInternal(objectSchema);
    }


    /*
     * The read/write functions all call readTypedValue/writeTypedValue respectively.
     * The readTypedValue/writeTypedValue functions which call the allocator. 
     * The type conversions for write's are handled within writeTypedValue.
     * The type conversions for read's are handled in the return statements of these function.
     * readTypedValue/writeTypedValue take the AttributeType as a parameter because they verify the type against the schema.
    */

    @Override
    public void writeInt(String objectName, String attributeName, int rowIndex, Integer value)
            throws IOException {
        writeTypedValue(objectName, attributeName, rowIndex, value, AttributeType.INT);
    }

    @Override
    public Integer readInt(String objectName, String attributeName, int rowIndex) throws IOException {
        byte[] bytes = readTypedValue(objectName, attributeName, rowIndex, AttributeType.INT);
        return bytes == null ? null : TypeByteConversions.bytesToInt(bytes);
    }

    @Override
    public void writeString(String objectName, String attributeName, int rowIndex, String value)
            throws IOException {
        writeTypedValue(objectName, attributeName, rowIndex, value, AttributeType.STRING);
    }

    @Override
    public String readString(String objectName, String attributeName, int rowIndex) throws IOException {
        byte[] bytes = readTypedValue(objectName, attributeName, rowIndex, AttributeType.STRING);
        return bytes == null ? null : TypeByteConversions.bytesToString(bytes);
    }

    @Override
    public void writeBool(String objectName, String attributeName, int rowIndex, Boolean value)
            throws IOException {
        writeTypedValue(objectName, attributeName, rowIndex, value, AttributeType.BOOL);
    }

    @Override
    public Boolean readBool(String objectName, String attributeName, int rowIndex) throws IOException {
        byte[] bytes = readTypedValue(objectName, attributeName, rowIndex, AttributeType.BOOL);
        return bytes == null ? null : TypeByteConversions.bytesToBool(bytes);
    }

    @Override
    public void writeId(String objectName, String attributeName, int rowIndex, UUID value)
            throws IOException {
        writeTypedValue(objectName, attributeName, rowIndex, value, AttributeType.ID);
    }

    @Override
    public UUID readId(String objectName, String attributeName, int rowIndex) throws IOException {
        byte[] bytes = readTypedValue(objectName, attributeName, rowIndex, AttributeType.ID);
        return bytes == null ? null : TypeByteConversions.bytesToUuid(bytes);
    }

    @Override
    public void deleteRow(String objectName, int rowIndex) throws IOException {
        ensureOpen();

        DatabaseSchema schema = loadSchemaInternal();
        ObjectSchema objectSchema = requireObject(schema, normalizeName(objectName));
        if (objectSchema.getAttributes().isEmpty()) {
            throw new IOException("Cannot delete row from object with no attributes");
        }

        for (String attributeSlug : objectSchema.getAttributes().keySet()) {
            Path attributeFilePath = getAttributeFilePath(objectSchema.getObjectName(), attributeSlug);
            List<Long> addresses = readAttributeAddresses(attributeFilePath);
            validateRowIndex(rowIndex, addresses.size());

            long address = addresses.remove(rowIndex);
            deleteAllocationIfPresent(address);
            writeAttributeAddresses(attributeFilePath, addresses);
        }

        allocator.flush();
    }

    @Override
    public String dumpDatabase() throws IOException {
        ensureOpen();

        DatabaseSchema schema = loadSchemaInternal();
        StringBuilder builder = new StringBuilder();
        builder.append("Database\n");
        if (schema.getObjects().isEmpty()) {
            builder.append("(empty)\n");
            return builder.toString();
        }

        boolean firstObject = true;
        for (ObjectSchema objectSchema : schema.getObjects().values()) {
            if (!firstObject) {
                builder.append('\n');
            }
            firstObject = false;

            String objectName = objectSchema.getObjectName();
            builder.append("OBJECT ").append(objectName);
            if (objectSchema.getParentObjectName() != null) {
                builder.append(" EXTENDS ").append(objectSchema.getParentObjectName());
            }
            builder.append('\n');

            builder.append("  ATTRIBUTES");
            if (objectSchema.getAttributes().isEmpty()) {
                builder.append(": (none)\n");
                builder.append("  ROWS (0)\n");
                continue;
            }
            builder.append(":\n");
            for (AttributeSchema attributeSchema : objectSchema.getAttributes().values()) {
                builder.append("    - ")
                    .append(attributeSchema.getAttributeName())
                    .append(": ")
                    .append(attributeSchema.getAttributeType())
                    .append('\n');
            }

            int rowCount = getRowCountInternal(objectSchema);
            builder.append("  ROWS (").append(rowCount).append(")\n");
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                builder.append("    [").append(rowIndex).append("] ");
                boolean firstAttribute = true;
                for (AttributeSchema attributeSchema : objectSchema.getAttributes().values()) {
                    if (!firstAttribute) {
                        builder.append(", ");
                    }
                    firstAttribute = false;
                    builder.append(attributeSchema.getAttributeName())
                        .append('=')
                        .append(renderValue(objectName, attributeSchema, rowIndex));
                }
                builder.append('\n');
            }
        }
        return builder.toString();
    }

    @Override
    public String toString() {
        try {
            return dumpDatabase();
        } catch (Exception exception) {
            return "CrudEngine(dump failed: " + exception.getMessage() + ")";
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        closed = true;
        allocator.close();
    }

    /**
     * Deletes one attribute from the object's schema and removes every allocator payload referenced
     * by that attribute file before deleting the file itself.
     */
    private void deleteAttributeInternal(DatabaseSchema schema, String objectSlug, String attributeSlug)
            throws IOException {
        ObjectSchema objectSchema = requireObject(schema, objectSlug);
        requireAttribute(objectSchema, attributeSlug);

        Path attributeFilePath = getAttributeFilePath(objectSlug, attributeSlug);
        for (long address : readAttributeAddresses(attributeFilePath)) {
            deleteAllocationIfPresent(address);
        }

        Files.deleteIfExists(attributeFilePath);
        objectSchema.getAttributes().remove(attributeSlug);
    }

    /**
     * Validates the object, attribute, row, and declared type, then writes a typed value into the
     * attribute file and allocator. A {@code null} value clears the current allocation and stores
     * the null sentinel instead.
     */
    private void writeTypedValue(
            String objectName,
            String attributeName,
            int rowIndex,
            Object value,
            AttributeType expectedType) throws IOException {
        ensureOpen();

        DatabaseSchema schema = loadSchemaInternal();
        String objectSlug = normalizeName(objectName);
        String attributeSlug = normalizeName(attributeName);
        ObjectSchema objectSchema = requireObject(schema, objectSlug);
        AttributeSchema attributeSchema = requireAttribute(objectSchema, attributeSlug);

        // ensure that the attribute's type matches what's specified in the schema
        requireType(attributeSchema, expectedType);

        // ensure that we're adding the value at the same index as any other values in the request
        Path attributeFilePath = getAttributeFilePath(objectSlug, attributeSlug);
        List<Long> addresses = readAttributeAddresses(attributeFilePath);
        validateRowIndex(rowIndex, addresses.size());


        long currentAddress = addresses.get(rowIndex);
        long nextAddress;
        if (value == null) {
            // if we're updating a previously non-null value to now be a null value 
            deleteAllocationIfPresent(currentAddress);
            nextAddress = NULL_ADDRESS;
        } else {
            byte[] payload = toBytes(expectedType, value); // where the type conversion occurs
            nextAddress = currentAddress == NULL_ADDRESS
                ? allocator.create(payload)
                : allocator.update(currentAddress, payload); // we have to check if it already exists because the update 
        }

        addresses.set(rowIndex, nextAddress);
        writeAttributeAddresses(attributeFilePath, addresses);
        allocator.flush();
    }

    /**
     * Validates the object, attribute, row, and declared type, then returns the stored raw payload
     * bytes for that cell. Returns {@code null} when the row stores the null sentinel.
     */
    private byte[] readTypedValue(
            String objectName,
            String attributeName,
            int rowIndex,
            AttributeType expectedType) throws IOException {
        ensureOpen();

        DatabaseSchema schema = loadSchemaInternal();
        String objectSlug = normalizeName(objectName);
        String attributeSlug = normalizeName(attributeName);
        ObjectSchema objectSchema = requireObject(schema, objectSlug);
        AttributeSchema attributeSchema = requireAttribute(objectSchema, attributeSlug);
        requireType(attributeSchema, expectedType);

        List<Long> addresses = readAttributeAddresses(getAttributeFilePath(objectSlug, attributeSlug));
        validateRowIndex(rowIndex, addresses.size());

        long address = addresses.get(rowIndex);
        if (address == NULL_ADDRESS) {
            return null;
        }
        return allocator.read(address);
    }

    /**
     * Converts a typed Java value into the byte representation expected by the allocator for the
     * given attribute type.
     */
    private byte[] toBytes(AttributeType attributeType, Object value) throws IOException {
        return switch (attributeType) {
            case INT -> TypeByteConversions.intToBytes((Integer) value);
            case STRING -> TypeByteConversions.stringToBytes((String) value);
            case BOOL -> TypeByteConversions.boolToBytes((Boolean) value);
            case ID -> TypeByteConversions.uuidToBytes((UUID) value);
        };
    }

    private String renderValue(String objectName, AttributeSchema attributeSchema, int rowIndex) throws IOException {
        return switch (attributeSchema.getAttributeType()) {
            case INT -> renderScalar(readInt(objectName, attributeSchema.getAttributeName(), rowIndex));
            case STRING -> renderScalar(readString(objectName, attributeSchema.getAttributeName(), rowIndex));
            case BOOL -> renderScalar(readBool(objectName, attributeSchema.getAttributeName(), rowIndex));
            case ID -> renderScalar(readId(objectName, attributeSchema.getAttributeName(), rowIndex));
        };
    }

    private String renderScalar(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof String stringValue) {
            return "\"" + stringValue.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
        }
        return String.valueOf(value);
    }

    /**
     * Ensures that the schema-declared type for an attribute matches the typed CRUD operation being
     * performed.
     */
    private void requireType(AttributeSchema attributeSchema, AttributeType expectedType) throws IOException {
        if (attributeSchema.getAttributeType() != expectedType) {
            throw new IOException(
                "Attribute "
                    + attributeSchema.getAttributeName()
                    + " has type "
                    + attributeSchema.getAttributeType()
                    + ", not "
                    + expectedType);
        }
    }

    /**
     * Loads the schema JSON from disk, normalizes any missing in-memory defaults left by
     * deserialization, and verifies that the schema still matches the object folders and attribute
     * files on disk.
     */
    private DatabaseSchema loadSchemaInternal() throws IOException {
        DatabaseSchema schema = objectMapper.readValue(schemaPath.toFile(), DatabaseSchema.class);
        
        // If the schema file omitted the objects map or Jackson deserialized it as null, restore
        // the empty map so the rest of the CRUD code can treat "no objects yet" as a normal state.
        if (schema.getObjects() == null) {
            schema.setObjects(new LinkedHashMap<>());
        }

        // The schema version tracks the CRUD layer's metadata/file-layout format. If an older or
        // partially written schema has no version yet, default it to the current format version.
        if (schema.getSchemaVersion() == 0) {
            schema.setSchemaVersion(SCHEMA_VERSION);
        }

        // Rebuild any missing nested defaults left by deserialization so later code can rely on
        // object names, attribute maps, and attribute names being populated in memory.
        for (Map.Entry<String, ObjectSchema> entry : schema.getObjects().entrySet()) {
            ObjectSchema objectSchema = entry.getValue();
            if (objectSchema.getObjectName() == null) {
                objectSchema.setObjectName(entry.getKey());
            }
            if (objectSchema.getAttributes() == null) {
                objectSchema.setAttributes(new LinkedHashMap<>());
            }
            for (Map.Entry<String, AttributeSchema> attributeEntry : objectSchema.getAttributes().entrySet()) {
                AttributeSchema attributeSchema = attributeEntry.getValue();
                if (attributeSchema.getAttributeName() == null) {
                    attributeSchema.setAttributeName(attributeEntry.getKey());
                }
            }
        }

        // ensures that the loaded schema matches what's on disk
        validateSchemaState(schema);
        return schema;
    }

    /**
     * Persists the full schema object to {@code metadata/schema.json}, creating the metadata
     * directory first if needed.
     */
    private void writeSchemaInternal(DatabaseSchema schema) throws IOException {
        // 
        Files.createDirectories(metadataPath);
        objectMapper.writeValue(schemaPath.toFile(), schema);
    }

    /**
     * Verifies that the schema is internally consistent and that every schema reference has a
     * matching object folder and attribute file on disk with aligned row counts.
     */
    private void validateSchemaState(DatabaseSchema schema) throws IOException {
        if (schema.getSchemaVersion() <= 0) {
            throw new IOException("Schema version must be positive");
        }

        for (Map.Entry<String, ObjectSchema> entry : schema.getObjects().entrySet()) {
            String objectSlug = entry.getKey();
            ObjectSchema objectSchema = entry.getValue();
            Path objectDirectory = getObjectDirectory(objectSlug);
            if (!Files.isDirectory(objectDirectory)) {
                throw new IOException("Missing object directory: " + objectSlug);
            }
            if (!Objects.equals(objectSlug, objectSchema.getObjectName())) {
                throw new IOException("Schema object name mismatch for: " + objectSlug);
            }
            if (objectSchema.getParentObjectName() != null
                    && !schema.getObjects().containsKey(objectSchema.getParentObjectName())) {
                throw new IOException("Missing parent object: " + objectSchema.getParentObjectName());
            }

            Integer rowCount = null;
            for (Map.Entry<String, AttributeSchema> attributeEntry : objectSchema.getAttributes().entrySet()) {
                String attributeSlug = attributeEntry.getKey();
                AttributeSchema attributeSchema = attributeEntry.getValue();
                if (!Objects.equals(attributeSlug, attributeSchema.getAttributeName())) {
                    throw new IOException(
                        "Schema attribute name mismatch for: " + objectSlug + "." + attributeSlug);
                }
                Path attributeFilePath = getAttributeFilePath(objectSlug, attributeSlug);
                if (!Files.exists(attributeFilePath)) {
                    throw new IOException("Missing attribute file: " + attributeFilePath);
                }

                int attributeRows = readAttributeAddresses(attributeFilePath).size();
                if (rowCount == null) {
                    rowCount = attributeRows;
                } else if (rowCount != attributeRows) {
                    throw new IOException("Inconsistent row counts for object: " + objectSlug);
                }
            }
        }
    }

    /**
     * Returns the schema entry for the requested object or throws when the object does not exist.
     */
    private ObjectSchema requireObject(DatabaseSchema schema, String objectSlug) throws IOException {
        ObjectSchema objectSchema = schema.getObjects().get(objectSlug);
        if (objectSchema == null) {
            throw new IOException("Unknown object: " + objectSlug);
        }
        return objectSchema;
    }

    /**
     * Returns the schema entry for the requested attribute within one object or throws when the
     * attribute does not exist.
     */
    private AttributeSchema requireAttribute(ObjectSchema objectSchema, String attributeSlug)
            throws IOException {
        AttributeSchema attributeSchema = objectSchema.getAttributes().get(attributeSlug);
        if (attributeSchema == null) {
            throw new IOException(
                "Unknown attribute: " + objectSchema.getObjectName() + "." + attributeSlug);
        }
        return attributeSchema;
    }

    /**
     * Derives the object's row count from its attribute files and confirms that every attribute file
     * has the same number of rows.
     */
    private int getRowCountInternal(ObjectSchema objectSchema) throws IOException {
        if (objectSchema.getAttributes().isEmpty()) {
            return 0;
        }

        int rowCount = -1;
        for (String attributeSlug : objectSchema.getAttributes().keySet()) {
            int size = readAttributeAddresses(getAttributeFilePath(objectSchema.getObjectName(), attributeSlug)).size();
            if (rowCount == -1) {
                rowCount = size;
            } else if (rowCount != size) {
                throw new IOException("Inconsistent row counts for object: " + objectSchema.getObjectName());
            }
        }
        return rowCount;
    }

    /**
     * Builds an in-memory list filled with the null sentinel so a new attribute file can be
     * backfilled for existing rows.
     */
    private List<Long> createNullAddresses(int rowCount) {
        List<Long> addresses = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            addresses.add(NULL_ADDRESS);
        }
        return addresses;
    }

    /**
     * Ensures that a requested row index points to an existing row in the current attribute file.
     */
    private void validateRowIndex(int rowIndex, int rowCount) throws IOException {
        if (rowIndex < 0 || rowIndex >= rowCount) {
            throw new IOException("Row index out of bounds: " + rowIndex);
        }
    }

    /**
     * Reads one attribute file and parses each line as a decimal allocator address, preserving row
     * order exactly as stored on disk.
     */
    private List<Long> readAttributeAddresses(Path attributeFilePath) throws IOException {
        if (!Files.exists(attributeFilePath)) {
            throw new IOException("Missing attribute file: " + attributeFilePath);
        }

        List<String> lines = Files.readAllLines(attributeFilePath, StandardCharsets.UTF_8);
        List<Long> addresses = new ArrayList<>(lines.size());
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                throw new IOException("Blank address line in attribute file: " + attributeFilePath);
            }
            try {
                addresses.add(Long.parseLong(trimmed));
            } catch (NumberFormatException exception) {
                throw new IOException("Invalid address line in attribute file: " + attributeFilePath, exception);
            }
        }
        return addresses;
    }

    /**
     * Rewrites one attribute file from an in-memory list of addresses, storing one decimal address
     * per line in row order.
     */
    private void writeAttributeAddresses(Path attributeFilePath, List<Long> addresses) throws IOException {
        List<String> lines = new ArrayList<>(addresses.size());
        for (long address : addresses) {
            lines.add(Long.toString(address));
        }

        Files.createDirectories(attributeFilePath.getParent());
        Files.write(attributeFilePath, lines, StandardCharsets.UTF_8);
    }

    /**
     * Deletes an allocator payload only when the stored address is not the null sentinel.
     */
    private void deleteAllocationIfPresent(long address) throws IOException {
        if (address != NULL_ADDRESS) {
            allocator.delete(address);
        }
    }

    /**
     * Copies attribute metadata into a new linked map so child objects can start with their own
     * effective schema instead of sharing the parent's map instance.
     */
    private LinkedHashMap<String, AttributeSchema> copyAttributes(Map<String, AttributeSchema> attributes) {
        LinkedHashMap<String, AttributeSchema> copied = new LinkedHashMap<>();
        for (Map.Entry<String, AttributeSchema> entry : attributes.entrySet()) {
            copied.put(
                entry.getKey(),
                new AttributeSchema(entry.getValue().getAttributeName(), entry.getValue().getAttributeType()));
        }
        return copied;
    }

    /**
     * Ensures that a child object already contains every attribute required by the proposed parent
     * using the same normalized attribute names and declared types.
     */
    private void requireAllParentAttributesPresent(ObjectSchema objectSchema, ObjectSchema parentSchema)
            throws IOException {
        for (Map.Entry<String, AttributeSchema> parentEntry : parentSchema.getAttributes().entrySet()) {
            AttributeSchema childAttribute = objectSchema.getAttributes().get(parentEntry.getKey());
            if (childAttribute == null) {
                throw new IOException(
                    "Object "
                        + objectSchema.getObjectName()
                        + " is missing parent attribute "
                        + parentEntry.getKey());
            }
            if (childAttribute.getAttributeType() != parentEntry.getValue().getAttributeType()) {
                throw new IOException(
                    "Attribute type mismatch for inherited attribute "
                        + parentEntry.getKey());
            }
        }
    }

    /**
     * Returns whether attaching the proposed parent would create a cycle in the single-parent
     * inheritance chain.
     */
    private boolean wouldCreateParentCycle(
            DatabaseSchema schema,
            String childSlug,
            String proposedParentSlug) throws IOException {
        String currentSlug = proposedParentSlug;
        while (currentSlug != null) {
            if (childSlug.equals(currentSlug)) {
                return true;
            }
            currentSlug = requireObject(schema, currentSlug).getParentObjectName();
        }
        return false;
    }

    /**
     * Returns the directory used to store all attribute files for one object.
     */
    private Path getObjectDirectory(String objectSlug) {
        return objectsRootPath.resolve(objectSlug);
    }

    /**
     * Returns the path to one attribute file based on normalized object and attribute names.
     */
    private Path getAttributeFilePath(String objectSlug, String attributeSlug) {
        return getObjectDirectory(objectSlug).resolve(attributeSlug + ATTRIBUTE_FILE_EXTENSION);
    }

    /**
     * Normalizes a user-facing object or attribute name into the lowercase slug form used for
     * schema keys and on-disk paths.
     */
    private String normalizeName(String rawName) {
        if (rawName == null) {
            throw new IllegalArgumentException("Name cannot be null");
        }

        String normalized = rawName.trim()
            .toLowerCase(Locale.ROOT)
            .replaceAll("[^a-z0-9]+", "_")
            .replaceAll("^_+|_+$", "");

        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("Name must contain at least one alphanumeric character");
        }
        return normalized;
    }

    /**
     * Recursively deletes a directory tree from the bottom up so files are removed before their
     * parent directories.
     */
    private void deleteDirectory(Path directoryPath) throws IOException {
        if (!Files.exists(directoryPath)) {
            return;
        }

        List<Path> paths = Files.walk(directoryPath).sorted((left, right) -> right.compareTo(left)).toList();
        for (Path path : paths) {
            Files.deleteIfExists(path);
        }
    }

    /**
     * Guards every public operation after shutdown so the engine cannot be reused once closed.
     */
    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("CRUD engine has been closed");
        }
    }
}
