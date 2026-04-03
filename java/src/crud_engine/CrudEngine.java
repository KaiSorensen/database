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
        ensureOpen();
        Files.createDirectories(databaseRoot);
        Files.createDirectories(metadataPath);
        Files.createDirectories(objectsRootPath);
        allocator.initialize();

        if (!Files.exists(schemaPath)) {
            writeSchemaInternal(new DatabaseSchema(SCHEMA_VERSION, new LinkedHashMap<>()));
        }

        validateSchemaState(loadSchemaInternal());
    }

    @Override
    public void createObject(String objectName, String parentObjectNameNullable) throws IOException {
        ensureOpen();

        DatabaseSchema schema = loadSchemaInternal();
        String objectSlug = normalizeName(objectName);
        if (schema.getObjects().containsKey(objectSlug)) {
            throw new IOException("Object already exists: " + objectName);
        }

        ObjectSchema objectSchema = new ObjectSchema();
        objectSchema.setObjectName(objectSlug);

        if (parentObjectNameNullable != null) {
            String parentSlug = normalizeName(parentObjectNameNullable);
            ObjectSchema parentSchema = requireObject(schema, parentSlug);
            objectSchema.setParentObjectName(parentSlug);
            objectSchema.setAttributes(copyAttributes(parentSchema.getAttributes()));
        } else {
            objectSchema.setParentObjectName(null);
            objectSchema.setAttributes(new LinkedHashMap<>());
        }

        Files.createDirectories(getObjectDirectory(objectSlug));
        for (String attributeSlug : objectSchema.getAttributes().keySet()) {
            writeAttributeAddresses(getAttributeFilePath(objectSlug, attributeSlug), List.of());
        }

        schema.getObjects().put(objectSlug, objectSchema);
        writeSchemaInternal(schema);
    }

    @Override
    public void deleteObject(String objectName) throws IOException {
        ensureOpen();

        DatabaseSchema schema = loadSchemaInternal();
        String objectSlug = normalizeName(objectName);
        requireObject(schema, objectSlug);

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
    public void close() throws IOException {
        if (closed) {
            return;
        }

        closed = true;
        allocator.close();
    }

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
        requireType(attributeSchema, expectedType);

        Path attributeFilePath = getAttributeFilePath(objectSlug, attributeSlug);
        List<Long> addresses = readAttributeAddresses(attributeFilePath);
        validateRowIndex(rowIndex, addresses.size());

        long currentAddress = addresses.get(rowIndex);
        long nextAddress;
        if (value == null) {
            deleteAllocationIfPresent(currentAddress);
            nextAddress = NULL_ADDRESS;
        } else {
            byte[] payload = toBytes(expectedType, value);
            nextAddress = currentAddress == NULL_ADDRESS
                ? allocator.create(payload)
                : allocator.update(currentAddress, payload);
        }

        addresses.set(rowIndex, nextAddress);
        writeAttributeAddresses(attributeFilePath, addresses);
        allocator.flush();
    }

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

    private byte[] toBytes(AttributeType attributeType, Object value) throws IOException {
        return switch (attributeType) {
            case INT -> TypeByteConversions.intToBytes((Integer) value);
            case STRING -> TypeByteConversions.stringToBytes((String) value);
            case BOOL -> TypeByteConversions.boolToBytes((Boolean) value);
            case ID -> TypeByteConversions.uuidToBytes((UUID) value);
        };
    }

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

    private DatabaseSchema loadSchemaInternal() throws IOException {
        DatabaseSchema schema = objectMapper.readValue(schemaPath.toFile(), DatabaseSchema.class);
        if (schema.getObjects() == null) {
            schema.setObjects(new LinkedHashMap<>());
        }
        if (schema.getSchemaVersion() == 0) {
            schema.setSchemaVersion(SCHEMA_VERSION);
        }

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

        validateSchemaState(schema);
        return schema;
    }

    private void writeSchemaInternal(DatabaseSchema schema) throws IOException {
        Files.createDirectories(metadataPath);
        objectMapper.writeValue(schemaPath.toFile(), schema);
    }

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

    private ObjectSchema requireObject(DatabaseSchema schema, String objectSlug) throws IOException {
        ObjectSchema objectSchema = schema.getObjects().get(objectSlug);
        if (objectSchema == null) {
            throw new IOException("Unknown object: " + objectSlug);
        }
        return objectSchema;
    }

    private AttributeSchema requireAttribute(ObjectSchema objectSchema, String attributeSlug)
            throws IOException {
        AttributeSchema attributeSchema = objectSchema.getAttributes().get(attributeSlug);
        if (attributeSchema == null) {
            throw new IOException(
                "Unknown attribute: " + objectSchema.getObjectName() + "." + attributeSlug);
        }
        return attributeSchema;
    }

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

    private List<Long> createNullAddresses(int rowCount) {
        List<Long> addresses = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            addresses.add(NULL_ADDRESS);
        }
        return addresses;
    }

    private void validateRowIndex(int rowIndex, int rowCount) throws IOException {
        if (rowIndex < 0 || rowIndex >= rowCount) {
            throw new IOException("Row index out of bounds: " + rowIndex);
        }
    }

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

    private void writeAttributeAddresses(Path attributeFilePath, List<Long> addresses) throws IOException {
        List<String> lines = new ArrayList<>(addresses.size());
        for (long address : addresses) {
            lines.add(Long.toString(address));
        }

        Files.createDirectories(attributeFilePath.getParent());
        Files.write(attributeFilePath, lines, StandardCharsets.UTF_8);
    }

    private void deleteAllocationIfPresent(long address) throws IOException {
        if (address != NULL_ADDRESS) {
            allocator.delete(address);
        }
    }

    private LinkedHashMap<String, AttributeSchema> copyAttributes(Map<String, AttributeSchema> attributes) {
        LinkedHashMap<String, AttributeSchema> copied = new LinkedHashMap<>();
        for (Map.Entry<String, AttributeSchema> entry : attributes.entrySet()) {
            copied.put(
                entry.getKey(),
                new AttributeSchema(entry.getValue().getAttributeName(), entry.getValue().getAttributeType()));
        }
        return copied;
    }

    private Path getObjectDirectory(String objectSlug) {
        return objectsRootPath.resolve(objectSlug);
    }

    private Path getAttributeFilePath(String objectSlug, String attributeSlug) {
        return getObjectDirectory(objectSlug).resolve(attributeSlug + ATTRIBUTE_FILE_EXTENSION);
    }

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

    private void deleteDirectory(Path directoryPath) throws IOException {
        if (!Files.exists(directoryPath)) {
            return;
        }

        List<Path> paths = Files.walk(directoryPath).sorted((left, right) -> right.compareTo(left)).toList();
        for (Path path : paths) {
            Files.deleteIfExists(path);
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("CRUD engine has been closed");
        }
    }
}
