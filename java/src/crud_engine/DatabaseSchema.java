package crud_engine;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseSchema {
    private int schemaVersion;
    private Map<String, ObjectSchema> objects;

    public DatabaseSchema() {
        this(1, new LinkedHashMap<>());
    }

    public DatabaseSchema(int schemaVersion, Map<String, ObjectSchema> objects) {
        this.schemaVersion = schemaVersion;
        this.objects = objects;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public Map<String, ObjectSchema> getObjects() {
        return objects;
    }

    public void setObjects(Map<String, ObjectSchema> objects) {
        this.objects = objects;
    }
}
