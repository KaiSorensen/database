package crud_engine;

import java.util.LinkedHashMap;
import java.util.Map;

public class ObjectSchema {
    private String objectName;
    private String parentObjectName;
    private Map<String, AttributeSchema> attributes;

    public ObjectSchema() {
        this.attributes = new LinkedHashMap<>();
    }

    public ObjectSchema(
            String objectName,
            String parentObjectName,
            Map<String, AttributeSchema> attributes) {
        this.objectName = objectName;
        this.parentObjectName = parentObjectName;
        this.attributes = attributes;
    }

    public String getObjectName() {
        return objectName;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    public String getParentObjectName() {
        return parentObjectName;
    }

    public void setParentObjectName(String parentObjectName) {
        this.parentObjectName = parentObjectName;
    }

    public Map<String, AttributeSchema> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, AttributeSchema> attributes) {
        this.attributes = attributes;
    }
}
