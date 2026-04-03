package crud_engine;

public class AttributeSchema {
    private String attributeName;
    private CrudEngineInterface.AttributeType attributeType;

    public AttributeSchema() {}

    public AttributeSchema(String attributeName, CrudEngineInterface.AttributeType attributeType) {
        this.attributeName = attributeName;
        this.attributeType = attributeType;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }

    public CrudEngineInterface.AttributeType getAttributeType() {
        return attributeType;
    }

    public void setAttributeType(CrudEngineInterface.AttributeType attributeType) {
        this.attributeType = attributeType;
    }
}
