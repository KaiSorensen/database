package crud_engine;

import java.io.IOException;
import java.util.UUID;

public interface CrudEngineInterface extends AutoCloseable {

    enum AttributeType {
        ID,
        INT,
        STRING,
        BOOL,
    }  

    // open questions:
    /* 
    -   At what level should filtering occur?
    -   This layer will be responsible for all things that require database file interaction.
        It will provide the abstraction over the actual database.
        So, the layer above will decide how to take the parse tree and execute fitlers on specific data.
        This layer will only allow you to choose which data you interact with, not how. It deals with all the addressing so that the layer above can focus on the query.
        So, we need to give the above layer ability to access any stored data. The layer above decides what to do with it.
        That means object-attribute control. Not anything past addressing the data.   
    -   BIG QUESTION that I haven't explicily addressed:
        If all the attributes are different files, how do we know what data goes in a row together? 
        Ultimately, it must be by index. So, we need a way to indicate a null value so that it still has an address.
        For strings, there's a designated byte for null. For integers, what shall we do? Signed? I don't know. For true/false? I don't know.
    */
    
    // create object
    void createObject(String objectName, String parentObjectNameNullable) throws IOException;

    // delete object
    void deleteObject(String objectName) throws IOException;

    // rename object (no update: you can only "update" the name)
    void renameObject(String oldObjectName, String newObjectName) throws IOException;

    void removeParent(String objectName) throws IOException;

    void addParent(String objectName, String parentObjectName) throws IOException;

    // create attribute
    void createAttribute(String objectName, String attributeName, AttributeType attributeType) throws IOException;

    // delete attribute
    void deleteAttribute(String objectName, String attributeName) throws IOException;

    // rename attribute (no update: you can only "update" the name)
    void renameAttribute(String objectName, String oldAttributeName, String newAttributeName) throws IOException;

    DatabaseSchema readSchema() throws IOException;

    int insertRow(String objectName) throws IOException;

    int getRowCount(String objectName) throws IOException;

    void writeInt(String objectName, String attributeName, int rowIndex, Integer value) throws IOException;

    Integer readInt(String objectName, String attributeName, int rowIndex) throws IOException;

    void writeString(String objectName, String attributeName, int rowIndex, String value) throws IOException;

    String readString(String objectName, String attributeName, int rowIndex) throws IOException;

    void writeBool(String objectName, String attributeName, int rowIndex, Boolean value) throws IOException;

    Boolean readBool(String objectName, String attributeName, int rowIndex) throws IOException;

    void writeId(String objectName, String attributeName, int rowIndex, UUID value) throws IOException;

    UUID readId(String objectName, String attributeName, int rowIndex) throws IOException;

    void deleteRow(String objectName, int rowIndex) throws IOException;

    
    // i'm not sure there's more to it. in what layer should we convert between bytes and types?
    // should this class provide a different function for each type and perform the conversions? probably.
    // each function would take an object and attribute column, then the data. we would know by the primitive type which one is being used
    // so how inefficient is this? I feel like an operation on a whole column will take forever! we'll find out. either way, this is a weekend project.

    @Override
    void close() throws IOException;
    
}
