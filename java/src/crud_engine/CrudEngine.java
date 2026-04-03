package crud_engine;

import java.io.File;
import java.io.IOException;

import memory_allocator.BitmapMemoryAllocator;

public class CrudEngine implements CrudEngineInterface {

    private BitmapMemoryAllocator allocator;

    public CrudEngine() {
        
    }

    @Override
    public void createObject(String objectName) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createObject'");
    }

    @Override
    public void deleteObject(String objectName) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'deleteObject'");
    }

    @Override
    public void renameObject(String oldObjectName, String newObjectName) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'renameObject'");
    }

    @Override
    public void createAttribute(String objectName, String attributeName, AttributeType attributeType)
            throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createAttribute'");
    }

    @Override
    public void deleteAttribute(String objectName, String attributeName) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'deleteAttribute'");
    }

    @Override
    public void renameAttribute(String objectName, String oldAttributeName, String newAttributeName)
            throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'renameAttribute'");
    }

    @Override
    public long createData(String objectName, String attributeName, byte[] data) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createData'");
    }

    @Override
    public byte[] readData(String objectName, String attributeName) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'readData'");
    }

    @Override
    public long updateData(String objectName, String attributeName, byte[] data) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'updateData'");
    }

    @Override
    public void deleteData(String objectName, String attributeName, long address) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'deleteData'");
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'close'");
    }

    
    
}
