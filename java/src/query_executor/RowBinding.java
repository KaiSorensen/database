package query_executor;

import java.util.LinkedHashMap;
import java.util.Map;

public final class RowBinding {
    private final int resultRowIndex;
    private final LinkedHashMap<String, Integer> sourceRowIndexByObject;

    public RowBinding(int resultRowIndex, Map<String, Integer> sourceRowIndexByObject) {
        this.resultRowIndex = resultRowIndex;
        this.sourceRowIndexByObject = new LinkedHashMap<>(sourceRowIndexByObject);
    }

    public int resultRowIndex() {
        return resultRowIndex;
    }

    public LinkedHashMap<String, Integer> sourceRowIndexByObject() {
        return new LinkedHashMap<>(sourceRowIndexByObject);
    }

    public Integer sourceRowIndexForObject(String objectName) {
        return sourceRowIndexByObject.get(objectName);
    }
}
