package language_parser;

import java.util.ArrayList;
import java.util.List;
import language_parser.ast.AstNodes;
import query_executor.DataInterface;

public final class LanguageConsoleSupport {
    private LanguageConsoleSupport() {}

    public static boolean shouldPrintReadResult(AstNodes.ScriptNode script) {
        return script.statements().size() == 1 && script.statements().getFirst() instanceof AstNodes.ReadStatementNode;
    }

    public static String formatQueryData(DataInterface data) {
        if (data == null) {
            return "(no data)";
        }
        List<String> columnNames = data.getColumnNames();
        if (columnNames.isEmpty()) {
            return "(no columns)";
        }

        List<Integer> widths = new ArrayList<>();
        for (String columnName : columnNames) {
            int width = columnName.length();
            for (Object value : data.getColumnValues(columnName)) {
                width = Math.max(width, renderValue(value).length());
            }
            widths.add(width);
        }

        StringBuilder builder = new StringBuilder();
        builder.append(renderRow(columnNames, widths)).append('\n');
        builder.append(renderDivider(widths)).append('\n');
        for (int rowIndex = 0; rowIndex < data.getRowCount(); rowIndex++) {
            List<String> rowValues = new ArrayList<>();
            for (String columnName : columnNames) {
                rowValues.add(renderValue(data.getColumnValues(columnName).get(rowIndex)));
            }
            builder.append(renderRow(rowValues, widths)).append('\n');
        }
        builder.append("(").append(data.getRowCount()).append(" row");
        if (data.getRowCount() != 1) {
            builder.append("s");
        }
        builder.append(")");
        return builder.toString();
    }

    private static String renderRow(List<String> values, List<Integer> widths) {
        StringBuilder builder = new StringBuilder();
        for (int index = 0; index < values.size(); index++) {
            if (index > 0) {
                builder.append(" | ");
            }
            builder.append(padRight(values.get(index), widths.get(index)));
        }
        return builder.toString();
    }

    private static String renderDivider(List<Integer> widths) {
        StringBuilder builder = new StringBuilder();
        for (int index = 0; index < widths.size(); index++) {
            if (index > 0) {
                builder.append("-+-");
            }
            builder.append("-".repeat(widths.get(index)));
        }
        return builder.toString();
    }

    private static String padRight(String value, int width) {
        return value + " ".repeat(Math.max(0, width - value.length()));
    }

    private static String renderValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof String stringValue) {
            return "\"" + stringValue.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
        }
        return String.valueOf(value);
    }
}
