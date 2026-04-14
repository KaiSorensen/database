package language_parser;

import crud_engine.CrudEngine;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import language_parser.ast.AstNodes;
import language_parser.parser.Parser;
import query_executor.QueryExecutionResult;

public final class LanguageRepl {
    private LanguageRepl() {}

    public static void main(String[] args) throws Exception {
        try (CrudEngine engine = new CrudEngine()) {
            engine.initialize();
            LanguageExecutor executor = new LanguageExecutor(engine);
            Parser parser = new Parser();
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

            System.out.println("Hierarchical SQL console");
            System.out.println("Enter one statement terminated by ';'. Press Ctrl-C to exit.");

            StringBuilder buffer = new StringBuilder();
            while (true) {
                System.out.print(buffer.isEmpty() ? "db> " : "... ");
                String line = reader.readLine();
                if (line == null) {
                    System.out.println();
                    break;
                }

                String trimmedLine = line.trim();
                if (buffer.isEmpty() && (trimmedLine.equalsIgnoreCase("exit;") || trimmedLine.equalsIgnoreCase("quit;"))) {
                    break;
                }
                if (trimmedLine.isEmpty() && buffer.isEmpty()) {
                    continue;
                }

                buffer.append(line).append('\n');
                if (!trimmedLine.endsWith(";")) {
                    continue;
                }

                String command = buffer.toString().trim();
                buffer.setLength(0);

                AstNodes.ScriptNode script;
                try {
                    script = parser.parse(command);
                } catch (Exception exception) {
                    System.out.println("ERROR: " + exception.getMessage());
                    continue;
                }

                QueryExecutionResult result = executor.execute(command);
                if (!result.success()) {
                    System.out.println("ERROR: " + result.message());
                    continue;
                }

                if (LanguageConsoleSupport.shouldPrintReadResult(script)) {
                    System.out.println(LanguageConsoleSupport.formatQueryData(result.returnedData()));
                } else {
                    System.out.println(engine.dumpDatabase());
                }
            }
        }
    }
}
