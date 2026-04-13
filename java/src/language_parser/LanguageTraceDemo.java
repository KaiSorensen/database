package language_parser;

import crud_engine.CrudEngine;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class LanguageTraceDemo {
    private static final Path DEMO_PATH = Path.of("TEMP_LANGUAGE_TRACE");

    private LanguageTraceDemo() {}

    public static void main(String[] args) throws Exception {
        resetDemoPath();

        List<String> commands = List.of(
            "create_object(people);",
            "create_attribute(people, name, string);",
            "create_attribute(people, age, int);",
            "create_attribute(people, bonus, int);",
            "create_rows(people, values(row(name(\"Ada\"), age(20), bonus(5)), row(name(\"Bob\"), age(30), bonus(7)), row(name(\"Cara\"), age(25), bonus(9))));",
            "read(people, attributes(name, age), where(greater_than(age, 20)));",
            "create_attribute(people, total, int, derive(add(age, bonus)));",
            "update(people, set(age, add(age, bonus)), where(greater_than(age, 20)));",
            "update(people, set(bonus, max(age)), where(greater_than(age, 20)));",
            "delete_rows(people, where(greater_than(age, 30)));",
            "delete_attributes(people, attributes(total));"
        );

        try (CrudEngine engine = new CrudEngine(DEMO_PATH)) {
            engine.initialize();
            LanguageExecutor executor = new LanguageExecutor(engine);

            System.out.println("Initial state");
            System.out.println(engine.dumpDatabase());

            for (int index = 0; index < commands.size(); index++) {
                String command = commands.get(index);
                System.out.println("Command " + (index + 1) + ": " + command);
                var result = executor.execute(command);
                if (!result.success()) {
                    System.out.println("FAILED: " + result.message());
                    break;
                }
                System.out.println(engine.dumpDatabase());
            }
        }

        System.out.println("Trace database path: " + DEMO_PATH.toAbsolutePath());
    }

    private static void resetDemoPath() throws IOException {
        if (!Files.exists(DEMO_PATH)) {
            return;
        }
        List<Path> paths = Files.walk(DEMO_PATH).sorted((left, right) -> right.compareTo(left)).toList();
        for (Path path : paths) {
            Files.deleteIfExists(path);
        }
    }
}
