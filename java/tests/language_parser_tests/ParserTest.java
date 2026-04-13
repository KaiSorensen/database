package language_parser_tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import language_parser.ast.AstNodes;
import language_parser.parser.ParseException;
import language_parser.parser.Parser;
import org.junit.jupiter.api.Test;

public class ParserTest {
    private final Parser parser = new Parser();

    @Test
    void parsesAllStatementFamilies() {
        AstNodes.ScriptNode script = parser.parse(
            """
            create_object(people, extends(person));
            read_object(people, include_attributes(), include_row_count());
            update_object(people, rename(customers));
            delete_object(old_people);
            create_attribute(people, age, int);
            create_attribute(people, total, int, derive(add(age, bonus)));
            read_attribute(people, age, include_type());
            update_attribute(people, age, rename(years));
            delete_attribute(people, bonus);
            read(people, attributes(name, age), where(greater_than(age, 20)));
            create_rows(people, values(row(name("Dora"), age(40), bonus(11))));
            update(people, set(age, add(age, bonus)), where(greater_than(age, 20)));
            delete_rows(people, where(greater_than(age, 20)));
            delete_attributes(people, attributes(bonus, age));
            """
        );

        assertEquals(14, script.statements().size());
        assertInstanceOf(AstNodes.ObjectStatementNode.class, script.statements().get(0));
        assertInstanceOf(AstNodes.AttributeStatementNode.class, script.statements().get(4));
        assertInstanceOf(AstNodes.ReadStatementNode.class, script.statements().get(9));
        assertInstanceOf(AstNodes.CreateRowsStatementNode.class, script.statements().get(10));
        assertInstanceOf(AstNodes.UpdateStatementNode.class, script.statements().get(11));
        assertInstanceOf(AstNodes.DeleteRowsStatementNode.class, script.statements().get(12));
        assertInstanceOf(AstNodes.DeleteAttributesStatementNode.class, script.statements().get(13));
    }

    @Test
    void parsesJoinReadProjection() {
        AstNodes.ScriptNode script = parser.parse(
            """
            read(join(people, pets, on(equals(people.name, pets.owner_name))), attributes(people.name, pet_name));
            """
        );

        AstNodes.ReadStatementNode statement = (AstNodes.ReadStatementNode) script.statements().getFirst();
        assertInstanceOf(AstNodes.JoinSourceNode.class, statement.source());
        assertInstanceOf(AstNodes.AttributeProjectionNode.class, statement.projection());
    }

    @Test
    void rejectsMissingSemicolon() {
        assertThrows(ParseException.class, () -> parser.parse("create_object(people)"));
    }

    @Test
    void rejectsDuplicateRowKeys() {
        assertThrows(
            ParseException.class,
            () -> parser.parse("create_rows(people, values(row(name(\"Dora\"), name(\"Evan\"))));")
        );
    }

    @Test
    void rejectsMixedProjectionForms() {
        assertThrows(
            ParseException.class,
            () -> parser.parse("read(people, attributes(name), derive(total, add(age, bonus)));")
        );
    }
}
