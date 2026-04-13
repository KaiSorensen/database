package language_parser_tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import language_parser.lexer.Lexer;
import language_parser.lexer.Token;
import language_parser.lexer.TokenType;
import org.junit.jupiter.api.Test;

public class LexerTest {
    private final Lexer lexer = new Lexer();

    @Test
    void tokenizesCommandsReferencesAndLiterals() {
        List<Token> tokens = lexer.lex(
            """
            read(people, attributes(name, pets.owner_name), where(greater_than(age, 20)));
            create_rows(people, values(row(name("Dora"), age(40), bonus(11))));
            """
        );

        assertEquals(TokenType.KEYWORD, tokens.get(0).tokenType());
        assertEquals("read", tokens.get(0).lexeme());
        assertEquals(TokenType.IDENTIFIER, tokens.get(2).tokenType());
        assertEquals("people", tokens.get(2).lexeme());
        assertEquals(1L, tokens.stream().filter(token -> token.tokenType() == TokenType.DOT).count());
        assertEquals(TokenType.INTEGER, tokens.stream().filter(token -> token.literalValue() instanceof Integer).findFirst().orElseThrow().tokenType());
        assertEquals(TokenType.STRING, tokens.stream().filter(token -> "Dora".equals(token.literalValue())).findFirst().orElseThrow().tokenType());
        assertEquals(TokenType.SEMICOLON, tokens.stream().filter(token -> token.tokenType() == TokenType.SEMICOLON).findFirst().orElseThrow().tokenType());
    }
}
