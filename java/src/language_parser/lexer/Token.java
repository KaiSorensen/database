package language_parser.lexer;

public record Token(
    TokenType tokenType,
    String lexeme,
    Object literalValue,
    int line,
    int column
) {
}
