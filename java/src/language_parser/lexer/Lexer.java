package language_parser.lexer;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public final class Lexer {
    private static final Set<String> KEYWORDS = Set.of(
        "create_object",
        "read_object",
        "update_object",
        "delete_object",
        "create_attribute",
        "read_attribute",
        "update_attribute",
        "delete_attribute",
        "read",
        "join",
        "on",
        "attributes",
        "derive",
        "summarize",
        "where",
        "create_rows",
        "values",
        "row",
        "update",
        "set",
        "delete_rows",
        "delete_attributes",
        "extends",
        "include_attributes",
        "include_row_count",
        "include_type",
        "rename",
        "add",
        "subtract",
        "multiply",
        "divide",
        "modulo",
        "contains",
        "equals",
        "not_equals",
        "greater_than",
        "less_than",
        "greater_than_or_equals",
        "less_than_or_equals",
        "and",
        "or",
        "not",
        "sum",
        "average",
        "min",
        "max",
        "count",
        "median",
        "mode",
        "int",
        "string",
        "bool",
        "id",
        "true",
        "false"
    );

    public List<Token> lex(String source) {
        if (source == null) {
            throw new IllegalArgumentException("Source text must not be null.");
        }
        List<Token> tokens = new ArrayList<>();
        int index = 0;
        int line = 1;
        int column = 1;

        while (index < source.length()) {
            char character = source.charAt(index);
            if (Character.isWhitespace(character)) {
                if (character == '\n') {
                    line++;
                    column = 1;
                } else {
                    column++;
                }
                index++;
                continue;
            }

            int tokenColumn = column;
            switch (character) {
                case '(' -> {
                    tokens.add(new Token(TokenType.LEFT_PAREN, "(", null, line, tokenColumn));
                    index++;
                    column++;
                }
                case ')' -> {
                    tokens.add(new Token(TokenType.RIGHT_PAREN, ")", null, line, tokenColumn));
                    index++;
                    column++;
                }
                case ',' -> {
                    tokens.add(new Token(TokenType.COMMA, ",", null, line, tokenColumn));
                    index++;
                    column++;
                }
                case '.' -> {
                    tokens.add(new Token(TokenType.DOT, ".", null, line, tokenColumn));
                    index++;
                    column++;
                }
                case ';' -> {
                    tokens.add(new Token(TokenType.SEMICOLON, ";", null, line, tokenColumn));
                    index++;
                    column++;
                }
                case '"' -> {
                    StringBuilder text = new StringBuilder();
                    index++;
                    column++;
                    boolean closed = false;
                    while (index < source.length()) {
                        char next = source.charAt(index);
                        if (next == '"' && (index == 0 || source.charAt(index - 1) != '\\')) {
                            closed = true;
                            index++;
                            column++;
                            break;
                        }
                        if (next == '\\' && index + 1 < source.length()) {
                            char escaped = source.charAt(index + 1);
                            text.append(switch (escaped) {
                                case '"' -> '"';
                                case '\\' -> '\\';
                                case 'n' -> '\n';
                                case 't' -> '\t';
                                default -> escaped;
                            });
                            index += 2;
                            column += 2;
                            continue;
                        }
                        text.append(next);
                        if (next == '\n') {
                            line++;
                            column = 1;
                        } else {
                            column++;
                        }
                        index++;
                    }
                    if (!closed) {
                        throw new IllegalArgumentException("Unterminated string literal at line " + line + ", column " + tokenColumn);
                    }
                    tokens.add(new Token(TokenType.STRING, source.substring(index - text.length() - 2, index), text.toString(), line, tokenColumn));
                }
                default -> {
                    if (Character.isDigit(character)) {
                        int start = index;
                        while (index < source.length() && Character.isDigit(source.charAt(index))) {
                            index++;
                            column++;
                        }
                        String lexeme = source.substring(start, index);
                        tokens.add(new Token(TokenType.INTEGER, lexeme, Integer.parseInt(lexeme), line, tokenColumn));
                    } else if (isIdentifierStart(character)) {
                        int start = index;
                        while (index < source.length() && isIdentifierPart(source.charAt(index))) {
                            index++;
                            column++;
                        }
                        String lexeme = source.substring(start, index);
                        String lowered = lexeme.toLowerCase();
                        if ("true".equals(lowered) || "false".equals(lowered)) {
                            tokens.add(new Token(TokenType.BOOLEAN, lexeme, Boolean.parseBoolean(lowered), line, tokenColumn));
                        } else if (KEYWORDS.contains(lowered)) {
                            tokens.add(new Token(TokenType.KEYWORD, lexeme, null, line, tokenColumn));
                        } else {
                            tokens.add(new Token(TokenType.IDENTIFIER, lexeme, null, line, tokenColumn));
                        }
                    } else {
                        throw new IllegalArgumentException("Unexpected character '" + character + "' at line " + line + ", column " + tokenColumn);
                    }
                }
            }
        }

        tokens.add(new Token(TokenType.EOF, "", null, line, column));
        return List.copyOf(tokens);
    }

    private boolean isIdentifierStart(char character) {
        return Character.isLetter(character) || character == '_';
    }

    private boolean isIdentifierPart(char character) {
        return Character.isLetterOrDigit(character) || character == '_';
    }
}
