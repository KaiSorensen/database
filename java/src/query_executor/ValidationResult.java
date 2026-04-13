package query_executor;

public final class ValidationResult {
    private final boolean valid;
    private final String message;

    private ValidationResult(boolean valid, String message) {
        this.valid = valid;
        this.message = message;
    }

    public static ValidationResult valid() {
        return new ValidationResult(true, "");
    }

    public static ValidationResult invalid(String message) {
        return new ValidationResult(false, message);
    }

    public boolean isValid() {
        return valid;
    }

    public String message() {
        return message;
    }

    public ValidationResult and(ValidationResult other) {
        if (!valid) {
            return this;
        }
        return other;
    }
}
