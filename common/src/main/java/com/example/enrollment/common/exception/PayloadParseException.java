package com.example.enrollment.common.exception;

/** Raised when an inbound payload cannot be parsed into the canonical model. */
public class PayloadParseException extends RuntimeException {

    public PayloadParseException(String message) {
        super(message);
    }

    public PayloadParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
