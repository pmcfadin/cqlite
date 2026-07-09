package in.mcfad.cqlite.flight;

import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fail-fast arrow-java memory preflight (issues #2193, #2290): the probe must raise an actionable
 * Trino configuration error — naming the EXACT missing JVM flag — when arrow-java's {@code MemoryUtil}
 * static initializer fails, and must pass unrelated errors through untouched so it never masks a
 * different bug.
 *
 * <p>The Gradle test JVM sets the add-opens flag (build.gradle.kts), so {@link
 * ArrowMemoryPreflight#verify()} succeeds here; the flag-absent path is exercised by driving the
 * classifier/formatter with a synthesized arrow-init failure rather than manipulating live JVM flags.
 */
class ArrowMemoryPreflightTest {

    private static ExceptionInInitializerError memoryUtilInitFailure() {
        // Mirror the real failure shape: MemoryUtil.<clinit> throws, wrapped by the JVM in an
        // ExceptionInInitializerError whose stack frame is in ...memory.util.MemoryUtil.
        RuntimeException clinit = new RuntimeException(
                "Failed to initialize MemoryUtil: java.nio not open to org.apache.arrow.memory.core");
        clinit.setStackTrace(new StackTraceElement[] {
                new StackTraceElement("org.apache.arrow.memory.util.MemoryUtil", "<clinit>", "MemoryUtil.java", 1)
        });
        return new ExceptionInInitializerError(clinit);
    }

    @Test
    void messageNamesTheExactMissingFlag() {
        String message = ArrowMemoryPreflight.formatMessage(memoryUtilInitFailure());
        assertTrue(message.contains(
                        "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"),
                "remedy must name the exact arrow-java-documented flag");
        assertTrue(message.contains("jvm.config"), "remedy must point at jvm.config");
        assertEquals("--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED",
                ArrowMemoryPreflight.REQUIRED_ADD_OPENS);
    }

    @Test
    void classifiesMemoryUtilInitFailureByStackFrame() {
        assertTrue(ArrowMemoryPreflight.isArrowMemoryInitFailure(memoryUtilInitFailure()));
    }

    @Test
    void classifiesMemoryUtilFailureByMessage() {
        assertTrue(ArrowMemoryPreflight.isArrowMemoryInitFailure(
                new NoClassDefFoundError("Could not initialize class org.apache.arrow.memory.util.MemoryUtil")));
    }

    @Test
    void unrelatedFailureIsNotClassified() {
        assertFalse(ArrowMemoryPreflight.isArrowMemoryInitFailure(
                new IllegalStateException("some other connector bug")));
    }

    @Test
    void arrowInitFailureBecomesConfigurationInvalid() {
        TrinoException thrown = assertThrows(TrinoException.class,
                () -> ArrowMemoryPreflight.throwIfArrowMemoryInit(memoryUtilInitFailure()));
        assertEquals(StandardErrorCode.CONFIGURATION_INVALID.toErrorCode(), thrown.getErrorCode());
        assertTrue(thrown.getMessage().contains(
                "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"));
    }

    @Test
    void unrelatedRuntimeExceptionIsRethrownUnchanged() {
        IllegalStateException original = new IllegalStateException("unrelated boom");
        IllegalStateException rethrown = assertThrows(IllegalStateException.class,
                () -> ArrowMemoryPreflight.throwIfArrowMemoryInit(original));
        assertSame(original, rethrown, "unrelated errors must pass through untouched, never masked");
    }

    @Test
    void unrelatedErrorIsRethrownUnchanged() {
        OutOfMemoryError original = new OutOfMemoryError("heap");
        OutOfMemoryError rethrown = assertThrows(OutOfMemoryError.class,
                () -> ArrowMemoryPreflight.throwIfArrowMemoryInit(original));
        assertSame(original, rethrown);
    }

    @Test
    void verifyPassesWhenFlagPresent() {
        // The Gradle test JVM carries the add-opens flag, so the real probe must succeed and leave
        // no allocator open (side-effect-free happy path).
        ArrowMemoryPreflight.verify();
    }
}
