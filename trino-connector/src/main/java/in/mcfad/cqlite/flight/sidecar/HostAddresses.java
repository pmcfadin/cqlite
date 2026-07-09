package in.mcfad.cqlite.flight.sidecar;

/**
 * The single authority for turning a Sidecar-reported replica address into a bare host
 * literal and back into a URI authority (issue #2227).
 *
 * <p>The Sidecar returns replicas as {@code host:storage_port}. Both split-pinning
 * ({@code CqliteFlightSplit#host}) and per-host snapshot URI construction
 * ({@link HostSnapshotApis}) must agree on the exact host string, so the normalization
 * lives here once rather than being duplicated (and drifting) across call sites.
 *
 * <p><b>IPv6 handling (RFC 3986).</b> A port is only ever attached to an IPv6 literal in
 * bracketed form — {@code [2001:db8::5]:7000} — because an unbracketed {@code v6:port} is
 * ambiguous: the trailing {@code :NNN} is indistinguishable from another address group.
 * We therefore:
 * <ul>
 *   <li>{@code [v6]:port} / {@code [v6]} → strip brackets (and port) to the bare literal;
 *   <li>{@code host:port} / {@code v4:port} (exactly one colon, numeric tail) → strip port;
 *   <li>bare unbracketed multi-colon literal → treat the <em>whole</em> string as the host
 *       (never strip the last group — it is part of the address, not a port).
 * </ul>
 * {@link #authority(String, int)} re-brackets a bare IPv6 literal so the URI parses.
 */
public final class HostAddresses {

    private HostAddresses() {}

    /**
     * Normalize a Sidecar replica address to a bare host literal: no brackets, no port.
     * Idempotent — a value already normalized by this method passes through unchanged.
     */
    public static String hostOnly(String address) {
        if (address == null || address.isEmpty()) {
            return address;
        }
        // Bracketed IPv6 literal, optionally with a port: [v6] or [v6]:port.
        if (address.charAt(0) == '[') {
            int close = address.indexOf(']');
            if (close > 1) {
                return address.substring(1, close);
            }
            return address; // malformed; leave as-is
        }
        int firstColon = address.indexOf(':');
        if (firstColon < 0) {
            return address; // bare hostname or IPv4, no port
        }
        if (firstColon == address.lastIndexOf(':')) {
            // Exactly one colon: host:port or v4:port. Strip a numeric port only.
            String maybePort = address.substring(firstColon + 1);
            if (!maybePort.isEmpty() && maybePort.chars().allMatch(Character::isDigit)) {
                return address.substring(0, firstColon);
            }
            return address;
        }
        // Multiple colons, unbracketed → a bare IPv6 literal. An unbracketed "v6:port"
        // form is indistinguishable from a bare v6 address; per RFC 3986 a port is only
        // attached in bracketed form ([v6]:port), so we treat the whole string as host.
        return address;
    }

    /**
     * Build a URI authority ({@code host:port}) from a bare host literal, wrapping an
     * IPv6 literal in {@code [...]} so the authority parses. Accepts an already-bracketed
     * host defensively (no double-bracketing).
     */
    public static String authority(String host, int port) {
        String h = host;
        if (!host.isEmpty() && host.charAt(0) != '[' && host.indexOf(':') >= 0) {
            h = "[" + host + "]";
        }
        return h + ":" + port;
    }
}
