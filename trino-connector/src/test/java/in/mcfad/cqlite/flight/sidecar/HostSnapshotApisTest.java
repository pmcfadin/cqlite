package in.mcfad.cqlite.flight.sidecar;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Per-host Sidecar client derivation (issue #2227): the factory reaches each replica's
 * co-located Sidecar at the configured scheme + port, caches one client per host, and
 * fails loudly if the configured URI has no explicit port to reuse.
 */
class HostSnapshotApisTest {

    @Test
    void cachesOneClientPerHost() {
        HostSnapshotApis factory = HostSnapshotApis.fromBaseUri(URI.create("http://db0:9043"));

        SnapshotApi a1 = factory.forHost("10.0.0.2");
        SnapshotApi a2 = factory.forHost("10.0.0.2");
        SnapshotApi b = factory.forHost("10.0.0.3");

        assertSame(a1, a2, "same host reuses one client");
        assertNotSame(a1, b, "different hosts get different clients");
    }

    @Test
    void portlessBaseUriFailsClosedWithActionableMessage() {
        HostSnapshotApis factory = HostSnapshotApis.fromBaseUri(URI.create("http://db0"));

        IllegalStateException ex =
                assertThrows(IllegalStateException.class, () -> factory.forHost("10.0.0.9"));
        assertTrue(ex.getMessage().contains("port"), ex.getMessage());
        assertTrue(ex.getMessage().contains("10.0.0.9"), ex.getMessage());
    }

    @Test
    void ipv6HostIsBracketedIntoAValidUri() {
        HostSnapshotApis factory = HostSnapshotApis.fromBaseUri(URI.create("http://db0:9043"));
        // A bare IPv6 literal must be bracketed or URI.create would throw — this must not.
        SnapshotApi client = factory.forHost("2001:db8::5");
        assertTrue(client instanceof SidecarClient);
        assertEquals(URI.create("http://[2001:db8::5]:9043"), ((SidecarClient) client).base());
    }

    /**
     * IPv6 in port-bearing forms (issue #2227): a bracketed {@code [v6]:port} or a bare
     * {@code v6} must all derive the SAME valid Sidecar URI at the configured port — and
     * collapse to one cached client, since they name the same host.
     */
    @Test
    void ipv6PortBearingFormsAllDeriveTheSameConfiguredPortUri() {
        HostSnapshotApis factory = HostSnapshotApis.fromBaseUri(URI.create("https://db0:9043"));
        URI expected = URI.create("https://[2001:db8::5]:9043");

        SnapshotApi fromBracketedPort = factory.forHost("[2001:db8::5]:7000");
        SnapshotApi fromBracketed = factory.forHost("[2001:db8::5]");
        SnapshotApi fromBare = factory.forHost("2001:db8::5");

        assertEquals(expected, ((SidecarClient) fromBracketedPort).base());
        assertEquals(expected, ((SidecarClient) fromBracketed).base());
        assertEquals(expected, ((SidecarClient) fromBare).base());
        // Same host across the three forms → one cached client.
        assertSame(fromBracketedPort, fromBracketed);
        assertSame(fromBracketed, fromBare);
    }

    @Test
    void ipv4PortBearingFormDerivesHostOnlyUri() {
        HostSnapshotApis factory = HostSnapshotApis.fromBaseUri(URI.create("http://db0:9043"));
        SnapshotApi client = factory.forHost("10.0.0.5:7000");
        assertEquals(URI.create("http://10.0.0.5:9043"), ((SidecarClient) client).base());
    }
}
