package in.mcfad.cqlite.flight.sidecar;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The single host-address authority (issue #2227): {@link HostAddresses#hostOnly} must
 * strip a storage port for IPv4/hostnames AND for bracketed IPv6, but must never chop a
 * group off a bare IPv6 literal; {@link HostAddresses#authority} re-brackets IPv6 so the
 * derived Sidecar URI parses.
 */
class HostAddressesTest {

    @Test
    void stripsPortFromIpv4AndHostname() {
        assertEquals("10.0.0.5", HostAddresses.hostOnly("10.0.0.5:7000"));
        assertEquals("db3", HostAddresses.hostOnly("db3:7000"));
    }

    @Test
    void leavesPortlessIpv4AndHostnameUntouched() {
        assertEquals("10.0.0.5", HostAddresses.hostOnly("10.0.0.5"));
        assertEquals("db3", HostAddresses.hostOnly("db3"));
    }

    @Test
    void stripsBracketsAndPortFromBracketedIpv6() {
        assertEquals("2001:db8::5", HostAddresses.hostOnly("[2001:db8::5]:7000"));
        assertEquals("2001:db8::5", HostAddresses.hostOnly("[2001:db8::5]"));
    }

    @Test
    void treatsBareUnbracketedIpv6AsHost() {
        // No brackets + multiple colons: the whole string is the address — the trailing
        // group is NOT a port (RFC 3986: a port is only attached in bracketed form).
        assertEquals("2001:db8::5", HostAddresses.hostOnly("2001:db8::5"));
        assertEquals("2001:db8::5:7000", HostAddresses.hostOnly("2001:db8::5:7000"));
    }

    @Test
    void hostOnlyIsIdempotent() {
        String bare = HostAddresses.hostOnly("[2001:db8::5]:7000");
        assertEquals(bare, HostAddresses.hostOnly(bare));
    }

    @Test
    void authorityBracketsIpv6ButNotIpv4() {
        assertEquals("[2001:db8::5]:9043", HostAddresses.authority("2001:db8::5", 9043));
        assertEquals("10.0.0.5:9043", HostAddresses.authority("10.0.0.5", 9043));
        assertEquals("db3:9043", HostAddresses.authority("db3", 9043));
    }

    @Test
    void authorityDoesNotDoubleBracket() {
        assertEquals("[2001:db8::5]:9043", HostAddresses.authority("[2001:db8::5]", 9043));
    }
}
