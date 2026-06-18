package com.rustyrazorblade.cqlite.flight;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

/** A projected column: its name and resolved Trino type. */
public record CqliteFlightColumnHandle(String name, Type type) implements ColumnHandle {}
