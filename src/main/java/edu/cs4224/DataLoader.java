package edu.cs4224;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;

import java.io.Closeable;

public class DataLoader implements Closeable {

    private final CqlSession session;

    public DataLoader() {
        session = CqlSession.builder().
                withKeyspace(CqlIdentifier.fromCql("wholesale")).
                build();
    }

    private void load() {

    }

    private void load(String tableName) {
    }

    @Override
    public void close() {
        if (session != null)
            session.close();
    }

    public static void main(String[] args) {
        try (DataLoader loader = new DataLoader()) {
            loader.load();
        }
    }
}
