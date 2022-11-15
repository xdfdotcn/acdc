/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package cn.xdf.acdc.connect.jdbc.dialect;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A registry of {@link DatabaseDialect} instances.
 *
 * <p>The dialect framework uses Java's {@link ServiceLoader} mechanism to find and automatically
 * register all {@link DatabaseDialectProvider} implementations on the classpath. Don't forget to include in your JAR
 * file a {@code META-INF/services/io.confluent.connect.jdbc.dialect .DatabaseDialectProvider} file that contains the
 * fully-qualified name of your implementation class (or one class per line if providing multiple implementations).
 *
 * <p>This discovery and registration process uses DEBUG messages to report the {@link
 * DatabaseDialectProvider} classes that are found and registered. If you have difficulties getting the connector to
 * find and register your dialect implementation classes, check that your JARs have the service provider file and your
 * JAR is included in the JDBC connector's plugin directory.
 */
@Slf4j
public final class DatabaseDialects {

    /**
     * The regular expression pattern to extract the JDBC subprotocol and subname from a JDBC URL of the form {@code
     * jdbc:<subprotocol>:<subname>} where {@code subprotocol} defines the kind of database connectivity mechanism that
     * may be supported by one or more drivers. The contents and syntax of the {@code subname} will depend on the
     * subprotocol.
     *
     * <p>The subprotocol will be in group 1, and the subname will be in group 2.
     */
    private static final Pattern PROTOCOL_PATTERN = Pattern.compile("jdbc:([^:]+):(.*)");

    // Sort lexicographically to maintain order
    private static final ConcurrentMap<String, DatabaseDialectProvider> REGISTRY = new ConcurrentSkipListMap<>();

    static {
        loadAllDialects();
    }

    private DatabaseDialects() {
    }

    // todo: better way
    // CHECKSTYLE:OFF
    private static void loadAllDialects() {
        log.debug("Searching for and loading all JDBC source dialects on the classpath");
        final AtomicInteger count = new AtomicInteger();
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            ServiceLoader<DatabaseDialectProvider> loadedDialects = ServiceLoader.load(
                    DatabaseDialectProvider.class
            );
            // Always use ServiceLoader.iterator() to get lazy loading (see JavaDocs)
            Iterator<DatabaseDialectProvider> dialectIterator = loadedDialects.iterator();
            try {
                while (dialectIterator.hasNext()) {
                    try {
                        DatabaseDialectProvider provider = dialectIterator.next();
                        REGISTRY.put(provider.getClass().getName(), provider);
                        count.incrementAndGet();
                        log.debug("Found '{}' provider {}", provider, provider.getClass());
                    } catch (Throwable t) {
                        log.debug("Skipping dialect provider after error while loading", t);
                    }
                }
            } catch (Throwable t) {
                log.debug("Error loading dialect providers", t);
            }
            return null;
        });
        log.debug("Registered {} source dialects", count.get());
    }
    // CHECKSTYLE:ON

    /**
     * Find the {@link DatabaseDialect} that has the highest {@link DatabaseDialectProvider#score(DatabaseDialectProvider.JdbcUrlInfo) score}
     * for the supplied JDBC URL and Connection, and return a new instance of that dialect. Note that the
     * DatabaseDialect needs to be {@link DatabaseDialect#close() closed}.
     *
     * @param jdbcUrl the JDBC connection URL; may not be null
     * @param config  the connector configuration used to create the dialect; may not be null
     * @return the {@link DatabaseDialect} instance with the greatest score; never null, but possibly the {@link
     * DatabaseDialect default DatabaseDialect}
     * @throws ConnectException if there is a problem with the JDBC URL
     */
    public static DatabaseDialect findBestFor(
            final String jdbcUrl,
            final AbstractConfig config
    ) throws ConnectException {
        final DatabaseDialectProvider.JdbcUrlInfo info = extractJdbcUrlInfo(jdbcUrl);
        log.debug("Finding best dialect for {}", info);
        int bestScore = DatabaseDialectProvider.NO_MATCH_SCORE;

        // Now find the dialect with the highest score ...
        DatabaseDialectProvider bestMatch = null;
        for (DatabaseDialectProvider provider : REGISTRY.values()) {
            int score = provider.score(info);
            log.debug("Dialect {} scored {} against {}", provider, score, info);
            if (score > bestScore) {
                bestMatch = provider;
                bestScore = score;
            }
        }
        log.debug("Using dialect {} with score {} against {}", bestMatch, bestScore, info);
        return bestMatch.create(config);
    }

    /**
     * Get the dialect with the specified name. Note that the DatabaseDialect needs to be {@link DatabaseDialect#close()
     * closed}.
     *
     * @param dialectName the dialect name
     * @param config      the connector configuration used to create the dialect; may not be null
     * @return the {@link DatabaseDialect} instance with the greatest score; never null, but possibly the {@link
     * DatabaseDialect default DatabaseDialect}
     * @throws ConnectException if the dialect could not be found
     */
    public static DatabaseDialect create(
            final String dialectName,
            final AbstractConfig config
    ) throws ConnectException {
        log.debug("Looking for named dialect '{}'", dialectName);
        Set<String> dialectNames = new HashSet<>();
        for (DatabaseDialectProvider provider : REGISTRY.values()) {
            dialectNames.add(provider.dialectName());
            if (provider.dialectName().equals(dialectName)) {
                return provider.create(config);
            }
        }
        for (DatabaseDialectProvider provider : REGISTRY.values()) {
            if (provider.dialectName().equalsIgnoreCase(dialectName)) {
                return provider.create(config);
            }
        }
        throw new ConnectException(
                "Unable to find dialect with name '" + dialectName + "' in the available dialects: "
                        + dialectNames
        );
    }

    static DatabaseDialectProvider.JdbcUrlInfo extractJdbcUrlInfo(final String url) {
        Matcher matcher = PROTOCOL_PATTERN.matcher(url);
        if (matcher.matches()) {
            return new JdbcUrlDetails(matcher.group(1), matcher.group(2), url);
        }
        throw new ConnectException("Not a valid JDBC URL: " + url);
    }

    /**
     * Return a copy of all of the available dialect providers.
     *
     * @return a set that contains all registered dialect providers; never null
     */
    public static Collection<DatabaseDialectProvider> registeredDialectProviders() {
        return new HashSet<>(REGISTRY.values());
    }

    /**
     * Return the names of all of the available dialects.
     *
     * @return the dialect names; never null
     */
    public static Collection<String> registeredDialectNames() {
        return REGISTRY.values()
                .stream()
                .map(DatabaseDialectProvider::dialectName)
                .collect(Collectors.toSet());
    }

    static class JdbcUrlDetails implements DatabaseDialectProvider.JdbcUrlInfo {

        private String subprotocol;

        private String subname;

        private String url;

        JdbcUrlDetails(
            final String subprotocol,
            final String subname,
            final String url
        ) {
            this.subprotocol = subprotocol;
            this.subname = subname;
            this.url = url;
        }

        @Override
        public String subprotocol() {
            return subprotocol;
        }

        @Override
        public String subname() {
            return subname;
        }

        @Override
        public String url() {
            return url;
        }

        @Override
        public String toString() {
            return "JDBC subprotocol '" + subprotocol + "' and source '" + url + "'";
        }
    }
}
