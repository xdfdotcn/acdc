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

package cn.xdf.acdc.connect.jdbc.util;

import java.util.ArrayList;
import java.util.List;

/**
 * The rules for how identifiers are parsed and quoted.
 */
public class IdentifierRules {

    public static final String UNSUPPORTED_QUOTE = " ";

    private static final String DEFAULT_QUOTE = "\"";

    private static final String DEFAULT_ID_DELIM = ".";

    public static final IdentifierRules DEFAULT = new IdentifierRules(DEFAULT_ID_DELIM, DEFAULT_QUOTE);

    private final String leadingQuoteString;

    private final String trailingQuoteString;

    private final String identifierDelimiter;

    /**
     * Create new identifier rules using the supplied quote string for both leading and trailing quotes, and the '{@link
     * #DEFAULT_ID_DELIM}' character for identifier delimiters.
     *
     * @param quoteString the string used for leading and trailing quotes; may be null if {@link #DEFAULT_QUOTE} is to
     *                    be used
     */
    public IdentifierRules(final String quoteString) {
        this(DEFAULT_ID_DELIM, quoteString, quoteString);
    }

    /**
     * Create new identifier rules using the supplied parameters.
     *
     * @param delimiter   the delimiter used within fully qualified names; may be null if {@link #DEFAULT_ID_DELIM} is
     *                    to be used
     * @param quoteString the string used for leading and trailing quotes; may be null if {@link #DEFAULT_QUOTE} is to
     *                    be used
     */
    public IdentifierRules(
            final String delimiter,
            final String quoteString
    ) {
        this(delimiter, quoteString, quoteString);
    }

    /**
     * Create new identifier rules using the supplied parameters.
     *
     * @param identifierDelimiter the delimiter used within fully qualified names; may be null if {@link
     *                            #DEFAULT_ID_DELIM} is to be used
     * @param leadingQuoteString  the string used for leading quotes; may be null if {@link #DEFAULT_QUOTE} is to be
     *                            used
     * @param trailingQuoteString the string used for leading quotes; may be null if {@link #DEFAULT_QUOTE} is to be
     *                            used
     */
    public IdentifierRules(
            final String identifierDelimiter,
            final String leadingQuoteString,
            final String trailingQuoteString
    ) {
        this.leadingQuoteString = leadingQuoteString != null ? leadingQuoteString : DEFAULT_QUOTE;
        this.trailingQuoteString = trailingQuoteString != null ? trailingQuoteString : DEFAULT_QUOTE;
        this.identifierDelimiter = identifierDelimiter != null ? identifierDelimiter : DEFAULT_ID_DELIM;
    }

    /**
     * Get the delimiter that is used to delineate segments within fully-qualified identifiers.
     *
     * @return the identifier delimiter; never null
     */
    public String identifierDelimiter() {
        return identifierDelimiter;
    }

    /**
     * Get the string used as a leading quote.
     *
     * @return the leading quote string; never null
     */
    public String leadingQuoteString() {
        return leadingQuoteString;
    }

    /**
     * Get the string used as a trailing quote.
     *
     * @return the trailing quote string; never null
     */
    public String trailingQuoteString() {
        return trailingQuoteString;
    }

    /**
     * Get an expression builder that uses these identifier rules.
     *
     * @return the new expression builder; never null
     */
    public ExpressionBuilder expressionBuilder() {
        return new ExpressionBuilder(this);
    }

    /**
     * Parse the unqualified or fully qualified name into its segments.
     *
     * @param fqn the unqualified or fully-qualified name; may not be null
     * @return the segments in the supplied name; never null, but possibly empty
     */
    public List<String> parseQualifiedIdentifier(final String fqn) {
        String orig = fqn;
        String tmpFqn = fqn;
        String delim = identifierDelimiter();
        String lead = leadingQuoteString();
        String trail = trailingQuoteString();
        List<String> parts = new ArrayList<>();
        int index;
        String segment;
        do {
            if (!lead.equals(UNSUPPORTED_QUOTE) && tmpFqn.startsWith(lead)) {
                int end = tmpFqn.indexOf(trail, lead.length());
                if (end < 0) {
                    throw new IllegalArgumentException(
                            "Failure parsing fully qualified identifier; missing trailing quote in " + orig);
                }
                segment = tmpFqn.substring(lead.length(), end);
                tmpFqn = tmpFqn.substring(end + trail.length());
                if (tmpFqn.startsWith(delim)) {
                    tmpFqn = tmpFqn.substring(delim.length());
                    fqlShouldNotEmpty(orig, tmpFqn, "Failure parsing fully qualified identifier; ends in delimiter ");
                }
            } else {
                index = tmpFqn.indexOf(delim, 0);
                if (index == -1) {
                    segment = tmpFqn;
                    tmpFqn = "";
                } else {
                    segment = tmpFqn.substring(0, index);
                    tmpFqn = tmpFqn.substring(index + delim.length());
                    fqlShouldNotEmpty(orig, tmpFqn, "Failure parsing fully qualified identifier; ends in delimiter ");
                }
            }
            parts.add(segment);
        } while (tmpFqn.length() > 0);
        return parts;
    }

    /**
     * fql should be not empty.
     *
     * @param origFqn      origFqn
     * @param fqn          fqn
     * @param exceptionMsg exceptionMsg
     */
    protected void fqlShouldNotEmpty(final String origFqn, final String fqn, final String exceptionMsg) {
        if (fqn.isEmpty()) {
            throw new IllegalArgumentException(exceptionMsg + origFqn);
        }
    }

    /**
     * Return a new IdentifierRules that escapes quotes with the specified prefix.
     *
     * @param prefix the prefix
     * @return the new IdentifierRules, or this builder if the prefix is null or empty
     */
    public IdentifierRules escapeQuotesWith(final String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            return this;
        }
        return new IdentifierRules(
                identifierDelimiter,
                prefix + leadingQuoteString,
                prefix + trailingQuoteString
        );
    }
}
