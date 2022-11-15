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

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class IdentifierRulesTest {

    private IdentifierRules rules;

    private List<String> parts;

    @Before
    public void beforeEach() {
        rules = IdentifierRules.DEFAULT;
    }

    @Test
    public void testParsingWithMultiCharacterQuotes() {
        rules = new IdentifierRules(".", "'''", "'''");
        assertParts("'''p1'''.'''p2'''.'''p3'''", "p1", "p2", "p3");
        assertParts("'''p1'''.'''p3'''", "p1", "p3");
        assertParts("'''p1'''", "p1");
        assertParts("'''p1.1.2.3'''", "p1.1.2.3");
        assertParts("'''p1.1.2.3.'''", "p1.1.2.3.");
        assertParts("", "");
        // unmatched quote
        assertParsingFailure("'''p1.p2");
        // ends with delim
        assertParsingFailure("'''p1'''.'''p3'''.");
    }

    @Test
    public void testParsingWithDifferentLeadingAndTrailingQuotes() {
        rules = new IdentifierRules(".", "[", "]");
        assertParts("[p1].[p2].[p3]", "p1", "p2", "p3");
        assertParts("[p1].[p3]", "p1", "p3");
        assertParts("[p1]", "p1");
        assertParts("[p1.1.2.3]", "p1.1.2.3");
        assertParts("[p1[.[1.[2.3]", "p1[.[1.[2.3");
        assertParts("", "");
        // ends with delim
        assertParsingFailure("[p1].[p3].");
    }

    @Test
    public void testParsingWithSingleCharacterQuotes() {
        rules = new IdentifierRules(".", "'", "'");
        assertParts("'p1'.'p2'.'p3'", "p1", "p2", "p3");
        assertParts("'p1'.'p3'", "p1", "p3");
        assertParts("'p1'", "p1");
        assertParts("'p1.1.2.3'", "p1.1.2.3");
        assertParts("", "");
        // ends with delim
        assertParsingFailure("'p1'.'p3'.");
    }

    @Test
    public void testParsingWithoutQuotes() {
        rules = new IdentifierRules(".", "'", "'");
        assertParts("p1.p2.p3", "p1", "p2", "p3");
        assertParts("p1.p3", "p1", "p3");
        assertParts("p1", "p1");
        assertParts("", "");
        // ends with delim
        assertParsingFailure("'p1'.'p3'.");
        // ends with delim
        assertParsingFailure("p1.p3.");
    }

    @Test
    public void testParsingWithUnsupportedQuotes() {
        rules = new IdentifierRules(".", " ", " ");
        assertParts("p1.p2.p3", "p1", "p2", "p3");
        assertParts("p1.p3", "p1", "p3");
        assertParts("p1", "p1");
        assertParts("", "");
    }

    protected void assertParts(final String fqn, final String... expectedParts) {
        parts = rules.parseQualifiedIdentifier(fqn);
        assertEquals(expectedParts.length, parts.size());
        int index = 0;
        for (String expectedPart : expectedParts) {
            assertEquals(expectedPart, parts.get(index++));
        }
    }

    protected void assertParsingFailure(final String fqn) {
        try {
            parts = rules.parseQualifiedIdentifier(fqn);
            fail("expected parsing error");
        } catch (IllegalArgumentException e) {
            // success
        }
    }

}
