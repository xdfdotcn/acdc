package cn.xdf.acdc.devops.core.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.assertj.core.api.Assertions;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.TypeSafeMatcher;
import org.springframework.format.datetime.standard.DateTimeFormatterRegistrar;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.format.support.FormattingConversionService;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;

/**
 * Utility class for testing REST controllers.
 */
public final class TestUtil {
    
    private static final ObjectMapper MAPPER = createObjectMapper();
    
    private TestUtil() {
    }
    
    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS, false);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }
    
    /**
     * Convert an object to JSON byte array.
     *
     * @param object the object to convert.
     * @return the JSON byte array.
     * @throws IOException exception
     */
    public static byte[] convertObjectToJsonBytes(final Object object) throws IOException {
        return MAPPER.writeValueAsBytes(object);
    }
    
    /**
     * Create a byte array with a specific size filled with specified data.
     *
     * @param size the size of the byte array.
     * @param data the data to put in the byte array.
     * @return the JSON byte array.
     */
    public static byte[] createByteArray(final int size, final String data) {
        byte[] byteArray = new byte[size];
        for (int i = 0; i < size; i++) {
            byteArray[i] = Byte.parseByte(data, 2);
        }
        return byteArray;
    }
    
    /**
     * Creates a matcher that matches when the examined string represents the same instant as the reference datetime.
     *
     * @param date the reference datetime against which the examined string is checked.
     * @return matcher
     */
    public static ZonedDateTimeMatcher sameInstant(final ZonedDateTime date) {
        return new ZonedDateTimeMatcher(date);
    }
    
    /**
     * Creates a matcher that matches when the examined number represents the same value as the reference BigDecimal.
     *
     * @param number the reference BigDecimal against which the examined number is checked.
     * @return matcher
     */
    public static NumberMatcher sameNumber(final BigDecimal number) {
        return new NumberMatcher(number);
    }
    
    /**
     * Verifies the equals/hashcode contract on the domain object.
     *
     * @param clazz clazz
     * @param <T> T
     * @throws Exception exception
     */
    public static <T> void equalsVerifier(final Class<T> clazz) throws Exception {
        T domainObject1 = clazz.getConstructor().newInstance();
        Assertions.assertThat(domainObject1.toString()).isNotNull();
        Assertions.assertThat(domainObject1).isEqualTo(domainObject1);
        Assertions.assertThat(domainObject1).hasSameHashCodeAs(domainObject1);
        // Test with an instance of another class
        Object testOtherObject = new Object();
        Assertions.assertThat(domainObject1).isNotEqualTo(testOtherObject);
        Assertions.assertThat(domainObject1).isNotEqualTo(null);
        // Test with an instance of the same class
        T domainObject2 = clazz.getConstructor().newInstance();
        Assertions.assertThat(domainObject1).isNotEqualTo(domainObject2);
        // HashCodes are equals because the objects are not persisted yet
        Assertions.assertThat(domainObject1).hasSameHashCodeAs(domainObject2);
    }
    
    /**
     * Create a {@link FormattingConversionService} which use ISO date format, instead of the localized one.
     *
     * @return the {@link FormattingConversionService}.
     */
    public static FormattingConversionService createFormattingConversionService() {
        DefaultFormattingConversionService dfcs = new DefaultFormattingConversionService();
        DateTimeFormatterRegistrar registrar = new DateTimeFormatterRegistrar();
        registrar.setUseIsoFormat(true);
        registrar.registerFormatters(dfcs);
        return dfcs;
    }
    
    /**
     * Makes a an executes a query to the EntityManager finding all stored objects.
     *
     * @param <T> The type of objects to be searched
     * @param em The instance of the EntityManager
     * @param clss The class type to be searched
     * @return A list of all found objects
     */
    public static <T> List<T> findAll(final EntityManager em, final Class<T> clss) {
        CriteriaBuilder cb = em.getCriteriaBuilder();
        CriteriaQuery<T> cq = cb.createQuery(clss);
        Root<T> rootEntry = cq.from(clss);
        CriteriaQuery<T> all = cq.select(rootEntry);
        TypedQuery<T> allQuery = em.createQuery(all);
        return allQuery.getResultList();
    }
    
    /**
     * A matcher that tests that the examined string represents the same instant as the reference datetime.
     */
    public static class ZonedDateTimeMatcher extends TypeSafeDiagnosingMatcher<String> {
        
        private final ZonedDateTime date;
        
        public ZonedDateTimeMatcher(final ZonedDateTime date) {
            this.date = date;
        }
        
        @Override
        protected boolean matchesSafely(final String item, final Description mismatchDescription) {
            try {
                if (!date.isEqual(ZonedDateTime.parse(item))) {
                    mismatchDescription.appendText("was ").appendValue(item);
                    return false;
                }
                return true;
            } catch (DateTimeParseException e) {
                mismatchDescription.appendText("was ").appendValue(item).appendText(", which could not be parsed as a ZonedDateTime");
                return false;
            }
        }
        
        @Override
        public void describeTo(final Description description) {
            description.appendText("a String representing the same Instant as ").appendValue(date);
        }
    }
    
    /**
     * A matcher that tests that the examined number represents the same value - it can be Long, Double, etc - as the reference BigDecimal.
     */
    public static class NumberMatcher extends TypeSafeMatcher<Number> {
        
        private final BigDecimal value;
        
        public NumberMatcher(final BigDecimal value) {
            this.value = value;
        }
        
        @Override
        public void describeTo(final Description description) {
            description.appendText("a numeric value is ").appendValue(value);
        }
        
        @Override
        protected boolean matchesSafely(final Number item) {
            BigDecimal bigDecimal = asDecimal(item);
            return bigDecimal != null && value.compareTo(bigDecimal) == 0;
        }
        
        private static BigDecimal asDecimal(final Number item) {
            if (item == null) {
                return null;
            }
            if (item instanceof BigDecimal) {
                return (BigDecimal) item;
            } else if (item instanceof Long) {
                return BigDecimal.valueOf((Long) item);
            } else if (item instanceof Integer) {
                return BigDecimal.valueOf((Integer) item);
            } else if (item instanceof Double) {
                return BigDecimal.valueOf((Double) item);
            } else if (item instanceof Float) {
                return BigDecimal.valueOf((Float) item);
            } else {
                return BigDecimal.valueOf(item.doubleValue());
            }
        }
    }
}
