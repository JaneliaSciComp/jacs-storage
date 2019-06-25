package org.janelia.jacsstorage.model.jacsstorage;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

class UsageDataHelper {

    private static final Logger LOG = LoggerFactory.getLogger(UsageDataHelper.class);

    static Number parseValue(String fieldName, String fieldValue, Function<String, Number> valueConverter) {
        if (StringUtils.isBlank(fieldValue)) {
            LOG.warn("Null or empty field '{}' -> '{}'", fieldName, fieldValue);
        } else {
            try {
                return valueConverter.apply(fieldValue);
            } catch (Exception e) {
                LOG.error("Invalid '{}' value: '{}'", fieldName, fieldValue, e);
            }
        }
        return null;
    }

    static Number addValues(Number v1, Number v2, Function<BigDecimal, Number> converter) {
        if (v1 != null && v2 != null) {
            return converter.apply(new BigDecimal(v1.toString()).add(new BigDecimal(v2.toString())));
        } else if (v1 != null) {
            return converter.apply(new BigDecimal(v1.toString()));
        } else if (v2 != null) {
            return converter.apply(new BigDecimal(v2.toString()));
        } else {
            return null;
        }
    }

    static Double minValue(Double v1, Double v2) {
        if (v1 != null && v2 != null) {
            return Math.min(v1, v2);
        } else if (v1 != null) {
            return v1;
        } else if (v2 != null) {
            return v2;
        } else {
            return null;
        }
    }

    static Double percentage(Number numerator, Number denominator) {
        if (numerator == null || denominator == null || denominator.doubleValue() == 0.) {
            return null;
        } else {
            return numerator.doubleValue() / denominator.doubleValue();
        }
    }

    static Integer compare(Number v1, Number v2) {
        if (v1 == null || v2 == null) {
            return null;
        } else if (v1.doubleValue() < v2.doubleValue()) {
            return -1;
        } else if (v1.doubleValue() > v2.doubleValue()) {
            return 1;
        } else {
            return 0;
        }
    }

}
