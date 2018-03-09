package org.janelia.jacsstorage.model.jacsstorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

class UsageDataHelper {

    private static final Logger LOG = LoggerFactory.getLogger(UsageDataHelper.class);

    static Number parseValue(String fieldName, String fieldValue, Function<String, Number> valueConverter) {
        try {
            return valueConverter.apply(fieldValue);
        } catch (Exception e) {
            LOG.warn("Invalid '{}' value: {}", fieldName, fieldValue, e);
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

    static Optional<Double> percentage(Number numerator, Number denominator) {
        if (numerator == null || denominator == null || denominator.doubleValue() == 0.) {
            return Optional.empty();
        } else {
            return Optional.of(numerator.doubleValue() / denominator.doubleValue());
        }
    }

    static Optional<Integer> compare(Number v1, Number v2) {
        if (v1 == null || v2 == null) {
            return Optional.empty();
        } else if (v1.doubleValue() < v2.doubleValue()) {
            return Optional.of(-1);
        } else if (v1.doubleValue() > v2.doubleValue()) {
            return Optional.of(1);
        } else {
            return Optional.of(0);
        }
    }
}
