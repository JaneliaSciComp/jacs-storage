package org.janelia.jacsstorage.dao.mongo.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.util.Date;

public class ISODateDeserializer extends JsonDeserializer<Date> {

    @Override
    public Date deserialize(JsonParser jsonParser, DeserializationContext ctxt) throws IOException {
        JsonNode node = jsonParser.readValueAsTree();
        String dateValue;
        if (node.has("$date")) {
            dateValue = node.get("$date").asText();
        } else {
            dateValue = node.asText();
        }
        Date date = null;
        if (StringUtils.isNotBlank(dateValue)) {
            if (dateValue.contains("T"))
                if (dateValue.indexOf('.') == -1) {
                    return ISODateTimeFormat.dateTimeNoMillis().parseDateTime(dateValue).toDate();
                } else {
                    return ISODateTimeFormat.dateTime().parseDateTime(dateValue).toDate();
                }
            else
                return new Date(Long.parseLong(dateValue));
        }
        return date;
    }
}
