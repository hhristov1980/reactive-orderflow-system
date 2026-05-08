package com.order.infrastructure.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.r2dbc.convert.R2dbcCustomConversions;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

public class R2dbcConverterConfig {

    @Value("${TIMEZONE:UTC}")
    private String timezoneOffset;

    @Bean
    public R2dbcCustomConversions r2dbcCustomConversions(){
        return new R2dbcCustomConversions(R2dbcCustomConversions.STORE_CONVERSIONS,
                List.of(new LocalDateTimeToOffsetDateTimeConverter(timezoneOffset)));
    }

    @ReadingConverter
    public static class LocalDateTimeToOffsetDateTimeConverter implements Converter<LocalDateTime, OffsetDateTime> {

        private final ZoneOffset zoneOffset;

        public LocalDateTimeToOffsetDateTimeConverter(String timezoneOffset){
            this.zoneOffset = "UTC".equalsIgnoreCase(timezoneOffset)
                    ? ZoneOffset.UTC
                    : ZoneOffset.of(timezoneOffset);
        }
        @Override
        public OffsetDateTime convert(LocalDateTime source) {
            return source.atOffset(zoneOffset);
        }
    }
}
