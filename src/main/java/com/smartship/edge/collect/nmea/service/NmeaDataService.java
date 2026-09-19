package com.smartship.edge.collect.nmea.service;

import com.smartship.edge.collect.nmea.parser.NmeaParser;
import com.smartship.edge.config.EdgeProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class NmeaDataService {

    private final NmeaParser nmeaParser;
    private final EdgeProperties properties;

    public void processSentence(String sentence, String source) {
        log.debug("[NMEA-RECV] 来源: {} 报文: {}", source, sentence);
        nmeaParser.parse(sentence, source);
    }

    public String getCurrentMmsi() {
        return properties.getMmsi();
    }
}
