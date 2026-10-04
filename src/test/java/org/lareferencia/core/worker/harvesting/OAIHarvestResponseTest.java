package org.lareferencia.core.worker.harvesting;

import static org.junit.jupiter.api.Assertions.*;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.lareferencia.core.util.date.DateHelper;
import org.lareferencia.core.util.date.YearMonthDayDateFormatter;
import org.springframework.test.util.ReflectionTestUtils;

class OAIHarvestResponseTest {
    @Test void lastPageWithoutTokenPreservesDeletedHeaderDateAndStops() throws Exception {
        var requests = new AtomicInteger();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/oai",exchange -> {
            requests.incrementAndGet();
            byte[] response = """
                    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
                      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                      xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
                      <responseDate>2026-01-03T00:00:00Z</responseDate>
                      <request verb="ListRecords" metadataPrefix="oai_dc">http://localhost/oai</request>
                      <ListRecords><record><header status="deleted">
                        <identifier>oai:fixture:deleted</identifier><datestamp>2026-01-02</datestamp>
                      </header></record></ListRecords>
                    </OAI-PMH>
                    """.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type","text/xml");
            exchange.sendResponseHeaders(200,response.length); exchange.getResponseBody().write(response); exchange.close();
        });
        server.start();
        try {
            var harvester = new OCLCBasedHarvesterImpl(); var dates = new DateHelper();
            dates.setDateTimeFormatters(Set.of(new YearMonthDayDateFormatter()));
            ReflectionTestUtils.setField(harvester,"dateHelper",dates);
            var events = new AtomicInteger();
            harvester.addEventListener(event -> {
                assertEquals(HarvestingEventStatus.OK,event.getStatus());
                assertEquals(LocalDateTime.of(2026,1,2,0,0),event.getDeletedRecordsDatestamps().get("oai:fixture:deleted"));
                events.incrementAndGet();
            });
            harvester.harvest("http://127.0.0.1:"+server.getAddress().getPort()+"/oai",null,"oai_dc","oai_dc","2026-01-01",null,null,1);
            assertEquals(1,requests.get()); assertEquals(1,events.get());
        } finally { server.stop(0); }
    }
}
