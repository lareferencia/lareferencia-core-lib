package org.lareferencia.core.worker.harvesting;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.lareferencia.core.metadata.MDFormatTransformerService;
import org.lareferencia.core.metadata.MedatadaDOMHelper;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.metadata.XsltMDFormatTransformer;
import org.lareferencia.core.util.date.DateHelper;
import org.lareferencia.core.util.date.SystemDateFormatter;
import org.lareferencia.core.util.date.YearDateFormatter;
import org.lareferencia.core.util.date.YearMonthDateFormatter;
import org.lareferencia.core.util.date.YearMonthDayDateFormatter;
import org.lareferencia.core.util.date.YearMonthDayHourDateFormatter;
import org.oclc.oai.harvester2.verb.ListRecords;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.util.ReflectionTestUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("OCLCBasedHarvesterImpl tests")
class OCLCBasedHarvesterImplTest {

    @Test
    @DisplayName("ListRecords without setSpec should keep processing and store null setSpec")
    void listRecordsWithoutSetSpecShouldStoreNullSetSpec() throws Exception {
        OCLCBasedHarvesterImpl harvester = createHarvester(null);
        ListRecords listRecords = mockListRecords("oai_listrecords_cerif_publication.xml");

        HarvestingEvent result = invokeCreateResult(harvester, listRecords, "oai_cerif_openaire", "oai_cerif_openaire");

        assertFalse(result.isRecordMissing());
        assertEquals(1, result.getRecords().size());
        OAIRecordMetadata record = result.getRecords().get(0);
        assertNull(record.getSetSpec());
        assertEquals("oai:impactu.colav.co:0000000004", record.getIdentifier());
        assertEquals("oai_cerif_openaire", record.getStoreSchema());
    }

    @Test
    @DisplayName("Records without metadata should still be marked as missing")
    void missingMetadataShouldStillMarkRecordMissing() throws Exception {
        OCLCBasedHarvesterImpl harvester = createHarvester(null);
        ListRecords listRecords = mockListRecords("oai_listrecords_missing_metadata.xml");

        HarvestingEvent result = invokeCreateResult(harvester, listRecords, "oai_cerif_openaire", "oai_cerif_openaire");

        assertTrue(result.isRecordMissing());
        assertTrue(result.getRecords().isEmpty());
        assertEquals(List.of("oai:impactu.colav.co:missing"), result.getMissingRecordsIdentifiers());
    }

    @Test
    @DisplayName("CERIF publication harvesting should transform metadata to xoai_openaire")
    void cerifPublicationHarvestingShouldTransformToXoaiOpenaire() throws Exception {
        MDFormatTransformerService service = new MDFormatTransformerService();
        service.setTransformers(List.of(new XsltMDFormatTransformer(
            "oai_cerif_openaire",
            "xoai_openaire",
            resolveAppFile("config/mdfcrosswalks/oai_cerif_openaire2xoai_openaire.xsl").toString()
        )));

        OCLCBasedHarvesterImpl harvester = createHarvester(service);
        ListRecords listRecords = mockListRecords("oai_listrecords_cerif_publication.xml");

        HarvestingEvent result = invokeCreateResult(harvester, listRecords, "oai_cerif_openaire", "xoai_openaire");

        assertEquals(1, result.getRecords().size());
        OAIRecordMetadata record = result.getRecords().get(0);

        assertEquals("xoai_openaire", record.getStoreSchema());
        assertNull(record.getSetSpec());
        assertEquals(List.of("Open science from CERIF"), record.getFieldOcurrences("datacite.titles.title"));
        assertEquals(List.of("Perez, Ana Maria"), record.getFieldOcurrences("datacite.creators.creator.creatorName"));
        assertEquals(List.of("10.1234/impactu.42"), record.getFieldOcurrences("datacite.identifier"));
        assertEquals(List.of("journal article"), record.getFieldOcurrences("oaire.resourceType"));
        assertEquals(List.of("https://impactu.example/files/42.pdf"), record.getFieldOcurrences("oaire.files.file"));
    }

    private HarvestingEvent invokeCreateResult(OCLCBasedHarvesterImpl harvester, ListRecords listRecords,
                                               String metadataPrefix, String metadataStoreSchema) {
        return ReflectionTestUtils.invokeMethod(
            harvester,
            "createResultFromListRecords",
            new HarvestingEvent(),
            listRecords,
            "https://oai.impactu.colav.co/oai",
            metadataPrefix,
            metadataStoreSchema
        );
    }

    private OCLCBasedHarvesterImpl createHarvester(MDFormatTransformerService service) {
        OCLCBasedHarvesterImpl harvester = new OCLCBasedHarvesterImpl();

        DateHelper dateHelper = new DateHelper();
        dateHelper.setDateTimeFormatters(Set.of(
            new SystemDateFormatter(),
            new YearMonthDayDateFormatter(),
            new YearMonthDateFormatter(),
            new YearMonthDayHourDateFormatter(),
            new YearDateFormatter()
        ));

        ReflectionTestUtils.setField(harvester, "dateHelper", dateHelper);
        ReflectionTestUtils.setField(harvester, "trfService", service);
        return harvester;
    }

    private ListRecords mockListRecords(String resourceName) throws Exception {
        Document document = loadDocument(resourceName);
        ListRecords listRecords = mock(ListRecords.class);

        when(listRecords.getSchemaLocation()).thenReturn(ListRecords.SCHEMA_LOCATION_V2_0);
        when(listRecords.getNodeList(anyString())).thenReturn(
            document.getElementsByTagNameNS("http://www.openarchives.org/OAI/2.0/", "record"));
        when(listRecords.getDocument()).thenReturn(document);
        when(listRecords.getSingleString(any(Node.class), anyString())).thenAnswer(invocation ->
            evaluateXPath((Node) invocation.getArgument(0), invocation.getArgument(1, String.class)));

        return listRecords;
    }

    private Document loadDocument(String resourceName) throws Exception {
        Path path = new ClassPathResource(resourceName).getFile().toPath();
        String xml = Files.readString(path);
        return MedatadaDOMHelper.XMLString2Document(xml);
    }

    private String evaluateXPath(Node node, String expression) throws Exception {
        XPath xpath = XPathFactory.newInstance().newXPath();
        xpath.setNamespaceContext(new NamespaceContext() {
            @Override
            public String getNamespaceURI(String prefix) {
                if ("oai20".equals(prefix)) {
                    return "http://www.openarchives.org/OAI/2.0/";
                }
                if ("xml".equals(prefix)) {
                    return XMLConstants.XML_NS_URI;
                }
                return XMLConstants.NULL_NS_URI;
            }

            @Override
            public String getPrefix(String namespaceURI) {
                return null;
            }

            @Override
            public java.util.Iterator<String> getPrefixes(String namespaceURI) {
                return List.<String>of().iterator();
            }
        });

        String value = (String) xpath.evaluate(expression, node, XPathConstants.STRING);
        return value == null ? "" : value;
    }

    private Path resolveAppFile(String relativePath) {
        Path cwd = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
        Path candidate = cwd.resolve(relativePath).normalize();
        if (Files.exists(candidate)) {
            return candidate;
        }

        candidate = cwd.resolve("../lareferencia-lrharvester-app").resolve(relativePath).normalize();
        if (Files.exists(candidate)) {
            return candidate;
        }

        candidate = cwd.resolve("lareferencia-lrharvester-app").resolve(relativePath).normalize();
        if (Files.exists(candidate)) {
            return candidate;
        }

        throw new IllegalStateException("Unable to resolve app resource: " + relativePath + " from " + cwd);
    }
}
