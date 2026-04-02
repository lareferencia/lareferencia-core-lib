package org.lareferencia.core.metadata;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.w3c.dom.Document;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("CERIF OpenAIRE to XOAI OpenAIRE transformer tests")
class CerifOpenaireToXoaiOpenaireTransformerTest {

    @Test
    @DisplayName("Publication fixture should transform to xoai_openaire with key fields")
    void publicationShouldTransformToXoaiOpenaire() throws Exception {
        XsltMDFormatTransformer transformer = createTransformer();
        Document source = loadXml("cerif_publication.xml");

        Document transformed = transformer.transform(source);
        OAIRecordMetadata metadata = new OAIRecordMetadata("oai:impactu.colav.co:0000000004", transformed);

        assertEquals(List.of("Open science from CERIF", "Ciencia abierta desde CERIF"),
            metadata.getFieldOcurrences("datacite.titles.title"));
        assertTrue(metadata.getFieldOcurrences("datacite.creators.creator.creatorName").contains("Perez, Ana Maria"));
        assertTrue(metadata.getFieldOcurrences("datacite.creators.creator.nameIdentifier").contains("0000-0002-1825-0097"));
        assertEquals(List.of("10.1234/impactu.42"), metadata.getFieldOcurrences("datacite.identifier"));
        assertEquals(List.of("DOI"), metadata.getFieldOcurrences("datacite.identifier:identifierType"));
        assertTrue(metadata.getFieldOcurrences("datacite.alternateIdentifiers.alternateIdentifier").contains("2049-3630"));
        assertTrue(metadata.getFieldOcurrences("datacite.alternateIdentifiers.alternateIdentifier").contains("https://impactu.example/publication/42"));
        assertTrue(metadata.getFieldOcurrences("datacite.relatedIdentifiers.relatedIdentifier").contains("1234-5678"));
        assertTrue(metadata.getFieldOcurrences("datacite.relatedIdentifiers.relatedIdentifier").contains("proj-77"));
        assertEquals(List.of("2025-03-12"), metadata.getFieldOcurrences("datacite.dates.date.Issued"));
        assertEquals(List.of("eng"), metadata.getFieldOcurrences("dc.language"));
        assertEquals(List.of("Impactu Press"), metadata.getFieldOcurrences("dc.publisher"));
        assertEquals(List.of("A publication harvested from a CERIF provider."), metadata.getFieldOcurrences("dc.description"));
        assertEquals(List.of("open access"), metadata.getFieldOcurrences("datacite.rights"));
        assertEquals(List.of("http://purl.org/coar/access_right/c_abf2"), metadata.getFieldOcurrences("datacite.rights:rightsURI"));
        assertEquals(List.of("journal article"), metadata.getFieldOcurrences("oaire.resourceType"));
        assertEquals(List.of("http://purl.org/coar/resource_type/c_6501"), metadata.getFieldOcurrences("oaire.resourceType:uri"));
        assertEquals(List.of("https://impactu.example/files/42.pdf"), metadata.getFieldOcurrences("oaire.files.file"));
        assertEquals(List.of("application/pdf"), metadata.getFieldOcurrences("oaire.files.file:mimeType"));
    }

    @Test
    @DisplayName("Publication without optional fields should still transform")
    void publicationWithoutOptionalFieldsShouldStillTransform() throws Exception {
        XsltMDFormatTransformer transformer = createTransformer();
        String xml = "<Publication xmlns=\"https://www.openaire.eu/cerif-profile/1.2/\">" +
            "<Title xml:lang=\"en\">Minimal CERIF publication</Title>" +
            "<PublicationDate>2025</PublicationDate>" +
            "<Authors><Author><Person><PersonName><FirstNames>Ana</FirstNames><FamilyNames>Perez</FamilyNames></PersonName></Person></Author></Authors>" +
            "</Publication>";

        Document transformed = transformer.transform(MedatadaDOMHelper.XMLString2Document(xml));
        OAIRecordMetadata metadata = new OAIRecordMetadata("oai:test:minimal", transformed);

        assertEquals(List.of("Minimal CERIF publication"), metadata.getFieldOcurrences("datacite.titles.title"));
        assertEquals(List.of("Perez, Ana"), metadata.getFieldOcurrences("datacite.creators.creator.creatorName"));
        assertEquals(List.of("2025"), metadata.getFieldOcurrences("datacite.dates.date.Issued"));
        assertTrue(metadata.getFieldOcurrences("datacite.identifier").isEmpty());
        assertTrue(metadata.getFieldOcurrences("dc.description").isEmpty());
        assertTrue(metadata.getFieldOcurrences("oaire.files.file").isEmpty());
    }

    @Test
    @DisplayName("Non publication roots should not fail and should remain empty")
    void nonPublicationRootsShouldNotFail() throws Exception {
        XsltMDFormatTransformer transformer = createTransformer();
        Document source = loadXml("cerif_person.xml");

        Document transformed = transformer.transform(source);
        OAIRecordMetadata metadata = new OAIRecordMetadata("oai:test:person", transformed);

        assertEquals("metadata", transformed.getDocumentElement().getLocalName());
        assertTrue(metadata.getFieldOcurrences("datacite.titles.title").isEmpty());
        assertTrue(metadata.getFieldOcurrences("oaire.resourceType").isEmpty());
    }

    private XsltMDFormatTransformer createTransformer() {
        return new XsltMDFormatTransformer("oai_cerif_openaire", "xoai_openaire", resolveAppFile(
            "config/mdfcrosswalks/oai_cerif_openaire2xoai_openaire.xsl").toString());
    }

    private Document loadXml(String resourceName) throws Exception {
        Path path = new ClassPathResource(resourceName).getFile().toPath();
        return MedatadaDOMHelper.XMLString2Document(Files.readString(path));
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
