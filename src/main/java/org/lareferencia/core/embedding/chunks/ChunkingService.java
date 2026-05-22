package org.lareferencia.core.embedding.chunks;

import java.text.Normalizer;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.document.DocumentSplitter;
import dev.langchain4j.data.document.splitter.DocumentSplitters;
import dev.langchain4j.data.segment.TextSegment;

@Service
public class ChunkingService {
  private final DocumentSplitter documentSplitter;
  private final int maxChunksSize;

  public ChunkingService(
      @Value("${embedding.max.segment.size.tokens:128}") int maxChunkSize,
      @Value("${embedding.max.overlap.size.tokens:0}") int maxOverlapSize,
      @Value("${embedding.max.chunks.size:5}") int maxChunksSize) {
    CustomTokenCountEstimator customTokenCountEstimator = new CustomTokenCountEstimator();
    this.documentSplitter = DocumentSplitters.recursive(
        maxChunkSize,
        maxOverlapSize,
        customTokenCountEstimator);
    this.maxChunksSize = maxChunksSize;
  }

  public String normalizeText(String t) {
    if (t == null || t.isBlank()) {
      return "";
    }
    // Canonicalize Unicode so equivalent accented forms are represented
    // consistently.
    String n = Normalizer.normalize(t, Normalizer.Form.NFC);
    // Join words split by hyphen + line break (common in OCR/PDF extracted text).
    n = n.replaceAll("-\\s*\\n\\s*", "");
    // Collapse any whitespace sequence to a single space and trim edges.
    n = n.replaceAll("\\s+", " ").trim();
    return n;
  }

  /**
   * Recommended splitter for document title/abstract processing.
   * 
   * It first tries to split by paragraphs and packs as many as possible into each
   * segment. If a paragraph is still too large, it recursively splits by lines,
   * then
   * sentences, then words, and finally characters until the segment fits.
   *
   * Effective behavior is controlled by the configured splitter parameters:
   * DEFAULT_MAX_CHUNK_SIZE, DEFAULT_MAX_OVERLAP_SIZE, and
   * CustomTokenCountEstimator.
   */
  public List<String> chunkTitleAndAbstract(String title, String abstractText) {
    String normalizedTitle = normalizeText(title);
    String normalizedAbstract = normalizeText(abstractText);

    if (normalizedTitle.isBlank() || normalizedAbstract.isBlank()) {
      return List.of();
    }

    List<TextSegment> segments = documentSplitter.split(Document.from(normalizedAbstract));

    return segments.stream()
        .map(TextSegment::text)
        .filter(fragment -> !fragment.isBlank())
        .limit(maxChunksSize)
        .map(fragment -> formatChunk(normalizedTitle, fragment))
        .toList();
  }

  private static String formatChunk(String title, String fragment) {
    return title + "\n" + fragment;
  }

}
