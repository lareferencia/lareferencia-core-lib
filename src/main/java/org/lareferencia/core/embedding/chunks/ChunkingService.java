/*
 *   Copyright (c) 2013-2026. LA Referencia / Red CLARA and others
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU Affero General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU Affero General Public License for more details.
 *
 *   You should have received a copy of the GNU Affero General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *   This file is part of LA Referencia software platform LRHarvester v5.x
 *   For any further information please contact Lautaro Matas <lmatas@gmail.com>
 */

package org.lareferencia.core.embedding.chunks;

import java.text.Normalizer;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.document.DocumentSplitter;
import dev.langchain4j.data.document.splitter.DocumentSplitters;
import dev.langchain4j.data.segment.TextSegment;

/**
 * Service responsible for normalizing and splitting textual metadata into
 * embedding-ready chunks.
 *
 * <p>
 * The resulting chunks follow a title + fragment template so each segment
 * preserves the original document context during vectorization.
 * </p>
 */
@Service
public class ChunkingService {
  private final DocumentSplitter documentSplitter;
  private final int maxChunksSize;

  /**
   * Builds a chunking service with token-aware limits.
   *
   * @param maxChunkSize   maximum token budget for a generated fragment
   * @param maxOverlapSize overlap budget in tokens between consecutive fragments
   * @param maxChunksSize  maximum number of chunks returned by
   *                       {@link #chunkTitleAndAbstract(String, String)}
   */
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

  /**
   * Applies lightweight text normalization suitable for chunking and
   * embedding.
   *
   * @param t input text, potentially null or blank
   * @return normalized text, or an empty string when the input is null/blank
   */
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
   * Splits a document abstract into chunks and prefixes each fragment with the
   * normalized title.
   *
   * <p>
   * The splitter first tries to preserve larger structures (paragraphs and
   * lines) and recursively falls back to sentences, words, and characters when
   * needed to respect token limits.
   * </p>
   *
   * @param title        document title to prepend to each chunk
   * @param abstractText document abstract/body to split
   * @return a list of non-blank chunks formatted as title + newline + fragment,
   *         limited by {@code maxChunksSize}; returns an empty list for null or
   *         blank title/abstract inputs
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

  /**
   * Formats a chunk payload with title context.
   *
   * @param title    normalized title
   * @param fragment split abstract fragment
   * @return chunk text in two lines: title and fragment
   */
  private static String formatChunk(String title, String fragment) {
    return title + "\n" + fragment;
  }

}
