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

package org.lareferencia.core.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.lareferencia.core.embedding.chunks.ChunkingService;

/**
 * Unit tests for {@link ChunkingService} chunk generation behavior.
 *
 * <p>
 * The suite validates formatting, boundary constraints, overlap expectations,
 * and null/blank input handling with deterministic synthetic abstracts.
 * </p>
 */
class ChunkingServiceTest {

  private static final int TEST_MAX_CHUNK_SIZE = 128;
  private static final int TEST_MAX_OVERLAP_SIZE = 0;
  private static final int TEST_MAX_CHUNKS_SIZE = 5;

  private static final Pattern WORD_PATTERN = Pattern.compile("\\s+");

  private final ChunkingService chunkingService = new ChunkingService(TEST_MAX_CHUNK_SIZE, TEST_MAX_OVERLAP_SIZE,
      TEST_MAX_CHUNKS_SIZE);

  /**
   * Verifies that short abstracts are kept in a single chunk with exact
   * title-fragment formatting.
   */
  @Test
  void shouldFormatSingleChunkExactlyForShortAbstract() {
    String title = "Transformer Architectures for Multilingual Repositories";
    String abstractText = "This paper presents a semantic metadata alignment strategy for institutional repositories.";

    List<String> chunks = chunkingService.chunkTitleAndAbstract(title, abstractText);

    assertEquals(1, chunks.size());
    assertEquals(
        title + "\n" + abstractText,
        chunks.get(0));
  }

  /**
   * Verifies that long abstracts are split into multiple chunks and keep the
   * required title prefix template.
   */
  @Test
  void shouldSplitLongAbstractIntoExpectedChunkRangeAndPreserveTemplate() {
    String title = "Semantic Interoperability in Regional Open Access Networks";
    String abstractText = buildLongAbstract(4, 65);

    List<String> chunks = chunkingService.chunkTitleAndAbstract(title, abstractText);

    assertTrue(chunks.size() >= 2, "Expected long abstract to produce multiple chunks");
    assertTrue(chunks.size() <= TEST_MAX_CHUNKS_SIZE,
        "Expected chunk count to respect configured upper bound");

    for (String chunk : chunks) {
      assertTrue(chunk.startsWith(title + "\n"));
      assertFalse(chunk.substring((title + "\n").length()).isBlank());
    }
  }

  /**
   * Verifies that overlap between consecutive fragments respects the configured
   * overlap upper bound.
   */
  @Test
  void shouldRespectConfiguredOverlapBoundaryBetweenConsecutiveChunks() {
    String title = "Cross-Repository Semantic Similarity";
    String abstractText = buildSingleSentenceAbstract(320);

    List<String> chunks = chunkingService.chunkTitleAndAbstract(title, abstractText);
    assertTrue(chunks.size() >= 2, "Test requires at least two chunks to evaluate overlap");

    for (int i = 0; i < chunks.size() - 1; i++) {
      String currentFragment = extractFragment(chunks.get(i));
      String nextFragment = extractFragment(chunks.get(i + 1));

      List<String> currentWords = words(currentFragment);
      List<String> nextWords = words(nextFragment);

      List<String> currentTail = currentWords.subList(Math.max(0, currentWords.size() - 30), currentWords.size());
      Set<String> nextSet = nextWords.stream().collect(Collectors.toSet());

      long sharedCount = currentTail.stream().filter(nextSet::contains).count();

      assertTrue(sharedCount <= TEST_MAX_OVERLAP_SIZE,
          "Observed overlap should not exceed configured overlap boundary");
    }
  }

  /**
   * Verifies that chunk generation is capped by the configured maximum chunk
   * count.
   */
  @Test
  void shouldRespectConfiguredMaxChunksLimit() {
    ChunkingService limitedChunkingService = new ChunkingService(TEST_MAX_CHUNK_SIZE, TEST_MAX_OVERLAP_SIZE, 2);
    String title = "Chunk limit test";
    String abstractText = buildLongAbstract(6, 80);

    List<String> chunks = limitedChunkingService.chunkTitleAndAbstract(title, abstractText);
    assertTrue(chunks.size() <= 2, "Expected chunk count to be capped by configured max chunks");
  }

  /**
   * Verifies null and blank values for title/abstract return an empty result.
   */
  @Test
  void shouldHandleNullOrBlankInputsGracefully() {
    assertTrue(chunkingService.chunkTitleAndAbstract(null, "valid").isEmpty());
    assertTrue(chunkingService.chunkTitleAndAbstract(" ", "valid").isEmpty());
    assertTrue(chunkingService.chunkTitleAndAbstract("valid", null).isEmpty());
    assertTrue(chunkingService.chunkTitleAndAbstract("valid", " \n\t ").isEmpty());
  }

  /**
   * Extracts the fragment part from a formatted chunk.
   *
   * @param chunk chunk in title + newline + fragment format
   * @return fragment text after the first newline
   */
  private static String extractFragment(String chunk) {
    int index = chunk.indexOf('\n');
    return chunk.substring(index + 1);
  }

  /**
   * Tokenizes text using whitespace and strips non-alphanumeric separators from
   * each token.
   *
   * @param text source text
   * @return normalized token list used for overlap checks
   */
  private static List<String> words(String text) {
    return Arrays.stream(WORD_PATTERN.split(text.trim()))
        .filter(token -> !token.isBlank())
        .map(token -> token.replaceAll("[^a-zA-Z0-9_-]", ""))
        .filter(token -> !token.isBlank())
        .toList();
  }

  /**
   * Builds a synthetic multi-sentence abstract for chunking tests.
   *
   * @param sentenceCount    number of sentences to generate
   * @param wordsPerSentence number of numbered tokens to inject per sentence
   * @return generated abstract text
   */
  private static String buildLongAbstract(int sentenceCount, int wordsPerSentence) {
    StringBuilder sb = new StringBuilder();

    int token = 1;
    for (int sentence = 1; sentence <= sentenceCount; sentence++) {
      sb.append("Sentence ").append(sentence).append(" discusses semantic indexing relevance ");

      for (int word = 1; word <= wordsPerSentence; word++) {
        sb.append("token").append(token++).append(' ');
      }

      sb.append("for regional repositories and multilingual discovery.");
      if (sentence < sentenceCount) {
        sb.append(' ');
      }
    }

    return sb.toString();
  }

  /**
   * Builds a single long sentence with numbered tokens.
   *
   * @param words number of tokens to generate
   * @return generated sentence ending with a period
   */
  private static String buildSingleSentenceAbstract(int words) {
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= words; i++) {
      sb.append("token").append(i);
      if (i < words) {
        sb.append(' ');
      }
    }
    sb.append('.');
    return sb.toString();
  }
}
