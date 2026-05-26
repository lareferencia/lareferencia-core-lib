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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.stereotype.Component;

import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.model.TokenCountEstimator;

/**
 * Lightweight estimator designed for semantic chunking when a model tokenizer
 * is unavailable.
 *
 * <p>
 * The strategy blends lexical units, punctuation and CJK-aware character
 * counts to approximate token budgets for splitter decisions.
 * </p>
 */
@Component
public class CustomTokenCountEstimator implements TokenCountEstimator {

    private static final Pattern WORD_OR_NUMBER = Pattern.compile("[\\p{L}\\p{N}]+(?:['’-][\\p{L}\\p{N}]+)*");
    private static final Pattern PUNCTUATION = Pattern.compile("[\\p{Punct}]");

    /**
     * Estimates token count for plain text.
     *
     * @param text source text
     * @return estimated token count; returns 0 for null/blank input
     */
    @Override
    public int estimateTokenCountInText(String text) {
        if (text == null || text.isBlank()) {
            return 0;
        }

        int wordsAndNumbers = countMatches(WORD_OR_NUMBER, text);
        int punctuation = countMatches(PUNCTUATION, text);
        int cjkChars = countCjkCodePoints(text);

        // Approximation tuned for transformer BPE tokenization:
        // - base lexical units
        // - punctuation contributes as standalone tokens in many vocabularies
        // - CJK scripts typically split into more granular units
        double estimate = (wordsAndNumbers * 1.10d) + (punctuation * 0.35d) + (cjkChars * 0.65d);

        int count = Math.max(1, (int) Math.ceil(estimate));
        return count;
    }

    /**
     * Estimates token count for supported LangChain4j message types.
     *
     * @param message chat message to inspect
     * @return estimated token count; returns 0 for null message
     * @throws IllegalArgumentException if the message type is not recognized by
     *                                  this estimator
     */
    @Override
    public int estimateTokenCountInMessage(ChatMessage message) {
        if (message == null) {
            return 0;
        }

        if (message instanceof dev.langchain4j.data.message.SystemMessage systemMessage) {
            return estimateTokenCountInText(systemMessage.text());
        } else if (message instanceof dev.langchain4j.data.message.UserMessage userMessage) {
            return estimateTokenCountInText(userMessage.singleText());
        } else if (message instanceof dev.langchain4j.data.message.AiMessage aiMessage) {
            return estimateTokenCountInText(aiMessage.text());
        } else if (message instanceof dev.langchain4j.data.message.ToolExecutionResultMessage toolExecutionResultMessage) {
            return estimateTokenCountInText(toolExecutionResultMessage.text());
        }

        throw new IllegalArgumentException("Unknown message type: " + message.getClass().getName());
    }

    /**
     * Aggregates token estimates for a collection of chat messages.
     *
     * @param messages iterable message collection
     * @return total estimated tokens across all messages
     */
    @Override
    public int estimateTokenCountInMessages(Iterable<ChatMessage> messages) {
        int tokens = 0;
        for (ChatMessage message : messages) {
            tokens += estimateTokenCountInMessage(message);
        }
        return tokens;
    }

    /**
     * Counts regex matches in a text.
     *
     * @param pattern compiled pattern to apply
     * @param text    source text
     * @return number of matches
     */
    private static int countMatches(Pattern pattern, String text) {
        Matcher matcher = pattern.matcher(text);
        int count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    /**
     * Counts code points that belong to CJK-related Unicode blocks.
     *
     * @param text source text
     * @return number of CJK code points detected in the text
     */
    private static int countCjkCodePoints(String text) {
        int count = 0;
        for (int i = 0; i < text.length();) {
            int codePoint = text.codePointAt(i);
            Character.UnicodeBlock block = Character.UnicodeBlock.of(codePoint);
            if (isCjkBlock(block)) {
                count++;
            }
            i += Character.charCount(codePoint);
        }
        return count;
    }

    /**
     * Indicates whether a Unicode block should be treated as CJK for estimation.
     *
     * @param block Unicode block to evaluate
     * @return true when the block belongs to supported CJK families
     */
    private static boolean isCjkBlock(Character.UnicodeBlock block) {
        return block == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
                || block == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
                || block == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B
                || block == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_C
                || block == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_D
                || block == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_E
                || block == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_F
                || block == Character.UnicodeBlock.HIRAGANA
                || block == Character.UnicodeBlock.KATAKANA
                || block == Character.UnicodeBlock.HANGUL_SYLLABLES;
    }
}