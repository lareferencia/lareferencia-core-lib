/*
 *   Copyright (c) 2013-2022. LA Referencia / Red CLARA and others
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
 *   This file is part of LA Referencia software platform LRHarvester v4.x
 *   For any further information please contact Lautaro Matas <lmatas@gmail.com>
 */

package org.lareferencia.core.embedding;

import java.util.List;
import java.util.Optional;

/**
 * Domain-level service for semantic embedding generation.
 *
 * <p>
 * This interface decouples the domain (workers, pipelines) from the concrete
 * embedding transport mechanism (HTTP, local model, etc.).
 * Multiple implementations can coexist (e.g., OpenAI-compatible HTTP API,
 * an embedded local model, a no-op stub for testing).
 * </p>
 *
 * <p>
 * Implementations are responsible for:
 * </p>
 * <ul>
 * <li>Choosing the model and encoding format</li>
 * <li>Validating the returned vector dimension</li>
 * <li>Handling transport-level errors and retries</li>
 * </ul>
 *
 * <p>
 * Callers receive an {@link Optional} so they can decide the fallback strategy
 * without catching exceptions from lower layers.
 * </p>
 */
public interface IEmbeddingService {

    /**
     * Generates a semantic embedding vector for the given text.
     *
     * @param text the text to embed (pre-truncated to model limits if necessary)
     * @return an {@link Optional} containing the embedding vector as a list of
     *         floats,
     *         or {@link Optional#empty()} if generation fails for any reason
     */
    Optional<List<Float>> embed(String text);

    /**
     * Generates semantic embedding vectors for multiple texts in one request.
     *
     * @param texts the texts to embed
     * @return an {@link Optional} containing one vector per input text,
     *         or {@link Optional#empty()} if generation fails
     */
    Optional<List<List<Float>>> embed(List<String> texts);

    /**
     * Returns the configured embedding dimension for this service instance.
     * Can be used by callers to pre-validate or log dimension expectations.
     *
     * @return expected vector dimension, or 0 if not enforced
     */
    int getEmbeddingDimension();
}
