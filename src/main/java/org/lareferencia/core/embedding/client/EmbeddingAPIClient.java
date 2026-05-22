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

package org.lareferencia.core.embedding.client;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.service.annotation.PostExchange;

/**
 * Declarative HTTP client for the semantic vector embedding API (Spring 6 HTTP
 * Interface).
 *
 * <p>
 * This is a low-level transport interface that maps directly to the
 * OpenAI-compatible
 * embedding endpoint. It should <strong>not</strong> be used directly by domain
 * components
 * (workers, pipelines). Use
 * {@link org.lareferencia.core.embedding.IEmbeddingService}
 * instead.
 * </p>
 *
 * <p>
 * The bean is instantiated and configured by
 * {@link org.lareferencia.core.EmbeddingAPIConfig.embedding.config.SemanticVectorAPIConfig}.
 * </p>
 */
public interface EmbeddingAPIClient {

    /**
     * Calls the remote embedding endpoint and returns the raw API response.
     *
     * @param request the embedding request payload (model, input, format,
     *                dimensions)
     * @return the raw API response containing one or more embedding vectors
     */
    @PostExchange("")
    EmbeddingResponse generateEmbedding(@RequestBody EmbeddingRequest request);
}
