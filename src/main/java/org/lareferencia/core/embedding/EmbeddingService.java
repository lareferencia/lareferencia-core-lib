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

package org.lareferencia.core.embedding;

import java.text.MessageFormat;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.embedding.client.EmbeddingAPIClient;
import org.lareferencia.core.embedding.client.EmbeddingData;
import org.lareferencia.core.embedding.client.EmbeddingRequest;
import org.lareferencia.core.embedding.client.EmbeddingResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Default {@link IEmbeddingService} implementation backed by
 * {@link EmbeddingAPIClient}.
 *
 * <p>
 * This service encapsulates request construction, basic response validation,
 * and dimension checks so callers receive a domain-oriented
 * {@link Optional} contract.
 * </p>
 */
@Service
public class EmbeddingService implements IEmbeddingService {

  private static Logger logger = LogManager.getLogger(EmbeddingService.class);
  @Autowired
  private EmbeddingAPIClient embeddingAPIClient;
  @Value("${embedding.model.name}")
  private String embeddingModelName;
  @Value("${embedding.model.datatype:float}")
  private String embeddingModelDataType;
  @Value("${embedding.model.dimension}")
  private int embeddingModelDimension;
  @Value("${embedding.model.applicationId:lareferencia}")
  private String embeddingApplicationId;

  /**
   * {@inheritDoc}
   */
  @Override
  public Optional<List<Float>> embed(String text) {
    return callEmbeddingAPI(text)
        .filter(list -> !list.isEmpty())
        .map(list -> list.get(0));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Optional<List<List<Float>>> embed(List<String> texts) {
    logger.debug(MessageFormat.format("Count of texts for embeddings: {0}", texts.size()));
    return callEmbeddingAPI(texts);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getEmbeddingDimension() {
    return embeddingModelDimension;
  }

  /**
   * Calls the remote embedding API and validates the returned vectors.
   *
   * @param textForEmbedding source payload accepted by the API endpoint
   *                         (single text or list of texts)
   * @return an {@link Optional} with all returned vectors when the API call and
   *         validation succeed, otherwise {@link Optional#empty()}
   */
  private Optional<List<List<Float>>> callEmbeddingAPI(Object textForEmbedding) {
    try {
      EmbeddingResponse response = embeddingAPIClient.generateEmbedding(
          new EmbeddingRequest(textForEmbedding,
              embeddingModelName,
              embeddingModelDataType,
              embeddingModelDimension,
              embeddingApplicationId));

      if (response != null && response.getData() != null && !response.getData().isEmpty()) {

        List<List<Float>> embeddings = response.getData()
            .stream()
            .map(EmbeddingData::getEmbedding)
            .toList();

        if (embeddings == null || embeddings.isEmpty()) {
          logger.warn(MessageFormat.format("Embedding API returned empty vector for text: {0}", textForEmbedding));
          return Optional.empty();
        }

        logger.debug(MessageFormat.format("Count of vectors: {0}", embeddings.size()));

        var vectorsDimension = embeddings.get(0).size();

        if (vectorsDimension != embeddingModelDimension) {
          logger.warn(MessageFormat.format(
              "Embedding dimension mismatch: expected {0}, got {1}.", embeddingModelDimension, vectorsDimension));
          return Optional.empty();
        }

        return Optional.of(embeddings);
      }
    } catch (Exception e) {
      logger.error(MessageFormat.format("Failed to generate embedding | Error: {0}", e.getMessage()));

    }

    return Optional.empty();
  }

}
