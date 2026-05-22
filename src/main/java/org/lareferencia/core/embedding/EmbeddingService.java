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

@Service
public class EmbeddingService implements IEmbeddingService {

  private static Logger logger = LogManager.getLogger(EmbeddingService.class);
  @Autowired
  private EmbeddingAPIClient embeddingAPIClient;
  @Value("${embedding.model.name}")
  private String embeddingModelName;
  @Value("${embedding.model.datatype}")
  private String embeddingModelDataType;
  @Value("${embedding.model.dimension}")
  private int embeddingModelDimension;
  @Value("${embedding.model.applicationId}")
  private String embeddingApplicationId;

  @Override
  public Optional<List<Float>> embed(String text) {
    return callEmbeddingAPI(text)
        .filter(list -> !list.isEmpty())
        .map(list -> list.get(0));
  }

  @Override
  public Optional<List<List<Float>>> embed(List<String> texts) {
    logger.info(MessageFormat.format("Count of texts for embeddings: {0}", texts.size()));
    return callEmbeddingAPI(texts);
  }

  @Override
  public int getEmbeddingDimension() {
    return embeddingModelDimension;
  }

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

        logger.info(MessageFormat.format("Count of vectors: {0}", embeddings.size()));

        var  vectorsDimension = embeddings.get(0).size();

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
