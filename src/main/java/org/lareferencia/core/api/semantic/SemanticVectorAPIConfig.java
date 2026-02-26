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

package org.lareferencia.core.api.semantic;

import java.io.IOException;
import java.time.Duration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.reactive.function.client.support.WebClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

import reactor.netty.http.client.HttpClient;
import reactor.util.retry.Retry;

@Configuration
public class SemanticVectorAPIConfig {

    private static final Logger logger = LogManager.getLogger(SemanticVectorAPIConfig.class);

    @Value("${embedding.api.url}")
    private String embeddingApiUrl;

    @Value("${embedding.api.timeout.seconds:30}")
    private int embeddingApiTimeoutSeconds;

    @Bean
    public SemanticVectorAPI semanticVectorAPI() {
        HttpClient httpClient = HttpClient.create()
                .responseTimeout(Duration.ofSeconds(embeddingApiTimeoutSeconds));

        // Define the retry strategy
        Retry retryStrategy = Retry.backoff(3, Duration.ofSeconds(1))
                .jitter(0.75)
                .doBeforeRetry(retrySignal ->
                        logger.warn("Retrying API call. Attempt: {}. Cause: {}",
                                retrySignal.totalRetries() + 1,
                                retrySignal.failure().getMessage()));

        WebClient webClient = WebClient.builder()
                .baseUrl(embeddingApiUrl)
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(16 * 1024 * 1024))
                .defaultHeaders(headers -> headers.set("Content-Type", "application/json"))
                .filter((request, next) -> next.exchange(request).retryWhen(retryStrategy))
                .build();

        HttpServiceProxyFactory factory = HttpServiceProxyFactory.builder()
                .exchangeAdapter(WebClientAdapter.create(webClient))
                .build();

        return factory.createClient(SemanticVectorAPI.class);
    }
}
