/*
 *   Copyright (c) 2013-2025. LA Referencia / Red CLARA and others
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

package org.lareferencia.core.domain;

import java.time.LocalDateTime;

/**
 * Interfaz común para registros OAI en diferentes backends de almacenamiento.
 * <p>
 * Esta interfaz define el contrato mínimo para registros OAI persistidos en el
 * catálogo relacional de la plataforma.
 * </p>
 * 
 * <h2>IMPLEMENTACIÓN:</h2>
 * <ul><li>{@link OAIRecord} - Registro del catálogo SQLite</li></ul>
 * 
 * <h2>CAMPOS COMUNES:</h2>
 * <ul>
 *   <li><b>id</b>: Identificador único del registro</li>
 *   <li><b>identifier</b>: Identificador OAI-PMH del registro</li>
 *   <li><b>datestamp</b>: Fecha de última modificación del registro</li>
 *   <li><b>originalMetadataHash</b>: Hash MD5 del XML original cosechado</li>
 *   <li><b>deleted</b>: Flag indicando si el registro fue eliminado en el origen</li>
 * </ul>
 * 
 * @author LA Referencia Team
 * @see OAIRecord
 */
public interface IOAIRecord {
    
    /**
     * Obtiene el identificador único del record.
     * 
     * @return identificador estable del registro
     */
    String getId();
    
    /**
     * Obtiene el identificador OAI-PMH del registro.
     * 
     * @return identificador OAI único del registro
     */
    String getIdentifier();
    
    /**
     * Establece el identificador OAI-PMH del registro.
     * 
     * @param identifier identificador OAI único
     */
    void setIdentifier(String identifier);
    
    /**
     * Obtiene la fecha de última modificación del registro según OAI-PMH.
     * 
     * @return fecha de última modificación
     */
    LocalDateTime getDatestamp();
    
    /**
     * Establece la fecha de última modificación del registro.
     * 
     * @param datestamp fecha de última modificación
     */
    void setDatestamp(LocalDateTime datestamp);
    
    /**
     * Obtiene el hash MD5 del XML original cosechado.
     * 
     * @return hash MD5 en formato hexadecimal
     */
    String getOriginalMetadataHash();
    
    /**
     * Establece el hash MD5 del XML original cosechado.
     * 
     * @param hash hash MD5 en formato hexadecimal
     */
    void setOriginalMetadataHash(String hash);
}
