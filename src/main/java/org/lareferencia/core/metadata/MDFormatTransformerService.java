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

package org.lareferencia.core.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.w3c.dom.Document;

public class MDFormatTransformerService {

	HashMap<String, HashMap<String, IMDFormatTransformer>> trfMap; 

	public MDFormatTransformerService() {	
		trfMap = new HashMap<String, HashMap<String, IMDFormatTransformer>>();		
	}

	public void setTransformers(List<IMDFormatTransformer> transformers) {

		// builds a multidimensional map with [sourceMDF][targetMDF] as keys
		for (IMDFormatTransformer trf : transformers ) {

			if (!trfMap.containsKey(trf.getSourceMDFormat())) {
				trfMap.put(trf.getSourceMDFormat(), new HashMap<String, IMDFormatTransformer>());
			}
			trfMap.get(trf.getSourceMDFormat()).put(trf.getTargetMDFormat(), trf);

		}
	}

	public List<String> getSourceMetadataFormats() {
		return new ArrayList<String>( trfMap.keySet() );
	}


	public Document transform(String srcMDFormat, String tgtMDFormat, Document source) throws MDFormatTranformationException {
		return this.getMDTransformer(srcMDFormat, tgtMDFormat).transform(source) ;
	}

	public String   transformToString(String srcMDFormat, String tgtMDFormat, Document source) throws MDFormatTranformationException {
		return this.getMDTransformer(srcMDFormat, tgtMDFormat).transformToString(source);
	}

	public IMDFormatTransformer getMDTransformer(String srcMDFormat, String tgtMDFormat) throws MDFormatTranformationException {
		try {

			IMDFormatTransformer trf = trfMap.get(srcMDFormat).get(tgtMDFormat);

			if (trf == null)
				throw new MDFormatTranformationException("No existe transformador de formatos: " + srcMDFormat + " a " + tgtMDFormat + " declarado en el servicios de transformación de formato de metadatos." );
			return trf;
		} catch (NullPointerException e) {
			throw new MDFormatTranformationException("No existe transformador de formatos: " + srcMDFormat + " a " + tgtMDFormat + " declarado en el servicios de transformación de formato de metadatos." );
		}
	}




}
