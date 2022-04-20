
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

package org.lareferencia.backend.validation.transformer;

import lombok.Getter;
import lombok.Setter;

import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.util.RepositoryNameHelper;
import org.lareferencia.core.validation.AbstractTransformerRule;

public class AddRepoNameRule extends AbstractTransformerRule {

	private RepositoryNameHelper repositoryNameHelper;

	@Getter
	@Setter
	private Boolean doRepoNameAppend = false;

	@Getter
	@Setter
	private Boolean doRepoNameReplace = false;

	@Getter
	@Setter
	private String repoNameField = "dc.source.none";

	@Getter
	@Setter
	private String repoNamePrefix = "reponame:";

	@Getter
	@Setter
	private Boolean doInstNameAppend = false;

	@Getter
	@Setter
	private Boolean doInstNameReplace = false;

	@Getter
	@Setter
	private String instNameField = "dc.source.none" ;

	@Getter
	@Setter
	private String instNamePrefix = "instname:";
	
	@Getter
	@Setter
	private String instAcronField = "dc.source.none";

	@Getter
	@Setter
	private String instAcronPrefix = "instacron:";

	public AddRepoNameRule() {
	}

	@Override
	public boolean transform(OAIRecord record, OAIRecordMetadata metadata) {

		// Se carga el helper para la resolución de nombre de repositorios
		repositoryNameHelper = new RepositoryNameHelper();

		// Si está configurado agrega a la metadata el reponame y el instname
		if (doRepoNameAppend) {
			repositoryNameHelper.appendNameToMetadata(metadata, repoNameField, repoNamePrefix, record.getSnapshot().getNetwork().getName(), doRepoNameReplace);
		}

		if (doInstNameAppend) {
			repositoryNameHelper.appendNameToMetadata(metadata, instNameField,  instNamePrefix, record.getSnapshot().getNetwork().getInstitutionName(), doInstNameReplace);
			repositoryNameHelper.appendNameToMetadata(metadata, instAcronField, instAcronPrefix, record.getSnapshot().getNetwork().getInstitutionAcronym(), doInstNameReplace);

		}

		return doInstNameAppend || doRepoNameAppend;
	}

}
