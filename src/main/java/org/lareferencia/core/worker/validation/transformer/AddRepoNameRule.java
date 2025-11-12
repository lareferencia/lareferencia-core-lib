
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

package org.lareferencia.core.worker.validation.transformer;

import lombok.Getter;
import lombok.Setter;

import org.lareferencia.core.domain.IOAIRecord;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.util.RepositoryNameHelper;
import org.lareferencia.core.worker.validation.AbstractTransformerRule;

/**
 * Transformation rule that adds repository and institution names to metadata.
 * <p>
 * Enriches records with repository name, institution name, and institution
 * acronym fields. Can append or replace existing values with configured prefixes.
 * </p>
 * 
 * @author LA Referencia Team
 * @see AbstractTransformerRule
 */
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

	/**
	 * Creates a new repository name addition rule.
	 */
	public AddRepoNameRule() {
	}

	@Override
	public boolean transform(SnapshotMetadata snapshotMetadata, IOAIRecord record, OAIRecordMetadata metadata) {

		// Se carga el helper para la resolución de nombre de repositorios
		repositoryNameHelper = new RepositoryNameHelper();

		// Si está configurado agrega a la metadata el reponame y el instname
		if (doRepoNameAppend) {
			repositoryNameHelper.appendNameToMetadata(metadata, repoNameField, repoNamePrefix, snapshotMetadata.getNetwork().getName(), doRepoNameReplace);
		}

		if (doInstNameAppend) {
			repositoryNameHelper.appendNameToMetadata(metadata, instNameField,  instNamePrefix, snapshotMetadata.getNetwork().getInstitutionName(), doInstNameReplace);
			repositoryNameHelper.appendNameToMetadata(metadata, instAcronField, instAcronPrefix, snapshotMetadata.getNetwork().getInstitutionAcronym(), doInstNameReplace);

		}

		return doInstNameAppend || doRepoNameAppend;
	}

}
