
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

import java.util.Map;

import org.lareferencia.core.domain.Network;
import org.lareferencia.core.domain.IOAIRecord;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.worker.validation.AbstractTransformerRule;
import org.lareferencia.core.worker.validation.ValidationException;

/**
 * Transformation rule that adds provenance metadata to records.
 * <p>
 * This rule enriches metadata records with information about their source repository
 * and institution. It extracts data from the {@link Network} configuration and adds
 * standardized provenance fields to the metadata.
 * <p>
 * Added fields include:
 * </p>
 * <ul>
 *   <li>Repository type, URL, and base OAI-PMH URL</li>
 *   <li>Institution type and URL</li>
 *   <li>Responsible party information</li>
 *   <li>Additional network attributes</li>
 * </ul>
 * 
 * @author LA Referencia Team
 * @see AbstractTransformerRule
 * @see Network
 */
public class AddProvenanceMetadataRule extends AbstractTransformerRule {

	@Getter
	@Setter
	private static String repoTypeField = "repository:repositoryType";

	@Getter
	@Setter
	private static String repoUrlField = "repository:repositoryURL";

	@Getter
	@Setter
	private static String instTypeField = "repository:institutionType";

	@Getter
	@Setter
	private static String instUrlField = "repository:institutionURL";

	@Getter
	@Setter
	private static String oaiUrlField = "repository:baseURL";

	@Getter
	@Setter
	private static String lastFirstResponsibleField = "repository.responsible";

	@Getter
	@Setter
	private static String responsibleChargeField = "repository:responsibleCharge";

	@Getter
	@Setter
	private static String contactEmailField = "repository:mail";

	@Getter
	@Setter
	private static String countryField = "repository:country";

	@Getter
	@Setter
	private static String cityField = "repository:city";

	@Getter
	@Setter
	private static String phoneField = "repository:phone";

	@Getter
	@Setter
	private static String softwareField = "repository:software";

	@Getter
	@Setter
	private static String journalTitleField = "repository:journalTitle";

	@Getter
	@Setter
	private static String doiField = "repository:DOI";

	@Getter
	@Setter
	private static String issnField = "repository:ISSN";

	@Getter
	@Setter
	private static String issnLField = "repository:ISSN_L";

	@Getter
	@Setter
	private static String repoIdField = "repository:repositoryID";

	@Getter
	@Setter
	private static String repoNameField = "repository:name";

	@Getter
	@Setter
	private static String harvestDateField = "repository:harvestDate";

	@Getter
	@Setter
	private static String statusField = "repository:altered";

	@Getter
	@Setter
	private static String oaiIdentifierField = "others:identifier";

	/**
	 * Creates a new provenance metadata rule.
	 */
	public AddProvenanceMetadataRule() {
	}
	
	/**
	 * Retrieves a field value from the attributes map.
	 * 
	 * @param attributes the attribute map
	 * @param fieldName the field name to retrieve
	 * @return the field value as string, or null if not found
	 */
	private String getField(Map<String, Object> attributes, String fieldName) {
		
		if ( attributes.containsKey(fieldName) && attributes.get(fieldName) != null )
			return attributes.get(fieldName).toString();
		else
			return "";	
	}

	@Override
	public boolean transform(SnapshotMetadata snapshotMetadata, IOAIRecord record, OAIRecordMetadata metadata) throws ValidationException {

		// LAReferenciaNetworkAttributes attributes = (LAReferenciaNetworkAttributes)
		// snapshotMetadata.getNetwork().getAttributes();
		
		metadata.removeFieldOcurrence(repoNameField);
		metadata.removeFieldOcurrence(contactEmailField);
		metadata.removeFieldOcurrence(oaiIdentifierField);

		try {
			Network network = snapshotMetadata.getNetwork();
			// AbstractNetworkAttributes attributes = network.getAttributes();
			Map<String, Object> attributes = network.getAttributes();

			metadata.addFieldOcurrence(repoTypeField, getField(attributes,"source_type"));
			metadata.addFieldOcurrence(repoUrlField, getField(attributes,"source_url"));
			metadata.addFieldOcurrence(instTypeField, getField(attributes,"institution_type"));
			metadata.addFieldOcurrence(instUrlField, getField(attributes,"institution_url"));
			metadata.addFieldOcurrence(oaiUrlField, getField(attributes,"oai_url"));
			metadata.addFieldOcurrence(contactEmailField, getField(attributes,"contact_email"));
			metadata.addFieldOcurrence(countryField, getField(attributes,"country"));
			metadata.addFieldOcurrence(doiField, getField(attributes,"doi"));
			metadata.addFieldOcurrence(issnField, getField(attributes,"issn"));
			metadata.addFieldOcurrence(issnLField, getField(attributes,"issn_l"));

			metadata.addFieldOcurrence(oaiIdentifierField, metadata.getIdentifier());

			// Construcción de id con prefijo "od"
			/*
			 * int numUnderscore = 12 - (attr_fields.get("repository_id").length() + 2);
			 * String preUnderscore = ""; for(int i = 0; i<numUnderscore; i++) {
			 * preUnderscore += "_"; } String idWithPrefix =
			 * "od"+preUnderscore+attr_fields.get("repository_id");
			 */
			String idWithPrefix = "opendoar:" + getField(attributes,"repository_id");

			metadata.addFieldOcurrence(repoIdField, idWithPrefix);
			metadata.addFieldOcurrence(harvestDateField, record.getDatestamp().toString());
			metadata.addFieldOcurrence(repoNameField, "" + snapshotMetadata.getNetwork().getName() + " - "
				+ snapshotMetadata.getNetwork().getInstitutionName());
			// TODO: transformed status not available in IOAIRecord - need to get from validation stats
			// metadata.addFieldOcurrence(statusField, "" + record.getTransformed());
		} catch (Exception e) {
			throw new ValidationException("An exception occured during AddProvenaceMedatada Transformation:" + e.getMessage());
		}

		return true;
	}
}
