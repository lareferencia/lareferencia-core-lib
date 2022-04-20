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

package org.lareferencia.backend.validation.validator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.List;

@JsonIgnoreProperties({ "controlledValues" })
public class LargeControlledValueFieldContentValidatorRule extends ControlledValueFieldContentValidatorRule {
	
	private static Logger logger = LogManager.getLogger(LargeControlledValueFieldContentValidatorRule.class);


	private static final int MAX_PRINTED_LINES = 25;
	private static final String CSV_SEPARATOR = ";";

	@JsonProperty("controlledValuesCSV")
	private String controlledValuesCSV;

	public String getControlledValuesCSV() {
		if (controlledValuesCSV == null)
			return getCSVStringFromControlledValues(this.controlledValues);
		else
			return controlledValuesCSV;
	}

	public void setControlledValuesCSV(String controlledValuesCSV) {
		this.controlledValuesCSV = controlledValuesCSV;
		this.setControlledValuesFromCsvString(controlledValuesCSV);
	}

	public void setControlledValuesFromCsvString(String csvString) {

		logger.debug("\n\nCargando validador: valores controlados desde cadenaCSV");

		String[] values = csvString.split(CSV_SEPARATOR);

		for (int i = 0; i < values.length; i++) {

			this.controlledValues.add(values[i]);

			if (i < MAX_PRINTED_LINES)
				logger.debug(values[i]);
			else
				logger.debug(".");
		}

		logger.debug("\nFin Carga validador: valores controlados desde cadenaCSV");
	}

	private String getCSVStringFromControlledValues(List<String> controlledList) {
		// TODO: Cambiar por un String Buffer para mejorar performance
		String result = "";

		for (int i = 0; i < controlledList.size(); i++) {

			result += controlledList.get(i);
			if (i < controlledList.size() - 1)
				result += CSV_SEPARATOR;
		}

		return result;
	}

	public void setControlledValuesFileName(String filename) {

		try {

			logger.debug("\n\nCargando validador: valores controlados: " +filename);

			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "UTF8"));

			String line = br.readLine();

			int lineNumber = 0;

			while (line != null) {

				this.controlledValues.add(line);

				if (lineNumber++ < MAX_PRINTED_LINES)
					logger.debug(filename + " : " +line);
				else
					System.out.print(".");

				line = br.readLine();
			}

			br.close();

			logger.debug("\nFin Carga validador: valores controlados: " + filename +"\n\n");

		} catch (FileNotFoundException e) {
			logger.error("!!!!!! No se encontró el archivo de valores controlados:" +filename);
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
