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

package org.lareferencia.core.worker.validation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to define JSON schema properties for validator rule fields.
 * Used for automatic JSON schema and form generation.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface SchemaProperty {

    /**
     * Title displayed in the form.
     */
    String title();

    /**
     * Description/help text for the field.
     */
    String description() default "";

    /**
     * JSON Schema type: string, integer, boolean, array.
     * If empty, inferred from Java field type.
     */
    String type() default "";

    /**
     * UI widget type: textarea, select, etc.
     * If empty, uses default input.
     */
    String uiType() default "";

    /**
     * Default value as string.
     */
    String defaultValue() default "";

    /**
     * Order in form display. Lower values appear first.
     */
    int order() default 100;
}
