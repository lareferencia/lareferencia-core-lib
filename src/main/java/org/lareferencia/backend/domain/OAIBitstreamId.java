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

package org.lareferencia.backend.domain;

import java.io.Serializable;
import java.util.Objects;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;

import lombok.Getter;
import lombok.Setter;

@Embeddable
@Getter
@Setter
public class OAIBitstreamId implements Serializable {

	@ManyToOne
    @JoinColumn(name = "network_id")
    private Network network;
 
    @Column(name = "identifier")
    private String identifier;
    
    @Column(nullable = false)
	private String checksum;
	
    
    public OAIBitstreamId() {
    }
 
    public OAIBitstreamId(Network network, String identifier, String checksum) {
       this.identifier = identifier;
       this.network = network;
       this.checksum = checksum;
    }
 
 
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OAIBitstreamId)) return false;
        OAIBitstreamId that = (OAIBitstreamId) o;
        return Objects.equals(getIdentifier(), that.getIdentifier()) &&
                Objects.equals(getNetwork().getId(), that.getNetwork().getId()) && Objects.equals(getChecksum(), that.getChecksum());
    }
 
    @Override
    public int hashCode() {
        return Objects.hash(this.getChecksum(), this.getChecksum());
    }
}





 
    