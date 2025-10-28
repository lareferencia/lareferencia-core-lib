<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0" 
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    
    <xsl:output method="xml" indent="yes" omit-xml-declaration="yes"/>
    
    <!-- Parameter for testing -->
    <xsl:param name="testParam" select="'defaultValue'"/>
    
    <xsl:template match="/">
        <result>
            <parameter><xsl:value-of select="$testParam"/></parameter>
            <xsl:apply-templates/>
        </result>
    </xsl:template>
    
    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>
    
</xsl:stylesheet>
