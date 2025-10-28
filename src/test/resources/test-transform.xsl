<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0" 
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:dc="http://purl.org/dc/elements/1.1/">
    
    <xsl:output method="xml" indent="yes" omit-xml-declaration="yes"/>
    
    <!-- Simple identity transformation for testing -->
    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>
    
    <!-- Add a processing indicator -->
    <xsl:template match="/">
        <processed>
            <xsl:apply-templates/>
        </processed>
    </xsl:template>
    
</xsl:stylesheet>
