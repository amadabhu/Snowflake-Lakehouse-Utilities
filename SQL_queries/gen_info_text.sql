CREATE TABLE REFINITIV.STAGING.GEN_INFO_TEXT AS
SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING              AS "RepNo"
     , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING AS "OrganizationPermID"
     , GET(XMLGET(XML, 'CompanyName'), '$')::STRING        AS "CompanyName"
     , GET(XMLGET(XML, 'CompanyName'), '@Type')::STRING    AS "CompanyNameType"
     , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME   AS "ProductionDate"
     , GET(Text.value, '@Type')::STRING                    AS "TextType"
     , GET(Text.value, '@LastUpdated')::STRING             AS "TextLastUpdated"
     , GET(Text.value, '@Lang')::STRING                    AS "TextLanguage"
     , GET(Text.value, '@SourceFilingType')::STRING        AS "TextSourceFilingType"
     , GET(Text.value, '@SourceFilingDate')::DATETIME      AS "SourceFilingDate"
     , GET(Text.value, '$')::STRING                        AS "Text"
     , CURRENT_TIMESTAMP                                   AS "DataWarehouseInsertTime"
FROM REFINITIV.STAGING.GI_COMPANIES_XML
   , LATERAL FLATTEN(xml, RECURSIVE => TRUE) TextInfo
   , LATERAL FLATTEN(TextInfo.value, RECURSIVE => TRUE) Text
WHERE GET(TextInfo.value, '@') = 'TextInfo'
  AND GET(Text.value, '@') = 'Text';
