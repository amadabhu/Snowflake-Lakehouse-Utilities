CREATE TABLE REFINITIV.STAGING.GEN_INFO_WEB_LINKS AS
SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING              AS "RepNo"
     , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING AS "OrganizationPermID"
     , GET(XMLGET(XML, 'CompanyName'), '$')::STRING        AS "CompanyName"
     , GET(XMLGET(XML, 'CompanyName'), '@Type')::STRING    AS "CompanyNameType"
     , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME   AS "ProductionDate"
     , GET(WebLinksInfo.value, '@LastUpdated')::DATETIME   AS "WebLinkInfoLastUpdated"
     , GET(WebSite.value, '@Type')::STRING                 AS "WebSiteType"
     , GET(WebSite.value, '$')::STRING                     AS "WebSite"
     , CURRENT_TIMESTAMP                                   AS "DataWarehouseInsertTime"
FROM REFINITIV.STAGING.GI_COMPANIES_XML
   , LATERAL FLATTEN(xml, RECURSIVE => TRUE) WebLinksInfo
   , LATERAL FLATTEN(WebLinksInfo.value, RECURSIVE => TRUE) WebSite
WHERE GET(WebLinksInfo.value, '@') = 'WebLinksInfo'
  AND GET(WebSite.value, '@') = 'WebSite';
