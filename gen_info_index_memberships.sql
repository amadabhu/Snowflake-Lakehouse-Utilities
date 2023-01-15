CREATE TABLE REFINITIV.STAGING.GEN_INFO_INDEX_MEMBERSHIPS  AS
SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING              AS "RepNo"
     , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING AS "OrganizationPermID"
     , GET(XMLGET(XML, 'CompanyName'), '$')::STRING        AS "CompanyName"
     , GET(XMLGET(XML, 'CompanyName'), '@Type')::STRING    AS "CompanyNameType"
     , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME   AS "ProductionDate"
     , GET(ConstituentOf.value, '@IndexRIC')::STRING       AS "IndexRIC"
     , GET(ConstituentOf.value, '$')::STRING               AS "Index"
     , CURRENT_TIMESTAMP                                   AS "DataWarehouseInsertTime"
FROM REFINITIV.STAGING.GI_COMPANIES_XML
   , LATERAL FLATTEN(xml, RECURSIVE => TRUE) IndexMemberships
   , LATERAL FLATTEN(IndexMemberships.value, RECURSIVE => TRUE) ConstituentOf
WHERE GET(IndexMemberships.value, '@') = 'IndexMemberships'
  AND GET(ConstituentOf.value, '@') = 'ConstituentOf';
