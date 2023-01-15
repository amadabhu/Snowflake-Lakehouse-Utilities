CREATE TABLE REFINITIV.STAGING.GEN_INFO_AUDITORS AS
SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING              AS "RepNo"
     , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING AS "OrganizationPermID"
     , GET(XMLGET(XML, 'CompanyName'), '$')::STRING        AS "CompanyName"
     , GET(XMLGET(XML, 'CompanyName'), '@Type')::STRING    AS "CompanyNameType"
     , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME   AS "ProductionDate"
     , GET(Auditor.value, '@Code')::STRING                 AS "AuditorCode"
     , GET(Auditor.value, '@Name')::STRING                 AS "AuditorName"
     , CURRENT_TIMESTAMP                                   AS "DataWarehouseInsertTime"
FROM REFINITIV.STAGING.GI_COMPANIES_XML
   , LATERAL FLATTEN(xml, RECURSIVE => TRUE) Advisors
   , LATERAL FLATTEN(Advisors.value, RECURSIVE => TRUE) Auditor
WHERE GET(Advisors.value, '@') = 'Advisors'
  AND GET(Auditor.value, '@') = 'Auditor';
