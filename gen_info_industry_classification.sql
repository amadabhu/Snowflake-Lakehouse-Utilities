CREATE TABLE REFINITIV.STAGING.GEN_INFO_INDUSTRY_CLASSIFICATION AS
SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING              AS "RepNo"
     , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING AS "OrganizationPermID"
     , GET(XMLGET(XML, 'CompanyName'), '$')::STRING        AS "CompanyName"
     , GET(XMLGET(XML, 'CompanyName'), '@Type')::STRING    AS "CompanyNameType"
     , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME   AS "ProductionDate"
     , GET(Taxonomy.value, '@Type')::STRING                AS "IndustrialClassificationTaxonomyType"
     , GET(Detail.value, '@Code')::STRING                  AS "Code"
     , GET(Detail.value, '@Description')::STRING           AS "Description"
     , GET(Detail.value, '@Order')::STRING                 AS "Order"
     , CURRENT_TIMESTAMP                                   AS "DataWarehouseInsertTime"
FROM REFINITIV.STAGING.GI_COMPANIES_XML
   , LATERAL FLATTEN(xml, RECURSIVE => TRUE) IndustryClassification
   , LATERAL FLATTEN(IndustryClassification.value, RECURSIVE => TRUE) Taxonomy
   , LATERAL FLATTEN(Taxonomy.value, RECURSIVE => TRUE) Detail
WHERE GET(IndustryClassification.value, '@') = 'IndustryClassification'
  AND GET(Taxonomy.value, '@') = 'Taxonomy'
  AND GET(Detail.value, '@') = 'Detail';
