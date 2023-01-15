CREATE TABLE REFINITIV.STAGING.OFFICERS_AND_DIRECTORS_AFFILIATION AS
SELECT "RepNo"
     , "OrganizationPermID"
     , "CompanyName"
     , "CompanyNameType"
     , "ProductionDate"
     , "OfficerPermID"
     , "AffiliatedRepNo"
     , "AffiliatedOrganizationPermID"
     , "AffiliatedCompanyName"
     , "AffiliatedOfficerID"
     , "AffiliatedOfficerPermID"
     , "AffiliatedOfficerTitle"
     , "AffiliatedOfficerActiveStatus"
     , current_timestamp AS "DataWareHouseInsertTime"
FROM (
         SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING                                   AS "RepNo"
              , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING                      AS "OrganizationPermID"
              , GET(XMLGET(XML, 'CompanyName'), '$')::STRING                             AS "CompanyName"
              , GET(XMLGET(XML, 'CompanyName'), '@Type')::STRING                         AS "CompanyNameType"
              , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME                        AS "ProductionDate"
              , GET(officer.value, '@OfficerPermID')::STRING                             AS "OfficerPermID"
              , regexp_count(officer.path, '\\.|\\[') + 1                                as L
              , GET(XMLGET(affiliation.value, 'Company'), '@RepNo')::STRING              AS "AffiliatedRepNo"
              , GET(XMLGET(affiliation.value, 'Company'), '@OrganizationPermID')::STRING AS "AffiliatedOrganizationPermID"
              , GET(XMLGET(affiliation.value, 'Company'), '@Name')::STRING               AS "AffiliatedCompanyName"
              , GET(XMLGET(affiliation.value, 'Officer'), '@ID')::STRING                 AS "AffiliatedOfficerID"
              , GET(XMLGET(affiliation.value, 'Officer'), '@OfficerPermID')::STRING      AS "AffiliatedOfficerPermID"
              , GET(XMLGET(affiliation.value, 'Officer'), '@Title')::STRING              AS "AffiliatedOfficerTitle"
              , GET(XMLGET(affiliation.value, 'Officer'), '@Active')::STRING             AS "AffiliatedOfficerActiveStatus"
         FROM REFINITIV.STAGING.OFFICERS_AND_DIRECTORS_XML,
              LATERAL FLATTEN(input => XML, recursive => true) as officer,
              LATERAL FLATTEN(input => officer.value, recursive => true) as affiliation
         WHERE GET(officer.value, '@') = 'Officer'
           AND (L = 4 OR L = 5)
           AND GET(affiliation.value, '@') = 'Affiliation');
