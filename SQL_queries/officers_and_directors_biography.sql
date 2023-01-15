CREATE TABLE REFINITIV.STAGING.OFFICERS_AND_DIRECTORS_BIOGRAPHY
AS
SELECT "RepNo"
     , "OrganizationPermID"
     , "CompanyName"
     , "Production"
     , "OfficerPermID"
     , "Active"
     , "Status"
     , "Rank"
     , "ID"
     , "PersonID"
     , "PersonPermID"
     , "PersonActive"
     , "TextType"
     , "Text"
     , current_timestamp "DataWareHouseInsertTime"
FROM (
         SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING                        AS "RepNo"
              , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING           AS "OrganizationPermID"
              , GET(XMLGET(XML, 'CompanyName'), '$')::STRING                  AS "CompanyName"
              , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME             AS "Production"
              , GET(Officer.value, '@OfficerPermID')::STRING                  AS "OfficerPermID"
              , GET(Officer.value, '@Active')::STRING                         AS "Active"
              , GET(Officer.value, '@Status')::STRING                         AS "Status"
              , GET(Officer.value, '@Rank')::STRING                           AS "Rank"
              , GET(Officer.value, '@ID')::STRING                             AS "ID"
              , regexp_count(officer.path, '\\.|\\[') + 1                     as l
              , GET(XMLGET(Officer.value, 'Person'), '@ID')::STRING           AS "PersonID"
              , GET(XMLGET(Officer.value, 'Person'), '@PersonPermID')::STRING AS "PersonPermID"
              , GET(XMLGET(Officer.value, 'Person'), '@Active')::STRING       AS "PersonActive"
              , GET(BiographicalInformation.value, '@Type')::STRING           AS "TextType"
              , GET(BiographicalInformation.value, '$')::STRING               AS "Text"
         FROM REFINITIV.STAGING.OFFICERS_AND_DIRECTORS_XML,
              LATERAL FLATTEN(input => XML, recursive => true) as Officer,
              LATERAL FLATTEN(input => Officer.value, recursive => true) as BiographicalInformation
         WHERE GET(officer.value, '@') = 'Officer'
           AND (L = 4 OR L = 5)
           AND GET(BiographicalInformation.value, '@') = 'Text')
