CREATE TABLE REFINITIV.STAGING.GEN_INFO_OFFICER_TENURE_DATES AS
SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING                                                                       AS "RepNo"
     , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING                                                          AS "OrganizationPermID"
     , GET(XMLGET(XML, 'CompanyName'), '$')::STRING                                                                 AS "CompanyName"
     , GET(XMLGET(XML, 'CompanyName'), '@Type')::STRING                                                             AS "CompanyNameType"
     , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME                                                            AS "ProductionDate"
     , GET(Officer.value, '@lang')::STRING                                                                          AS "Language"
     , GET(Officer.value, '@ID')::STRING                                                                            AS "ID"
     , GET(Officer.value, '@PersonPermID')::STRING                                                                  AS "PersonPermID"
     , GET(Officer.value, '@OfficerPermID')::STRING                                                                 AS "OfficerPermID"
     , GET(Officer.value, '@Rank')::STRING                                                                          AS "Rank"
     , GET(Officer.value, '@Status')::STRING                                                                        AS "Status"
     , GET(Officer.value, '@PersonID')::STRING                                                                      AS "PersonID"
     , GET(XMLGET(XMLGET(XMLGET(Officer.value, 'NameAndTitle'), 'TenureDates'), 'OfficerStart'),
           '@Day')::STRING                                                                                          AS "OfficerStartDay"
     , GET(XMLGET(XMLGET(XMLGET(Officer.value, 'NameAndTitle'), 'TenureDates'), 'OfficerStart'),
           '@Month')::STRING                                                                                        AS "OfficerStartMonth"
     , GET(XMLGET(XMLGET(XMLGET(Officer.value, 'NameAndTitle'), 'TenureDates'), 'OfficerStart'),
           '@Year')::STRING                                                                                         AS "OfficerStartYear"
     , GET(XMLGET(XMLGET(XMLGET(Officer.value, 'NameAndTitle'), 'TenureDates'), 'OfficerEnd'),
           '@Day')::STRING                                                                                          AS "OfficerEndDay"
     , GET(XMLGET(XMLGET(XMLGET(Officer.value, 'NameAndTitle'), 'TenureDates'), 'OfficerEnd'),
           '@Month')::STRING                                                                                        AS "OfficerEndMonth"
     , GET(XMLGET(XMLGET(XMLGET(Officer.value, 'NameAndTitle'), 'TenureDates'), 'OfficerEnd'),
           '@Year')::STRING                                                                                         AS "OfficerEndYear"
     , GET(XMLGET(XMLGET(XMLGET(Officer.value, 'NameAndTitle'), 'TenureDates'), 'DirectorStart'),
           '@Day')::STRING                                                                                          AS "DirectorStartDay"
     , GET(XMLGET(XMLGET(XMLGET(Officer.value, 'NameAndTitle'), 'TenureDates'), 'DirectorStart'),
           '@Month')::STRING                                                                                        AS "DirectorStartMonth"
     , GET(XMLGET(XMLGET(XMLGET(Officer.value, 'NameAndTitle'), 'TenureDates'), 'DirectorStart'),
           '@Year')::STRING                                                                                         AS "DirectorStartYear"
     , GET(XMLGET(XMLGET(XMLGET(Officer.value, 'NameAndTitle'), 'TenureDates'), 'DirectorEnd'),
           '@Day')::STRING                                                                                          AS "DirectorEndDay"
     , GET(XMLGET(XMLGET(XMLGET(Officer.value, 'NameAndTitle'), 'TenureDates'), 'DirectorEnd'),
           '@Month')::STRING                                                                                        AS "DirectorEndMonth"
     , GET(XMLGET(XMLGET(XMLGET(Officer.value, 'NameAndTitle'), 'TenureDates'), 'DirectorEnd'),
           '@Year')::STRING                                                                                         AS "DirectorEndYear"
FROM REFINITIV.STAGING.GI_COMPANIES_XML
   , LATERAL FLATTEN(xml, RECURSIVE => TRUE) OfficersInfo
   , LATERAL FLATTEN(OfficersInfo.value, RECURSIVE => TRUE) Officer
WHERE GET(OfficersInfo.value, '@') = 'OfficersInfo'
  AND GET(Officer.value, '@') = 'Officer';
