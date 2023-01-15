CREATE TABLE REFINITIV.STAGING.GEN_INFO_OFFICER_TITLES AS
SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING                    AS "RepNo"
     , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING       AS "OrganizationPermID"
     , GET(XMLGET(XML, 'CompanyName'), '$')::STRING              AS "CompanyName"
     , GET(XMLGET(XML, 'CompanyName'), '@Type')::STRING          AS "CompanyNameType"
     , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME         AS "ProductionDate"
     , GET(Officer.value, '@lang')::STRING                       AS "Language"
     , GET(Officer.value, '@ID')::STRING                         AS "ID"
     , GET(Officer.value, '@PersonPermID')::STRING               AS "PersonPermID"
     , GET(Officer.value, '@OfficerPermID')::STRING              AS "OfficerPermID"
     , GET(Officer.value, '@Rank')::STRING                       AS "Rank"
     , GET(Officer.value, '@Status')::STRING                     AS "Status"
     , GET(Officer.value, '@PersonID')::STRING                   AS "PersonID"
     , GET(XMLGET(Designation.value, 'Start'), '@Day')::STRING   AS "DesignationStartDay"
     , GET(XMLGET(Designation.value, 'Start'), '@Month')::STRING AS "DesignationStartMonth"
     , GET(XMLGET(Designation.value, 'Start'), '@Year')::STRING  AS "DesignationStartYear"
     , GET(XMLGET(Designation.value, 'End'), '@Day')::STRING     AS "DesignationEndDay"
     , GET(XMLGET(Designation.value, 'End'), '@Month')::STRING   AS "DesignationEndMonth"
     , GET(XMLGET(Designation.value, 'End'), '@Year')::STRING    AS "DesignationEndYear"
     , GET(XMLGET(Designation.value, 'LongTitle'), '$')::STRING  AS "LongTitle"
     , GET(Title.value, '@Order')::STRING                        AS "TitleOrder"
     , GET(Title.value, '@ID')::STRING                           AS "TitleID"
     , GET(Title.value, '$')::STRING                             AS "Title"
FROM REFINITIV.STAGING.GI_COMPANIES_XML
   , LATERAL FLATTEN(xml, RECURSIVE => TRUE) OfficersInfo
   , LATERAL FLATTEN(OfficersInfo.value, RECURSIVE => TRUE) Officer
   , LATERAL FLATTEN(Officer.value, RECURSIVE => TRUE) Designation
   , LATERAL FLATTEN(Designation.value, RECURSIVE => TRUE) Title
WHERE GET(OfficersInfo.value, '@') = 'OfficersInfo'
  AND GET(Officer.value, '@') = 'Officer'
  AND GET(Designation.value, '@') = 'Designation'
  AND GET(Title.value, '@') = 'Title';
