CREATE TABLE REFINITIV.STAGING.OFFICERS_AND_DIRECTORS_COMMITTEE AS
SELECT "RepNo"
     , "OrganizationPermID"
     , "CompanyName"
     , "CompanyNameType"
     , "ProductionDate"
     , "OfficerPermID"
     , "CommitteeID"
     , "CommitteeName"
     , "CommitteePositionTitle"
     , "CommitteePositionStartYear"
     , "CommitteePositionStartMonth"
     , "CommitteePositionStartDay"
     , "CommitteePositionEndYear"
     , "CommitteePositionEndMonth"
     , "CommitteePositionEndDay"
     , current_timestamp AS "DataWareHouseInsertTime"
FROM (
         SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING                       AS "RepNo"
              , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING          AS "OrganizationPermID"
              , GET(XMLGET(XML, 'CompanyName'), '$')::STRING                 AS "CompanyName"
              , GET(XMLGET(XML, 'CompanyName'), '@Type')::STRING             AS "CompanyNameType"
              , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME            AS "ProductionDate"
              , GET(officer.value, '@OfficerPermID')::STRING                 AS "OfficerPermID"
              , regexp_count(officer.path, '\\.|\\[') + 1                    as L
              , GET(XMLGET(Committee.value, 'CommitteeName'), '@ID')::STRING AS "CommitteeID"
              , GET(XMLGET(Committee.value, 'CommitteeName'), '$')::STRING   AS "CommitteeName"
              , GET(XMLGET(Committee.value, 'Title'), '$')::STRING           AS "CommitteePositionTitle"
              , GET(XMLGET(Committee.value, 'Start'), '@Year')::STRING       AS "CommitteePositionStartYear"
              , GET(XMLGET(Committee.value, 'Start'), '@Month')::STRING      AS "CommitteePositionStartMonth"
              , GET(XMLGET(Committee.value, 'Start'), '@Day')::STRING        AS "CommitteePositionStartDay"
              , GET(XMLGET(Committee.value, 'End'), '@Year')::STRING         AS "CommitteePositionEndYear"
              , GET(XMLGET(Committee.value, 'End'), '@Month')::STRING        AS "CommitteePositionEndMonth"
              , GET(XMLGET(Committee.value, 'End'), '@Day')::STRING          AS "CommitteePositionEndDay"
         FROM REFINITIV.STAGING.OFFICERS_AND_DIRECTORS_XML,
              LATERAL FLATTEN(input => XML, recursive => true) as officer,
              LATERAL FLATTEN(input => officer.value, recursive => true) as Committee
         WHERE GET(officer.value, '@') = 'Officer'
           AND (L = 4 OR L = 5)
           AND GET(Committee.value, '@') = 'Committee');
