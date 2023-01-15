CREATE TABLE REFINITIV.STAGING.OFFICERS_AND_DIRECTORS_TITLES
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
     , "DesignationStartDay"
     , "DesignationStartMonth"
     , "DesignationStartYear"
     , "DesignationEndDay"
     , "DesignationEndMonth"
     , "DesignationEndYear"
     , "LongTitle"
     , "TitleOrder"
     , "TitleID"
     , "Title"
     , current_timestamp  AS "DataWareHouseInsertTime"
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
              , GET(XMLGET(Designation.value, 'Start'), '@Day')::STRING       AS "DesignationStartDay"
              , GET(XMLGET(Designation.value, 'Start'), '@Month')::STRING     AS "DesignationStartMonth"
              , GET(XMLGET(Designation.value, 'Start'), '@Year')::STRING      AS "DesignationStartYear"
              , GET(XMLGET(Designation.value, 'End'), '@Day')::STRING         AS "DesignationEndDay"
              , GET(XMLGET(Designation.value, 'End'), '@Month')::STRING       AS "DesignationEndMonth"
              , GET(XMLGET(Designation.value, 'End'), '@Year')::STRING        AS "DesignationEndYear"
              , GET(XMLGET(Designation.value, 'LongTitle'), '$')::STRING      AS "LongTitle"
              , GET(Title.value, '@Order')::STRING                            AS "TitleOrder"
              , GET(Title.value, '@ID')::STRING                               AS "TitleID"
              , GET(Title.value, '$')::STRING                                 AS "Title"
         FROM REFINITIV.STAGING.OFFICERS_XML
            , LATERAL FLATTEN(input => XML, recursive => true) as Officer
            , LATERAL FLATTEN(Officer.value, recursive => true) as PositionInformation
            , LATERAL FLATTEN(PositionInformation.value, recursive => true) as Titles
            , LATERAL FLATTEN(Titles.value, recursive => true) as Designation
            , LATERAL FLATTEN(Designation.value, recursive => true) as Title
         WHERE GET(officer.value, '@') = 'Officer'
           AND (L = 4 OR L = 5)
           AND GET(PositionInformation.value, '@') = 'PositionInformation'
           AND GET(Titles.value, '@') = 'Titles'
           AND GET(Designation.value, '@') = 'Designation'
           AND GET(Title.value, '@') = 'Title');
