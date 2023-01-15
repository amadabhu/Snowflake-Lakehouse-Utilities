CREATE TABLE REFINITIV.STAGING.OFFICERS_AND_DIRECTORS_PERSON_INFO
AS
SELECT RepNo              AS "RepNo"
     , OrganizationPermID AS "OrganizationPermID"
     , CompanyName        AS "CompanyName"
     , ProductionDate     AS "ProductionDate"
     , OfficerPermID      AS "OfficerPermID"
     , Active             AS "Active"
     , Status             AS "Status"
     , Rank               AS "Rank"
     , ID                 AS "ID"
     , PersonID           AS "PersonID"
     , PersonPermID       AS "PersonPermID"
     , PersonActive       AS "PersonActive"
     , LastModified       AS "LastModified"
     , BirthYear          AS "BirthYear"
     , BirthMonth         AS "BirthMonth"
     , BirthDay           As "BirthDay"
     , PREFIX             AS "Prefix"
     , FIRSTNAME          AS "FirstName"
     , MIDDLE_OR_INITIAL  AS "Middle/Initial"
     , LASTNAME           AS "LastName"
     , SUFFIX             AS "Suffix"
     , PREFERREDNAME      AS "PreferredName"
     , AGE                AS "Age"
     , SEX                AS "Sex"
     , current_timestamp  AS "DataWareHouseInsertTime"
FROM (
         SELECT *
         FROM (
                  SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING                                          AS "RepNo"
                       , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING                             AS "OrganizationPermID"
                       , GET(XMLGET(XML, 'CompanyName'), '$')::STRING                                    AS "CompanyName"
                       , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME                               AS "ProductionDate"
                       , GET(Officer.value, '@OfficerPermID')::STRING                                    AS "OfficerPermID"
                       , GET(Officer.value, '@Active')::STRING                                           AS "Active"
                       , GET(Officer.value, '@Status')::STRING                                           AS "Status"
                       , GET(Officer.value, '@Rank')::STRING                                             AS "Rank"
                       , GET(Officer.value, '@ID')::STRING                                               AS "ID"
                       , regexp_count(officer.path, '\\.|\\[') + 1                                       as l
                       , GET(XMLGET(Officer.value, 'Person'), '@ID')::STRING                             AS "PersonID"
                       , GET(XMLGET(Officer.value, 'Person'), '@PersonPermID')::STRING                   AS "PersonPermID"
                       , GET(XMLGET(Officer.value, 'Person'), '@Active')::STRING                         AS "PersonActive"
                       , GET(XMLGET(PersonInformation.value, 'LastModified'), '@Date')::STRING           AS "LastModified"
                       , GET(XMLGET(XMLGET(PersonInformation.value, 'Name'), 'Birth'), '@Year')::STRING  AS "BirthYear"
                       , GET(XMLGET(XMLGET(PersonInformation.value, 'Name'), 'Birth'), '@Month')::STRING AS "BirthMonth"
                       , GET(XMLGET(XMLGET(PersonInformation.value, 'Name'), 'Birth'), '@Day')::STRING   AS "BirthDay"
                       , CASE
                             WHEN GET(Info.value, '@Type')::STRING = 'Middle/Initial' THEN
                                 'Middle_Or_Initial'
                             ELSE
                                 GET(Info.value, '@Type')::STRING
                      END                                                                                AS "InfoType"
                       , GET(Info.value, '$')::STRING                                                    AS "InfoValue"
                  FROM REFINITIV.STAGING.OFFICERS_AND_DIRECTORS_XML,
                       LATERAL FLATTEN(input => XML, recursive => true) as Officer,
                       LATERAL FLATTEN(input => Officer.value, recursive => true) as PersonInformation,
                       LATERAL FLATTEN(input => PersonInformation.value, recursive => true) as Info
                  WHERE GET(officer.value, '@') = 'Officer'
                    AND (L = 4 OR L = 5)
                    AND GET(PersonInformation.value, '@') = 'PersonInformation'
                    AND GET(Info.value, '@') = 'Info')
                  PIVOT (MAX("InfoValue") FOR "InfoType" IN ('Prefix', 'FirstName', 'Middle_Or_Initial', 'LastName','Suffix','PreferredName','Age','Sex')) AS p (RepNo,
                                                                                                                                                                 OrganizationPermID,
                                                                                                                                                                 CompanyName,
                                                                                                                                                                 ProductionDate,
                                                                                                                                                                 OfficerPermID,
                                                                                                                                                                 Active,
                                                                                                                                                                 Status,
                                                                                                                                                                 Rank,
                                                                                                                                                                 ID,
                                                                                                                                                                 L,
                                                                                                                                                                 PersonID,
                                                                                                                                                                 PersonPermID,
                                                                                                                                                                 PersonActive,
                                                                                                                                                                 LastModified,
                                                                                                                                                                 BirthYear,
                                                                                                                                                                 BirthMonth,
                                                                                                                                                                 BirthDay,
                                                                                                                                                                 PREFIX,
                                                                                                                                                                 FIRSTNAME,
                                                                                                                                                                 MIDDLE_OR_INITIAL,
                                                                                                                                                                 LASTNAME,
                                                                                                                                                                 SUFFIX,
                                                                                                                                                                 PREFERREDNAME,
                                                                                                                                                                 AGE,
                                                                                                                                                                 SEX));
