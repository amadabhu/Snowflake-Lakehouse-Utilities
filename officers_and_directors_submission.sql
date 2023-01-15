CREATE TABLE REFINITIV.STAGING.OFFICERS_AND_DIRECTORS_SUBMISSION
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
     , "SubmissionType"
     , "SubmissionDay"
     , "SubmissionMonth"
     , "SubmissionYear"
     , current_timestamp  AS "DataWareHouseInsertTime"
FROM (
         SELECT "RepNo"
              , "OrganizationPermID"
              , "CompanyName"
              , "Production"
              , GET(officer.value, '@OfficerPermID')::STRING                  AS "OfficerPermID"
              , GET(officer.value, '@Active')::STRING                         AS "Active"
              , GET(officer.value, '@Status')::STRING                         AS "Status"
              , GET(officer.value, '@Rank')::STRING                           AS "Rank"
              , GET(officer.value, '@ID')::STRING                             AS "ID"
              , regexp_count(officer.path, '\\.|\\[') + 1                     as L1
              , regexp_count(submissions.path, '\\.|\\[') + 1                 as L2
              , GET(XMLGET(officer.value, 'Person'), '@ID')::STRING           AS "PersonID"
              , GET(XMLGET(officer.value, 'Person'), '@PersonPermID')::STRING AS "PersonPermID"
              , GET(XMLGET(officer.value, 'Person'), '@Active')::STRING       AS "PersonActive"
              , GET(submissions.value, '@Type')::STRING                       AS "SubmissionType"
              , GET(submissions.value, '@Year')::STRING                       AS "SubmissionYear"
              , GET(submissions.value, '@Month')::STRING                      AS "SubmissionMonth"
              , GET(submissions.value, '@Day')::STRING                        AS "SubmissionDay"
         FROM (
                  SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING              AS "RepNo"
                       , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING AS "OrganizationPermID"
                       , GET(XMLGET(XML, 'CompanyName'), '$')::STRING        AS "CompanyName"
                       , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME   AS "Production"
                       , XML
                  FROM REFINITIV.STAGING.OFFICERS_AND_DIRECTORS_XML),
              LATERAL FLATTEN(input => XML, recursive => true) as officer,
              LATERAL FLATTEN(input => officer.value, recursive => true) as submissions
         WHERE GET(officer.value, '@') = 'Officer'
           AND (L1 = 4 OR L1 = 5)
           AND GET(submissions.value, '@') = 'Submission'
           AND L2 = 3);
