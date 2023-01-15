CREATE TABLE REFINITIV.STAGING.OFFICERS_AND_DIRECTORS_STD_COMPENSATION AS
SELECT "RepNo"
     , "OrganizationPermID"
     , "CompanyName"
     , "CompanyNameType"
     , "ProductionDate"
     , "OfficerPermID"
     , "CompensationPeriodEndDate"
     , "CompensationSubmissionType"
     , "CompensationSubmissionYear"
     , "CompensationSubmissionMonth"
     , "CompensationSubmissionDay"
     , "CompensationCurrency"
     , SAL
     , BON
     , OAC
     , TAC
     , RSA
     , LTP
     , AOC
     , TLC
     , NEC
     , FYT
     , current_timestamp  AS "DataWareHouseInsertTime"
FROM (
         SELECT RepNo                       AS "RepNo"
              , OrganizationPermID          AS "OrganizationPermID"
              , CompanyName                 AS "CompanyName"
              , CompanyNameType             AS "CompanyNameType"
              , ProductionDate              AS "ProductionDate"
              , OfficerPermID               AS "OfficerPermID"
              , L
              , CompensationPeriodEndDate   AS "CompensationPeriodEndDate"
              , CompensationSubmissionType  AS "CompensationSubmissionType"
              , CompensationSubmissionYear  AS "CompensationSubmissionYear"
              , CompensationSubmissionMonth AS "CompensationSubmissionMonth"
              , CompensationSubmissionDay   AS "CompensationSubmissionDay"
              , CompensationCurrency        AS "CompensationCurrency"
              , SAL
              , BON
              , OAC
              , TAC
              , RSA
              , LTP
              , AOC
              , TLC
              , NEC
              , FYT
         FROM (
                  SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING                                     AS "RepNo"
                       , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING                        AS "OrganizationPermID"
                       , GET(XMLGET(XML, 'CompanyName'), '$')::STRING                               AS "CompanyName"
                       , GET(XMLGET(XML, 'CompanyName'), '@Type')::STRING                           AS "CompanyNameType"
                       , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME                          AS "ProductionDate"
                       , GET(officer.value, '@OfficerPermID')::STRING                               AS "OfficerPermID"
                       , regexp_count(officer.path, '\\.|\\[') + 1                                  as L
                       , GET(CompensationPeriod.value, '@EndDate')::DATE                            AS "CompensationPeriodEndDate"
                       , GET(XMLGET(CompensationPeriod.value, 'Submission'), '@Type')::STRING       AS "CompensationSubmissionType"
                       , GET(XMLGET(CompensationPeriod.value, 'Submission'), '@Year')::STRING       AS "CompensationSubmissionYear"
                       , GET(XMLGET(CompensationPeriod.value, 'Submission'), '@Month')::STRING      AS "CompensationSubmissionMonth"
                       , GET(XMLGET(CompensationPeriod.value, 'Submission'), '@Day')::STRING        AS "CompensationSubmissionDay"
                       , GET(XMLGET(CompensationPeriod.value, 'CompensationCurrency'), '$')::STRING AS "CompensationCurrency"
                       , GET(Compensation.value, '@COA')::STRING                                    AS "CompensationCOA"
                       , GET(Compensation.value, '$')::NUMBER                                       AS "CompensationAmount"
                  FROM REFINITIV.STAGING.OFFICERS_AND_DIRECTORS_XML,
                       LATERAL FLATTEN(input => XML, recursive => true) AS officer,
                       LATERAL FLATTEN(input => officer.value, recursive => true) AS CompensationPeriod,
                       LATERAL FLATTEN(input => CompensationPeriod.value, recursive => true) AS StandardizedCompensation,
                       LATERAL FLATTEN(input => StandardizedCompensation.value, recursive => true) AS Compensation
                  WHERE GET(officer.value, '@') = 'Officer'
                    AND (L = 4 OR L = 5)
                    AND GET(CompensationPeriod.value, '@') = 'CompensationPeriod'
                    AND GET(StandardizedCompensation.value, '@') = 'StandardizedCompensation'
                    AND GET(Compensation.value, '@') = 'Compensation')
                  PIVOT (MAX("CompensationAmount") FOR "CompensationCOA" IN ('SAL', 'BON', 'OAC', 'TAC','RSA','LTP','AOC','TLC','NEC','FYT')) AS p (RepNo,
                                                                                                                                                    OrganizationPermID,
                                                                                                                                                    CompanyName,
                                                                                                                                                    CompanyNameType,
                                                                                                                                                    ProductionDate,
                                                                                                                                                    OfficerPermID,
                                                                                                                                                    L,
                                                                                                                                                    CompensationPeriodEndDate,
                                                                                                                                                    CompensationSubmissionType,
                                                                                                                                                    CompensationSubmissionYear,
                                                                                                                                                    CompensationSubmissionMonth,
                                                                                                                                                    CompensationSubmissionDay,
                                                                                                                                                    CompensationCurrency,
                                                                                                                                                    SAL,
                                                                                                                                                    BON,
                                                                                                                                                    OAC,
                                                                                                                                                    TAC,
                                                                                                                                                    RSA,
                                                                                                                                                    LTP,
                                                                                                                                                    AOC,
                                                                                                                                                    TLC,
                                                                                                                                                    NEC,
                                                                                                                                                    FYT));
