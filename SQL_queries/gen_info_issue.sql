CREATE TABLE REFINITIV.STAGING.GEN_INFO_ISSUE AS
WITH issue_xrefs AS (
    SELECT REPNO              AS "RepNo"
         , ORGANIZATIONPERMID AS "OrganizationPermID"
         , ISSUETYPE          AS "IssueType"
         , ISSUEORDER         AS "IssueOrder"
         , ISSUEID            AS "IssueID"
         , INSTRUMENTPERMID   AS "InstrumentPermID"
         , QUOTEPERMID        AS "QuotePermID"
    FROM (
             SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING              AS "RepNo"
                  , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING AS "OrganizationPermID"
                  , GET(Issue.value, '@Type')::STRING                   AS "IssueType"
                  , GET(Issue.value, '@Order')::STRING                  AS "IssueOrder"
                  , GET(Issue.value, '@ID')::STRING                     AS "IssueID"
                  , GET(IssueXref.value, '@Type')::STRING               AS "IssueXrefType"
                  , GET(IssueXref.value, '$')::STRING                   AS "IssueXref"
             FROM REFINITIV.STAGING.GI_COMPANIES_XML
                , LATERAL FLATTEN(xml, RECURSIVE => TRUE) IssueInformation
                , LATERAL FLATTEN(IssueInformation.value, RECURSIVE => TRUE) Issue
                , LATERAL FLATTEN(Issue.value, RECURSIVE => TRUE) IssueXref
             WHERE GET(IssueInformation.value, '@') = 'IssueInformation'
               AND GET(Issue.value, '@') = 'Issue'
               AND GET(IssueXref.value, '@') = 'IssueXref')
             PIVOT (MAX("IssueXref") FOR "IssueXrefType" IN ('InstrumentPermID', 'QuotePermID')) AS p (
                                                                                                       REPNO,
                                                                                                       ORGANIZATIONPERMID,
                                                                                                       ISSUETYPE,
                                                                                                       ISSUEORDER,
                                                                                                       ISSUEID,
                                                                                                       INSTRUMENTPERMID,
                                                                                                       QUOTEPERMID)),
     issue_details AS (
         SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING               AS "RepNo"
              , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING  AS "OrganizationPermID"
              , GET(XMLGET(XML, 'CompanyName'), '$')::STRING         AS "CompanyName"
              , GET(XMLGET(XML, 'CompanyName'), '@Type')::STRING     AS "CompanyNameType"
              , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME    AS "ProductionDate"
              , GET(Issue.value, '@Type')::STRING                    AS "IssueType"
              , GET(Issue.value, '@Order')::STRING                   AS "IssueOrder"
              , GET(Issue.value, '@ID')::STRING                      AS "IssueID"
              , GET(IssueDetails.value, '@ParCurrency')::STRING      AS "ParCurrency"
              , GET(IssueDetails.value, '@ParValue')::STRING         AS "ParValue"
              , GET(IssueDetails.value, '@ConversionFactor')::STRING AS "ConversionFactor"
              , GET(IssueDetails.value, '@Votes')::STRING            AS "Votes"
              , GET(IssueDetails.value, '@ShsTreasDate')::DATE       AS "TreasurySharesDate"
              , GET(IssueDetails.value, '@ShsTreas')::STRING         AS "TreasuryShares"
              , GET(IssueDetails.value, '@ShsIssuedDate')::DATE      AS "SharesIssuedDate"
              , GET(IssueDetails.value, '@ShsIssued')::NUMBER        AS "SharesIssued"
              , GET(IssueDetails.value, '@FloatDate')::DATE          AS "FloatDate"
              , GET(IssueDetails.value, '@Float')::NUMBER            AS "Float"
              , GET(IssueDetails.value, '@ShsOutDate')::DATE         AS "SharesOutstandingDate"
              , GET(IssueDetails.value, '@ShsOut')::NUMBER           AS "SharesOutstanding"
              , GET(IssueDetails.value, '@ShsAuthorizedDate')::DATE  AS "SharesAuthorizedDate"
              , GET(IssueDetails.value, '@ShsAuthorized')::NUMBER    AS "SharesAuthorized"
         FROM REFINITIV.STAGING.GI_COMPANIES_XML
            , LATERAL FLATTEN(xml, RECURSIVE => TRUE) IssueInformation
            , LATERAL FLATTEN(IssueInformation.value, RECURSIVE => TRUE) Issue
            , LATERAL FLATTEN(Issue.value, RECURSIVE => TRUE) IssueDetails
         WHERE GET(IssueInformation.value, '@') = 'IssueInformation'
           AND GET(Issue.value, '@') = 'Issue'
           AND GET(IssueDetails.value, '@') = 'IssueDetails')
SELECT A.*
     , B."InstrumentPermID"
     , B."QuotePermID"
     , CURRENT_TIMESTAMP AS "DataWarehouseInsertTime"
FROM issue_details A
         INNER JOIN issue_xrefs B
                    ON A."RepNo" = B."RepNo" AND
                       A."OrganizationPermID" = B."OrganizationPermID" AND
                       A."IssueType" = B."IssueType" AND
                       A."IssueOrder" = B."IssueOrder" AND
                       A."IssueID" = B."IssueID";
