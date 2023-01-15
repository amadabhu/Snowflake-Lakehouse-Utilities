COPY INTO REFINITIV.STAGING.GI_COMPANIES_XML FROM @REFINITIV.STAGING.Stage_GenInfo on_error = 'Continue';

TRUNCATE REFINITIV.STAGING.GEN_INFO_AUDITORS;

INSERT INTO REFINITIV.STAGING.GEN_INFO_AUDITORS
SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING              AS "RepNo"
     , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING AS "OrganizationPermID"
     , GET(XMLGET(XML, 'CompanyName'), '$')::STRING        AS "CompanyName"
     , GET(XMLGET(XML, 'CompanyName'), '@Type')::STRING    AS "CompanyNameType"
     , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME   AS "ProductionDate"
     , GET(Auditor.value, '@Code')::STRING                 AS "AuditorCode"
     , GET(Auditor.value, '@Name')::STRING                 AS "AuditorName"
     , CURRENT_TIMESTAMP                                   AS "DataWarehouseInsertTime"
FROM REFINITIV.STAGING.GI_COMPANIES_XML
   , LATERAL FLATTEN(xml, RECURSIVE => TRUE) Advisors
   , LATERAL FLATTEN(Advisors.value, RECURSIVE => TRUE) Auditor
WHERE GET(Advisors.value, '@') = 'Advisors'
  AND GET(Auditor.value, '@') = 'Auditor';

TRUNCATE REFINITIV.STAGING.GEN_INFO_COMPANIES;

INSERT INTO REFINITIV.STAGING.GEN_INFO_COMPANIES
SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING                                                  AS "RepNo"
     , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING                                     AS "OrganizationPermID"
     , GET(XMLGET(XML, 'CompanyName'), '$')::STRING                                            AS "CompanyName"
     , GET(XMLGET(XML, 'CompanyName'), '@Type')::STRING                                        AS "CompanyNameType"
     , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME                                       AS "ProductionDate"
     , GET(XMLGET(XMLGET(XML, 'CompanyGeneralInfo'), 'Employees'), '$')::NUMBER                AS "Employees"
     , GET(XMLGET(XMLGET(XML, 'CompanyGeneralInfo'), 'Employees'), '@LastUpdated')::DATE       AS "EmployeesLastUpdated"
     , GET(XMLGET(XMLGET(XML, 'CompanyGeneralInfo'), 'TotalSharesOut'),
           '$')::NUMBER                                                                        AS "TotalSharesOutstanding"
     , GET(XMLGET(XMLGET(XML, 'CompanyGeneralInfo'), 'TotalSharesOut'),
           '@Date')::DATE                                                                      AS "TotalSharesOutstandingDate"
     , GET(XMLGET(XMLGET(XML, 'CompanyGeneralInfo'), 'TotalSharesOut'), '@TotalFloat')::NUMBER AS "TotalFloat"
     , GET(XMLGET(XMLGET(XML, 'CompanyGeneralInfo'), 'CommonShareholders'), '$')::NUMBER       AS "CommonShareholders"
     , GET(XMLGET(XMLGET(XML, 'CompanyGeneralInfo'), 'CommonShareholders'),
           '@Date')::DATE                                                                      AS "CommonShareholdersDate"
     , GET(XMLGET(XMLGET(XML, 'CompanyGeneralInfo'), 'IncorporatedIn'), '@Date')::STRING       AS "IncorporationDate"
     , GET(XMLGET(XMLGET(XML, 'CompanyGeneralInfo'), 'IncorporatedIn'),
           '@Country')::STRING                                                                 AS "CountryIncorporatedIn"
     , GET(XMLGET(XMLGET(XML, 'CompanyGeneralInfo'), 'PublicSince'), '$')::STRING              AS "PublicSince"
     , CURRENT_TIMESTAMP                                                                       AS "DataWarehouseInsertTime"
FROM REFINITIV.STAGING.GI_COMPANIES_XML;

TRUNCATE REFINITIV.STAGING.GEN_INFO_CONTACT_INFO;

INSERT INTO REFINITIV.STAGING.GEN_INFO_CONTACT_INFO
(WITH 
    gen_contact_info AS
    (SELECT GET(XMLGET(XML,'RepNo'),'$')::STRING AS "RepNo"
          ,GET(XMLGET(XML,'OrganizationPermID'),'$')::STRING AS "OrganizationPermID"
          ,GET(XMLGET(XML, 'CompanyName'),'$')::STRING AS "CompanyName"
          ,GET(XMLGET(XML, 'CompanyName'),'@Type')::STRING AS "CompanyNameType"
          ,GET(XMLGET(XML, 'Production'),'@Date')::DATETIME AS "ProductionDate"
          ,GET(XMLGET(XMLGET(ContactInfo.value,'ContactPerson'),'ContactName'),'$')::STRING AS "ContactName"
          ,GET(XMLGET(XMLGET(ContactInfo.value,'ContactPerson'),'ContactTitle'),'$')::STRING AS "ContactTitle"
          ,GET(XMLGET(ContactInfo.value,'EMail'),'$')::STRING AS "EMail"      
          ,GET(XMLGET(Address.value,'City'),'$')::STRING AS "City"
          ,GET(XMLGET(Address.value,'StateOrRegion'),'$')::STRING AS "StateOrRegion"
          ,GET(XMLGET(Address.value,'PostalCode'),'$')::STRING AS "PostalCode"
          ,GET(XMLGET(Address.value,'Country'),'$')::STRING AS "Country"      
          ,GET(XMLGET(Address.value,'Country'),'@Code')::STRING AS "CountryCode"
    FROM REFINITIV.STAGING.GI_COMPANIES_XML
    ,LATERAL FLATTEN(xml, RECURSIVE => TRUE) ContactInfo
    ,LATERAL FLATTEN(ContactInfo.value, RECURSIVE => TRUE) Address
    WHERE GET(ContactInfo.value, '@') = 'ContactInfo' AND
          GET(Address.value, '@') = 'Address'),     
    adress_streets AS 
    (SELECT REPNO AS "RepNo"
          ,ORGANIZATIONPERMID AS "OrganizationPermID"
          ,STREETADDRESSLINE1 AS "StreetAddressLine1"
          ,STREETADDRESSLINE2 AS "StreetAddressLine2"
          ,STREETADDRESSLINE3 AS "StreetAddressLine3"
    FROM ( SELECT GET(XMLGET(XML,'RepNo'),'$')::STRING AS "RepNo"
          ,GET(XMLGET(XML,'OrganizationPermID'),'$')::STRING AS "OrganizationPermID"
          ,'StreetAddressLine'||GET(StreetAddress.value,'@Line')::STRING AS "StreetAddressLineNumber"
          ,GET(StreetAddress.value,'$')::STRING AS "StreetAddressLine"
    FROM REFINITIV.STAGING.GI_COMPANIES_XML
    ,LATERAL FLATTEN(xml, RECURSIVE => TRUE) ContactInfo
    ,LATERAL FLATTEN(ContactInfo.value, RECURSIVE => TRUE) Address
    ,LATERAL FLATTEN(Address.value, RECURSIVE => TRUE) StreetAddress
    WHERE GET(ContactInfo.value, '@') = 'ContactInfo' AND
          GET(Address.value, '@') = 'Address'AND
          GET(StreetAddress.value,'@') = 'StreetAddress')
          PIVOT(MAX("StreetAddressLine") FOR "StreetAddressLineNumber" IN ('StreetAddressLine1', 'StreetAddressLine2','StreetAddressLine3')) AS p (
            REPNO
           ,ORGANIZATIONPERMID
           ,STREETADDRESSLINE1
           ,STREETADDRESSLINE2
           ,STREETADDRESSLINE3)),
    contact AS
    (SELECT
     "RepNo"
    ,"OrganizationPermID"
    ,"CompanyName"
    ,"CompanyNameType"
    ,"ProductionDate"
    ,"ContactName"
    ,"ContactTitle"
    ,"EMail"      
    ,"StreetAddressLine1"
    ,"StreetAddressLine2" 
    ,"StreetAddressLine3"
    ,"City"
    ,"StateOrRegion"
    ,"PostalCode"
    ,"Country"
    ,"CountryCode"
    , CASE WHEN NULLIF("StreetAddress",'') IS NOT NULL AND NULLIF("CityAndRegionAddress",'') IS NOT NULL AND NULLIF("CountryAddress",'') IS NOT NULL THEN            
                     "StreetAddress"||'\n'||"CityAndRegionAddress"||'\n'||"CountryAddress"
                WHEN NULLIF("StreetAddress",'') IS NULL AND NULLIF("CityAndRegionAddress",'') IS NOT NULL AND NULLIF("CountryAddress",'') IS NOT NULL THEN
                     "CityAndRegionAddress"||'\n'||"CountryAddress"
                WHEN NULLIF("StreetAddress",'') IS NOT NULL AND NULLIF("CityAndRegionAddress",'') IS NULL AND NULLIF("CountryAddress",'') IS NOT NULL THEN
                     "StreetAddress"||'\n'||"CountryAddress"                 
                WHEN NULLIF("StreetAddress",'') IS NULL AND NULLIF("CityAndRegionAddress",'') IS NULL AND NULLIF("CountryAddress",'') IS NOT NULL THEN   
                      "CountryAddress"
                WHEN NULLIF("StreetAddress",'') IS NOT NULL AND NULLIF("CityAndRegionAddress",'') IS NOT NULL AND NULLIF("CountryAddress",'') IS NULL THEN   
                      "StreetAddress"||'\n'||"CityAndRegionAddress"
                WHEN NULLIF("StreetAddress",'') IS NULL AND NULLIF("CityAndRegionAddress",'') IS NOT NULL AND NULLIF("CountryAddress",'') IS NULL THEN   
                      "CityAndRegionAddress"      
            END AS "AddressText"
    FROM (        
    SELECT 
           B."RepNo"
          ,B."OrganizationPermID"
          ,B."CompanyName"
          ,B."CompanyNameType"
          ,B."ProductionDate"
          ,B."ContactName"
          ,B."ContactTitle"
          ,B."EMail"       
          ,NULLIF(A."StreetAddressLine1",'') AS "StreetAddressLine1"
          ,NULLIF(A."StreetAddressLine2",'') AS "StreetAddressLine2"
          ,NULLIF(A."StreetAddressLine3",'') AS "StreetAddressLine3"
          ,NULLIF(B."City",'') AS "City"
          ,NULLIF(B."StateOrRegion",'') AS "StateOrRegion"
          ,NULLIF(B."PostalCode",'') AS "PostalCode"
          ,NULLIF(B."Country",'') AS "Country"
          ,NULLIF(B."CountryCode",'') AS "CountryCode"
          ,ARRAY_TO_STRING(ARRAY_COMPACT(ARRAY_CONSTRUCT(NULLIF(A."StreetAddressLine1",''),
                                                         NULLIF(A."StreetAddressLine2",''),
                                                         NULLIF(A."StreetAddressLine3",''))),'\n') AS "StreetAddress"
          ,ARRAY_TO_STRING(ARRAY_COMPACT(ARRAY_CONSTRUCT(INITCAP(NULLIF("City",'')),
                                                         NULLIF("StateOrRegion",''),
                                                         NULLIF("PostalCode",''))),', ') AS "CityAndRegionAddress"
          ,ARRAY_TO_STRING(ARRAY_COMPACT(ARRAY_CONSTRUCT(INITCAP("Country"),"CountryCode")),', ') AS "CountryAddress"                                                                                                                                                         
          FROM adress_streets A
    INNER JOIN gen_contact_info B ON A."OrganizationPermID" = B."OrganizationPermID" AND A."RepNo" = B."RepNo"))      
SELECT * FROM contact);

TRUNCATE REFINITIV.STAGING.GEN_INFO_INDEX_MEMBERSHIPS;

INSERT INTO REFINITIV.STAGING.GEN_INFO_INDEX_MEMBERSHIPS
SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING              AS "RepNo"
     , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING AS "OrganizationPermID"
     , GET(XMLGET(XML, 'CompanyName'), '$')::STRING        AS "CompanyName"
     , GET(XMLGET(XML, 'CompanyName'), '@Type')::STRING    AS "CompanyNameType"
     , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME   AS "ProductionDate"
     , GET(ConstituentOf.value, '@IndexRIC')::STRING       AS "IndexRIC"
     , GET(ConstituentOf.value, '$')::STRING               AS "Index"
     , CURRENT_TIMESTAMP                                   AS "DataWarehouseInsertTime"
FROM REFINITIV.STAGING.GI_COMPANIES_XML
   , LATERAL FLATTEN(xml, RECURSIVE => TRUE) IndexMemberships
   , LATERAL FLATTEN(IndexMemberships.value, RECURSIVE => TRUE) ConstituentOf
WHERE GET(IndexMemberships.value, '@') = 'IndexMemberships'
  AND GET(ConstituentOf.value, '@') = 'ConstituentOf';
  
TRUNCATE REFINITIV.STAGING.GEN_INFO_INDUSTRY_CLASSIFICATION;
  
INSERT INTO REFINITIV.STAGING.GEN_INFO_INDUSTRY_CLASSIFICATION
SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING              AS "RepNo"
     , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING AS "OrganizationPermID"
     , GET(XMLGET(XML, 'CompanyName'), '$')::STRING        AS "CompanyName"
     , GET(XMLGET(XML, 'CompanyName'), '@Type')::STRING    AS "CompanyNameType"
     , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME   AS "ProductionDate"
     , GET(Taxonomy.value, '@Type')::STRING                AS "IndustrialClassificationTaxonomyType"
     , GET(Detail.value, '@Code')::STRING                  AS "Code"
     , GET(Detail.value, '@Description')::STRING           AS "Description"
     , GET(Detail.value, '@Order')::STRING                 AS "Order"
     , CURRENT_TIMESTAMP                                   AS "DataWarehouseInsertTime"
FROM REFINITIV.STAGING.GI_COMPANIES_XML
   , LATERAL FLATTEN(xml, RECURSIVE => TRUE) IndustryClassification
   , LATERAL FLATTEN(IndustryClassification.value, RECURSIVE => TRUE) Taxonomy
   , LATERAL FLATTEN(Taxonomy.value, RECURSIVE => TRUE) Detail
WHERE GET(IndustryClassification.value, '@') = 'IndustryClassification'
  AND GET(Taxonomy.value, '@') = 'Taxonomy'
  AND GET(Detail.value, '@') = 'Detail';

TRUNCATE REFINITIV.STAGING.GEN_INFO_ISSUE;

INSERT INTO REFINITIV.STAGING.GEN_INFO_ISSUE
(WITH issue_xrefs AS (
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
                       A."IssueID" = B."IssueID");

TRUNCATE REFINITIV.STAGING.GEN_INFO_OFFICER_NAMES;

INSERT INTO REFINITIV.STAGING.GEN_INFO_OFFICER_NAMES
SELECT REPNO              AS "RepNo"
     , ORGANIZATIONPERMID AS "OrganizationPermID"
     , COMPANYNAME        AS "CompanyName"
     , COMPANYNAMETYPE    AS "CompanyNameType"
     , PRODUCTIONDATE     AS "ProductionDate"
     , LANGUAGE           AS "Language"
     , ID                 AS "ID"
     , PERSONPERMID       AS "PersonPermID"
     , OFFICERPERMID      AS "OfficerPermID"
     , RANK               AS "Rank"
     , STATUS             AS "Status"
     , PERSONID           AS "PersonID"
     , SUBMISSIONTYPE     AS "SubmissionType"
     , SUBMISSIONDAY      AS "SubmissionDay"
     , SUBMISSIONMONTH    AS "SubmissionMonth"
     , SUBMISSIONYEAR     AS "SubmissionYear"
     , LASTNAME           AS "LastName"
     , FIRSTNAME          AS "FirstName"
     , SUFFIX             AS "Suffix"
     , PREFIX             AS "Prefix"
     , PREFERREDNAME      AS "PreferredName"
     , MIDDLE_OR_INITIAL  AS "Middle/Initial"
     , SEX                AS "Sex"
     , AGE                AS "Age"
FROM (
         SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING                     AS "RepNo"
              , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING        AS "OrganizationPermID"
              , GET(XMLGET(XML, 'CompanyName'), '$')::STRING               AS "CompanyName"
              , GET(XMLGET(XML, 'CompanyName'), '@Type')::STRING           AS "CompanyNameType"
              , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME          AS "ProductionDate"
              , GET(Officer.value, '@lang')::STRING                        AS "Language"
              , GET(Officer.value, '@ID')::STRING                          AS "ID"
              , GET(Officer.value, '@PersonPermID')::STRING                AS "PersonPermID"
              , GET(Officer.value, '@OfficerPermID')::STRING               AS "OfficerPermID"
              , GET(Officer.value, '@Rank')::STRING                        AS "Rank"
              , GET(Officer.value, '@Status')::STRING                      AS "Status"
              , GET(Officer.value, '@PersonID')::STRING                    AS "PersonID"
              , GET(XMLGET(Officer.value, 'Submission'), '@Type')::STRING  AS "SubmissionType"
              , GET(XMLGET(Officer.value, 'Submission'), '@Day')::STRING   AS "SubmissionDay"
              , GET(XMLGET(Officer.value, 'Submission'), '@Month')::STRING AS "SubmissionMonth"
              , GET(XMLGET(Officer.value, 'Submission'), '@Year')::STRING  AS "SubmissionYear"
              , GET(Info.value, '@Type')::STRING                           AS "InfoType"
              , GET(Info.value, '$')::STRING                               AS "InfoValue"
         FROM REFINITIV.STAGING.GI_COMPANIES_XML
            , LATERAL FLATTEN(xml, RECURSIVE => TRUE) OfficersInfo
            , LATERAL FLATTEN(OfficersInfo.value, RECURSIVE => TRUE) Officer
            , LATERAL FLATTEN(Officer.value, RECURSIVE => TRUE) Info
         WHERE GET(OfficersInfo.value, '@') = 'OfficersInfo'
           AND GET(Officer.value, '@') = 'Officer'
           AND GET(Info.value, '@') = 'Info')
         PIVOT (MAX("InfoValue") FOR "InfoType" IN ('LastName','FirstName','Suffix','Prefix','PreferredName','Middle/Initial','Sex','Age')) p(REPNO,
                                                                                                                                              ORGANIZATIONPERMID,
                                                                                                                                              COMPANYNAME,
                                                                                                                                              COMPANYNAMETYPE,
                                                                                                                                              PRODUCTIONDATE,
                                                                                                                                              LANGUAGE,
                                                                                                                                              ID,
                                                                                                                                              PERSONPERMID,
                                                                                                                                              OFFICERPERMID,
                                                                                                                                              RANK,
                                                                                                                                              STATUS,
                                                                                                                                              PERSONID,
                                                                                                                                              SUBMISSIONTYPE,
                                                                                                                                              SUBMISSIONDAY,
                                                                                                                                              SUBMISSIONMONTH,
                                                                                                                                              SUBMISSIONYEAR,
                                                                                                                                              LASTNAME,
                                                                                                                                              FIRSTNAME,
                                                                                                                                              SUFFIX,
                                                                                                                                              PREFIX,
                                                                                                                                              PREFERREDNAME,
                                                                                                                                              MIDDLE_OR_INITIAL,
                                                                                                                                              SEX,
                                                                                                                                              AGE);

TRUNCATE REFINITIV.STAGING.GEN_INFO_OFFICER_TENURE_DATES;

INSERT INTO REFINITIV.STAGING.GEN_INFO_OFFICER_TENURE_DATES
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

TRUNCATE REFINITIV.STAGING.GEN_INFO_OFFICER_TITLES;

INSERT INTO REFINITIV.STAGING.GEN_INFO_OFFICER_TITLES
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

TRUNCATE REFINITIV.STAGING.GEN_INFO_PHONE_NUMBERS;

INSERT INTO REFINITIV.STAGING.GEN_INFO_PHONE_NUMBERS
SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING                    AS "RepNo"
     , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING       AS "OrganizationPermID"
     , GET(XMLGET(XML, 'CompanyName'), '$')::STRING              AS "CompanyName"
     , GET(XMLGET(XML, 'CompanyName'), '@Type')::STRING          AS "CompanyNameType"
     , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME         AS "ProductionDate"
     , GET(Phone.value, '@Type')::STRING                         AS "PhoneType"
     , GET(XMLGET(Phone.value, 'CountryPhoneCode'), '$')::STRING AS "CountryPhoneCode"
     , GET(XMLGET(Phone.value, 'CityAreaCode'), '$')::STRING     AS "CityAreaCode"
     , GET(XMLGET(Phone.value, 'Number'), '$')::STRING           AS "Number"
FROM REFINITIV.STAGING.GI_COMPANIES_XML
   , LATERAL FLATTEN(xml, RECURSIVE => TRUE) ContactInfo
   , LATERAL FLATTEN(ContactInfo.value, RECURSIVE => TRUE) PhoneNumbers
   , LATERAL FLATTEN(PhoneNumbers.value, RECURSIVE => TRUE) Phone
WHERE GET(ContactInfo.value, '@') = 'ContactInfo'
  AND GET(PhoneNumbers.value, '@') = 'PhoneNumbers'
  AND GET(Phone.value, '@') = 'Phone';

TRUNCATE REFINITIV.STAGING.GEN_INFO_TEXT;

INSERT INTO REFINITIV.STAGING.GEN_INFO_TEXT
SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING              AS "RepNo"
     , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING AS "OrganizationPermID"
     , GET(XMLGET(XML, 'CompanyName'), '$')::STRING        AS "CompanyName"
     , GET(XMLGET(XML, 'CompanyName'), '@Type')::STRING    AS "CompanyNameType"
     , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME   AS "ProductionDate"
     , GET(Text.value, '@Type')::STRING                    AS "TextType"
     , GET(Text.value, '@LastUpdated')::STRING             AS "TextLastUpdated"
     , GET(Text.value, '@Lang')::STRING                    AS "TextLanguage"
     , GET(Text.value, '@SourceFilingType')::STRING        AS "TextSourceFilingType"
     , GET(Text.value, '@SourceFilingDate')::DATETIME      AS "SourceFilingDate"
     , GET(Text.value, '$')::STRING                        AS "Text"
     , CURRENT_TIMESTAMP                                   AS "DataWarehouseInsertTime"
FROM REFINITIV.STAGING.GI_COMPANIES_XML
   , LATERAL FLATTEN(xml, RECURSIVE => TRUE) TextInfo
   , LATERAL FLATTEN(TextInfo.value, RECURSIVE => TRUE) Text
WHERE GET(TextInfo.value, '@') = 'TextInfo'
  AND GET(Text.value, '@') = 'Text';

TRUNCATE REFINITIV.STAGING.GEN_INFO_WEB_LINKS;

INSERT INTO REFINITIV.STAGING.GEN_INFO_WEB_LINKS
SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING              AS "RepNo"
     , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING AS "OrganizationPermID"
     , GET(XMLGET(XML, 'CompanyName'), '$')::STRING        AS "CompanyName"
     , GET(XMLGET(XML, 'CompanyName'), '@Type')::STRING    AS "CompanyNameType"
     , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME   AS "ProductionDate"
     , GET(WebLinksInfo.value, '@LastUpdated')::DATETIME   AS "WebLinkInfoLastUpdated"
     , GET(WebSite.value, '@Type')::STRING                 AS "WebSiteType"
     , GET(WebSite.value, '$')::STRING                     AS "WebSite"
     , CURRENT_TIMESTAMP                                   AS "DataWarehouseInsertTime"
FROM REFINITIV.STAGING.GI_COMPANIES_XML
   , LATERAL FLATTEN(xml, RECURSIVE => TRUE) WebLinksInfo
   , LATERAL FLATTEN(WebLinksInfo.value, RECURSIVE => TRUE) WebSite
WHERE GET(WebLinksInfo.value, '@') = 'WebLinksInfo'
  AND GET(WebSite.value, '@') = 'WebSite';
  
TRUNCATE REFINITIV.PUBLIC.GEN_INFO_AUDITORS;

INSERT INTO REFINITIV.PUBLIC.GEN_INFO_AUDITORS
SELECT * FROM REFINITIV.STAGING.GEN_INFO_AUDITORS;

TRUNCATE REFINITIV.PUBLIC.GEN_INFO_COMPANIES;

INSERT INTO REFINITIV.PUBLIC.GEN_INFO_COMPANIES
SELECT * FROM REFINITIV.STAGING.GEN_INFO_COMPANIES;

TRUNCATE REFINITIV.PUBLIC.GEN_INFO_CONTACT_INFO;

INSERT INTO REFINITIV.PUBLIC.GEN_INFO_CONTACT_INFO
SELECT * FROM REFINITIV.STAGING.GEN_INFO_CONTACT_INFO;

TRUNCATE REFINITIV.PUBLIC.GEN_INFO_INDEX_MEMBERSHIPS;

INSERT INTO REFINITIV.PUBLIC.GEN_INFO_INDEX_MEMBERSHIPS
SELECT * FROM REFINITIV.STAGING.GEN_INFO_INDEX_MEMBERSHIPS;

TRUNCATE REFINITIV.PUBLIC.GEN_INFO_INDUSTRY_CLASSIFICATION;

INSERT INTO REFINITIV.PUBLIC.GEN_INFO_INDUSTRY_CLASSIFICATION
SELECT * FROM REFINITIV.STAGING.GEN_INFO_INDUSTRY_CLASSIFICATION;

TRUNCATE REFINITIV.PUBLIC.GEN_INFO_ISSUE;

INSERT INTO REFINITIV.PUBLIC.GEN_INFO_ISSUE
SELECT * FROM REFINITIV.STAGING.GEN_INFO_ISSUE;

TRUNCATE REFINITIV.PUBLIC.GEN_INFO_OFFICER_NAMES;

INSERT INTO REFINITIV.PUBLIC.GEN_INFO_OFFICER_NAMES
SELECT * FROM REFINITIV.STAGING.GEN_INFO_OFFICER_NAMES;

TRUNCATE REFINITIV.PUBLIC.GEN_INFO_OFFICER_TENURE_DATES;

INSERT INTO REFINITIV.PUBLIC.GEN_INFO_OFFICER_TENURE_DATES
SELECT * FROM REFINITIV.STAGING.GEN_INFO_OFFICER_TENURE_DATES;

TRUNCATE REFINITIV.PUBLIC.GEN_INFO_OFFICER_TITLES;

INSERT INTO REFINITIV.PUBLIC.GEN_INFO_OFFICER_TITLES
SELECT * FROM REFINITIV.STAGING.GEN_INFO_OFFICER_TITLES;

TRUNCATE REFINITIV.PUBLIC.GEN_INFO_PHONE_NUMBERS;

INSERT INTO REFINITIV.PUBLIC.GEN_INFO_PHONE_NUMBERS
SELECT * FROM REFINITIV.STAGING.GEN_INFO_PHONE_NUMBERS;

TRUNCATE REFINITIV.PUBLIC.GEN_INFO_TEXT;

INSERT INTO REFINITIV.PUBLIC.GEN_INFO_TEXT
SELECT * FROM REFINITIV.STAGING.GEN_INFO_TEXT;

TRUNCATE REFINITIV.PUBLIC.GEN_INFO_WEB_LINKS;

INSERT INTO REFINITIV.PUBLIC.GEN_INFO_WEB_LINKS
SELECT * FROM REFINITIV.STAGING.GEN_INFO_WEB_LINKS;