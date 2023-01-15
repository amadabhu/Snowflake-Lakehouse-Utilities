CREATE TABLE REFINITIV.STAGING.GEN_INFO_OFFICER_NAMES AS
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
