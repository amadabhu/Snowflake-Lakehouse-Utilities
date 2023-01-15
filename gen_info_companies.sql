CREATE TABLE REFINITIV.STAGING.GEN_INFO_COMPANIES AS
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
