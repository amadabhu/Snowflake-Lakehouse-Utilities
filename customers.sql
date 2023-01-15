CREATE TABLE REFINITIV.STAGING.CUSTOMERS
AS 
SELECT REPNO AS "RepNo" 
      ,COMPANYNAME AS "CompanyName"
      ,IRSNO AS "IRSNo"
      ,CIKNO AS "CIKNo"
      ,ORGANIZATIONPERMID AS"OrganizationPermID"
      ,FiscalPeriod.value:"@EndDate"::STRING AS "EndDate"
      ,FiscalPeriod.value:"@Type"::STRING AS "Type"
      ,FiscalPeriod.value:"@FiscalYear"::INTEGER as "FiscalYear"
      ,FiscalPeriod.value:"@StatementDate"::DATE as "StatementDate"
      ,GET(XMLGET(FPHeader.value,'PeriodLength'),'$')::INTEGER AS "PeriodLength"
      ,GET(XMLGET(FPHeader.value,'PeriodType'),'$')::STRING AS "PeriodType"
      ,GET(XMLGET(FPHeader.value,'UpdatedType'),'$')::STRING AS "UpdatedType"
      ,GET(XMLGET(FPHeader.value,'Source'),'$')::STRING AS "Source"
      ,GET(XMLGET(FPHeader.value,'SourceFiling'),'@ID')::STRING AS "SourceFilingID"       
      ,CustomerOrderNumber.value:"@Name"::STRING AS "Name"
      ,CustomerOrderNumber.value:"@Revenue"::FLOAT AS "Revenue"
      ,CustomerOrderNumber.value:"@Percent"::FLOAT AS "Percent"
      ,CURRENT_TIMESTAMP AS "DataWarehouseInsertTime"
FROM (SELECT * 
      FROM (SELECT xml, 
                  GET(CoIDs.value,'@Type')::STRING AS "Name"
                 ,GET(CoIDs.value,'$')::STRING AS "Value"
            FROM REFINITIV.STAGING.CUSTOMERS_XML t,
                 LATERAL FLATTEN(GET(xml,'$'),OUTER => TRUE, RECURSIVE => TRUE) CoIDs
                ,LATERAL FLATTEN(GET(CoIDs.value,'$'),RECURSIVE => TRUE) CoID
            WHERE GET(CoIDs.value, '@') = 'CoID')
            PIVOT(MAX("Value") FOR "Name" IN ('RepNo', 'CompanyName', 'IRSNo', 'CIKNo','OrganizationPermID')) AS p (xml,REPNO,COMPANYNAME,IRSNO,CIKNO,ORGANIZATIONPERMID)) t,
     LATERAL FLATTEN(GET(xml,'$'),OUTER => TRUE, RECURSIVE => TRUE) FiscalPeriod
    ,LATERAL FLATTEN(GET(FiscalPeriod.value,'$'), RECURSIVE => TRUE) FPHeader
    ,LATERAL FLATTEN(GET(FiscalPeriod.value,'$'), RECURSIVE => TRUE) CustomerDetails
    ,LATERAL FLATTEN(GET(FiscalPeriod.value,'$'), RECURSIVE => TRUE) CustomerOrderNumber
WHERE
   GET(FiscalPeriod.value, '@') = 'FiscalPeriod'
   AND GET(FPHeader.value,'@') = 'FPHeader'
   AND GET(CustomerDetails.value, '@') = 'CustomerDetails'
   AND GET(CustomerOrderNumber.value, '@') = 'CustomerOrderNumber';
