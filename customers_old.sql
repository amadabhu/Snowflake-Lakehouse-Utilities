CREATE OR REPLACE VIEW REFINITIV.STAGING.CUSTOMERS_STAGING_V
AS 
SELECT GET(XMLGET(XMLGET(t.xml,'CoIDs'),'CoID',0),'$')::STRING AS "RepNo",
       GET(XMLGET(XMLGET(t.xml,'CoIDs'),'CoID',1),'$')::STRING AS "CompanyName",
       GET(XMLGET(XMLGET(t.xml,'CoIDs'),'CoID',2),'$')::STRING AS "OrganizationPermID", 
       FiscalPeriod.value:"@EndDate"::STRING AS "EndDate",
       FiscalPeriod.value:"@Type"::STRING AS "Type",
       FiscalPeriod.value:"@FiscalYear"::INTEGER as "FiscalYear",
       FiscalPeriod.value:"@StatementDate"::DATE as "StatementDate",
       GET(XMLGET(FPHeader.value,'PeriodLength'),'$')::INTEGER AS "PeriodLength",
       GET(XMLGET(FPHeader.value,'PeriodType'),'$')::STRING AS "PeriodType",
       GET(XMLGET(FPHeader.value,'UpdatedType'),'$')::STRING AS "UpdatedType",
       GET(XMLGET(FPHeader.value,'Source'),'$')::STRING AS "Source",
       GET(XMLGET(FPHeader.value,'SourceFiling'),'@ID')::STRING AS "SourceFilingID",       
       CustomerOrderNumber.value:"@Name"::STRING AS "Name",
       CustomerOrderNumber.value:"@Revenue"::FLOAT AS "Revenue",
       CustomerOrderNumber.value:"@Percent"::FLOAT AS "Percent",
       CURRENT_TIMESTAMP AS "DataWarehouseInsertTime"                    
FROM REFINITIV.STAGING.CUSTOMERS_XML t, 
     LATERAL FLATTEN(GET(xml,'$'),OUTER => TRUE, RECURSIVE => TRUE) FiscalPeriod,
     LATERAL FLATTEN(GET(FiscalPeriod.value,'$'),OUTER => TRUE, RECURSIVE => TRUE) FPHeader,
     LATERAL FLATTEN(GET(FiscalPeriod.value,'$'),OUTER => TRUE, RECURSIVE => TRUE) CustomerDetails,
     LATERAL FLATTEN(GET(FiscalPeriod.value,'$'),OUTER => TRUE, RECURSIVE => TRUE) CustomerOrderNumber
WHERE
   GET(FiscalPeriod.value, '@') = 'FiscalPeriod' AND
   GET(FPHeader.value,'@') = 'FPHeader' AND
   GET(CustomerDetails.value, '@') = 'CustomerDetails' AND
   GET(CustomerOrderNumber.value, '@') = 'CustomerOrderNumber' ;
