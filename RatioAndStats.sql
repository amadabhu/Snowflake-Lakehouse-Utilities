TRUNCATE REFINITIV.STAGING.RATIO_AND_STATISTICS_XML;
          
COPY INTO REFINITIV.STAGING.RATIO_AND_STATISTICS_XML FROM @REFINITIV.STAGING.Stage_RatioAndStats on_error = 'Continue';

CREATE OR REPLACE VIEW REFINITIV.STAGING.RATIO_AND_STATISTICS_STAGING_V AS
WITH ids AS (SELECT REPNO AS "RepNo"
                   ,XML
                   ,ORGANIZATIONPERMID AS "OrganizationPermID"
                   ,IRSNO AS "IRSNo"
                   ,CIKNO AS "CIKNo"
              FROM (SELECT GET(XMLGET(XML,'RepNo'),'$')::STRING AS "RepNo"
                          ,XML
                          ,GET(CompanyXref.value,'@Type')::STRING AS TYPE
                          ,GET(CompanyXref.value,'$')::STRING AS VALUE
                    FROM REFINITIV.STAGING.RATIO_AND_STATISTICS_XML,
                         LATERAL FLATTEN(input => XML, recursive => true) AS CompanyXref
                    WHERE GET(CompanyXref.value,'@') = 'CompanyXref')
             PIVOT(MAX("VALUE") FOR TYPE IN ('OrganizationPermID', 'IRSNo', 'CIKNo')) AS p (REPNO,XML,ORGANIZATIONPERMID,IRSNO,CIKNO)),


	 refInfo AS (SELECT t."RepNo"
				  ,t."OrganizationPermID"
				  ,t."IRSNo"
				  ,t."CIKNo"
				  ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'CompanyInformation'),'Company'),'@Name')::STRING AS "CompanyName"
				  ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'CompanyInformation'),'Company'),'@Type')::STRING AS "CompanyType"
				  ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'CompanyInformation'),'ReportingCurrency'),'$')::STRING AS "ReportingCurrency"
				  ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'CompanyInformation'),'ReportingCurrency'),'@ExRateDate')::DATE AS "ExchangeRateDate"
				  ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'CompanyInformation'),'ReportingCurrency'),'@USDToRepExRate')::FLOAT AS "USDToReportingCurrencyExchangeRate"
				  ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'CompanyInformation'),'Employees'),'$')::STRING AS "Employees"
				  ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'CompanyInformation'),'Employees'),'@LastUpdated')::STRING AS "EmployeesLastUpdated"
				  ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'StatementInfo'),'COAType'),'$')::STRING AS "COAType"
				  ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'StatementInfo'),'BalanceSheetDisplay'),'$')::STRING AS "BalanceSheetDisplay"
				  ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'StatementInfo'),'CashFlowMethod'),'$')::STRING AS "CashFlowMethod"
				  ,GET(XMLGET(XMLGET(t.XML,'Ratios'),'PricingCurrency'),'$')::STRING AS "PricingCurrency"
				  ,GET(XMLGET(XMLGET(t.XML,'Ratios'),'PricingCurrency'),'@Code')::STRING AS "PricingCurrencyCode"
				  ,GET(XMLGET(XMLGET(t.XML,'Ratios'),'PricingCurrency'),'@ExRateDate')::STRING AS "PricingCurrencyExRateDate"
				  ,GET(XMLGET(XMLGET(t.XML,'Ratios'),'PricingCurrency'),'@RepToPrcExRate')::STRING AS "PricingCurrencyRepToPrcExRate"
				  ,XMLGET(t.XML,'Ratios') AS RATIOS_XML
			    FROM ids t )
SELECT "RepNo"
       ,"IRSNo"
       ,"OrganizationPermID"
	   ,"CIKNo"
	   ,"CompanyName"
	   ,"CompanyType"
	   ,"ReportingCurrency"
	   ,"ExchangeRateDate"
	   ,"USDToReportingCurrencyExchangeRate"
	   ,"Employees"
	   ,"EmployeesLastUpdated"
	   ,"COAType"
	   ,"BalanceSheetDisplay"
	   ,"CashFlowMethod"
	   ,GET(XMLGET(RATIOS_XML,'PricingCurrency'),'$')::STRING AS "PricingCurrency"
	   ,GET(XMLGET(RATIOS_XML,'PricingCurrency'),'@Code')::STRING AS "PricingCurrencyCode"
	   ,GET(XMLGET(RATIOS_XML,'PricingCurrency'),'@ExRateDate')::STRING AS "PricingCurrencyExRateDate"
	   ,GET(XMLGET(RATIOS_XML,'PricingCurrency'),'@RepToPrcExRate')::STRING AS "PricingCurrencyRepToPrcExRate"
	   ,'Issue Specific' AS "RatioScope"
	   ,GET(IssueSpecific.value,'@Order') AS "Order"
	   ,TRIM(GET(IssueSpecificGroup.value,'@ID'),'"') AS "GroupID"
	   ,TRIM(GET(IssueSpecificRatios.value,'@FieldName'),'"') AS "FieldName"
	   ,TRIM(GET(IssueSpecificRatios.value,'@Type'),'"') AS "RatioType"
	   ,GET(IssueSpecificRatios.value,'$') AS "RatioValue"
FROM refInfo,
	 LATERAL FLATTEN(input => RATIOS_XML, recursive => true) AS IssueSpecific,
     LATERAL FLATTEN(input => IssueSpecific.value, recursive => true) IssueSpecificGroup,
     LATERAL FLATTEN(input => IssueSpecificGroup.value, recursive => true) IssueSpecificRatios
     WHERE GET(IssueSpecific.value,'@') = 'IssueSpecific' AND
           GET(IssueSpecificGroup.value,'@') = 'Group' AND
           GET(IssueSpecificRatios.value,'@') = 'Ratio'
UNION
SELECT "RepNo"
       ,"IRSNo"
       ,"OrganizationPermID"
	   ,"CIKNo"
	   ,"CompanyName"
	   ,"CompanyType"
	   ,"ReportingCurrency"
	   ,"ExchangeRateDate"
	   ,"USDToReportingCurrencyExchangeRate"
	   ,"Employees"
	   ,"EmployeesLastUpdated"
	   ,"COAType"
	   ,"BalanceSheetDisplay"
	   ,"CashFlowMethod"
	   ,GET(XMLGET(RATIOS_XML,'PricingCurrency'),'$')::STRING AS "PricingCurrency"
	   ,GET(XMLGET(RATIOS_XML,'PricingCurrency'),'@Code')::STRING AS "PricingCurrencyCode"
	   ,GET(XMLGET(RATIOS_XML,'PricingCurrency'),'@ExRateDate')::STRING AS "PricingCurrencyExRateDate"
	   ,GET(XMLGET(RATIOS_XML,'PricingCurrency'),'@RepToPrcExRate')::STRING AS "PricingCurrencyRepToPrcExRate"
	   ,'Company Specific' AS "RatioScope"
	   ,GET(CompanySpecific.value,'@Order') AS "Order"
	   ,TRIM(GET(CompanySpecificGroup.value,'@ID'),'"') AS "GroupID"
	   ,TRIM(GET(CompanySpecificRatios.value,'@FieldName'),'"') AS "FieldName"
	   ,TRIM(GET(CompanySpecificRatios.value,'@Type'),'"') AS "RatioType"
	   ,GET(CompanySpecificRatios.value,'$') AS "RatioValue"
FROM refInfo,
     LATERAL FLATTEN(input => RATIOS_XML, recursive => true) AS CompanySpecific,
     LATERAL FLATTEN(input => CompanySpecific.value, recursive => true) CompanySpecificGroup,
     LATERAL FLATTEN(input => CompanySpecificGroup.value, recursive => true) CompanySpecificRatios
     WHERE GET(CompanySpecific.value,'@') = 'CompanySpecific' AND
           GET(CompanySpecificGroup.value,'@') = 'Group' AND
           GET(CompanySpecificRatios.value,'@') = 'Ratio'
ORDER BY "RepNo","RatioScope",TO_NUMBER("Order"),"GroupID","FieldName" ;

TRUNCATE REFINITIV.PUBLIC.RATIO_AND_STATISTICS;

INSERT INTO REFINITIV.PUBLIC.RATIO_AND_STATISTICS
    SELECT * FROM REFINITIV.STAGING.RATIO_AND_STATISTICS_STAGING_V;