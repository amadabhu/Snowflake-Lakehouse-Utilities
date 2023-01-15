CREATE TABLE REFINITIV.STAGING.CSF_QUANT_ANN_DATA_LAYOUT
 AS
 SELECT GET(XMLGET(XML,'RepNo'),'$')::STRING AS "RepNo"
       ,GET(FinancialsLayout.value,'@Type')::STRING AS "LayoutType"
       ,GET(FinancialsLayoutMapItem.value,'@LineID')::INTEGER AS "LineID"
       ,GET(FinancialsLayoutMapItem.value,'@STDLineID')::STRING AS "STDLineID"
       ,GET(FinancialsLayoutMapItem.value,'@COA')::STRING AS "COA"
       ,GET(FinancialsLayoutMapItem.value,'@Display')::STRING AS "Display" 
       ,GET(FinancialsLayoutMapItem.value,'$')::STRING AS "Description"
FROM REFINITIV.STAGING.CSF_QUANT_ANN_DATA_XML t
     ,LATERAL FLATTEN(XMLGET(XMLGET(XML,'FinancialInformation'),'FinancialsLayout'),'$',RECURSIVE => TRUE) FinancialsLayout
     ,LATERAL FLATTEN(FinancialsLayout.value ,'$',RECURSIVE => TRUE) FinancialsLayoutMapItem
WHERE
GET(FinancialsLayout.value, '@') = 'Layout' AND
GET(FinancialsLayoutMapItem.value,'@') = 'MapItem'
ORDER BY "RepNo","LayoutType","LineID";


CREATE TABLE REFINITIV.STAGING.CSF_QUANT_ANN_DATA_FV
AS
SELECT GET(XMLGET(XML,'RepNo'),'$')::STRING AS "RepNo"
       ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'CompanyInformation'),'Company'),'@Name')::STRING AS "CompanyName"
       ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'CompanyInformation'),'Company'),'@Type')::STRING AS "CompanyType"
       ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'CompanyInformation'),'Company'),'@ActiveStatus')::STRING AS "ActiveStatus"
       ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'CompanyInformation'),'LastModified'),'@Other')::DATE AS "OtherLastModified"
       ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'CompanyInformation'),'LastModified'),'@Financials')::DATE AS "FinancialsLastModified"
       ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'CompanyInformation'),'LatestFinancials'),'@Interim')::DATE AS "LastInterimFinancialsDate"
       ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'CompanyInformation'),'LatestFinancials'),'@Annual')::DATE AS "LastAnnualFinancialsDate"       
       ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'CompanyInformation'),'ReportingCurrency'),'$')::STRING AS "ReportingCurrency"
       ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'CompanyInformation'),'ReportingCurrency'),'@ExRateDate')::DATE AS "ExchangeRateDate"
       ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'CompanyInformation'),'ReportingCurrency'),'@USDToRepExRate')::FLOAT AS "USDToReportingCurrencyExchangeRate"
       ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'CompanyInformation'),'CurrentAuditor'),'@Name')::STRING AS "CurrentAuditorName"
       ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'CompanyInformation'),'CurrentAuditor'),'@Code')::STRING AS "CurrentAuditorCode"     
       ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'StatementInfo'),'COAType'),'$')::STRING AS "COAType"
       ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'StatementInfo'),'BalanceSheetDisplay'),'$')::STRING AS "BalanceSheetDisplay"
       ,GET(XMLGET(XMLGET(XMLGET(t.XML,'ReferenceInformation'),'StatementInfo'),'CashFlowMethod'),'$')::STRING AS "CashFlowMethod"
       ,GET(XMLGET(XMLGET(XMLGET(t.XML,'FinancialInformation'),'Availability'),'AnnualCAS'),'@Code')::STRING AS "AnnualCASCode"
       ,GET(XMLGET(XMLGET(XMLGET(t.XML,'FinancialInformation'),'Availability'),'Interims'),'@Code')::STRING AS "InterimsCode"
       ,GET(XMLGET(XMLGET(XMLGET(t.XML,'FinancialInformation'),'Availability'),'InterimINC'),'@Code')::STRING AS "InterimINCCode"
       ,GET(XMLGET(XMLGET(XMLGET(t.XML,'FinancialInformation'),'Availability'),'InterimBAL'),'@Code')::STRING AS "InterimBALCode"
       ,GET(XMLGET(XMLGET(XMLGET(t.XML,'FinancialInformation'),'Availability'),'InterimCAS'),'@Code')::STRING AS "InterimCASCode" 
       ,GET(Period.value,'@PeriodType')::STRING AS "PeriodType"
       ,GET(Period.value,'@PeriodEndDate')::STRING AS "PeriodEndDate"
       ,GET(XMLGET(XMLGET(Period.value,'PeriodHeader'),'FiscalPeriod'),'@InterimNumber')::INTEGER AS "InterimNumber" 
       ,GET(XMLGET(XMLGET(Period.value,'PeriodHeader'),'FiscalPeriod'),'@InterimType')::STRING AS "InterimType"
       ,GET(XMLGET(XMLGET(Period.value,'PeriodHeader'),'FiscalPeriod'),'@IsHybrid')::STRING AS "IsHybrid" 
       ,GET(XMLGET(XMLGET(Period.value,'PeriodHeader'),'FiscalPeriod'),'@FiscalMonth')::INTEGER AS "FiscalMonth" 
       ,GET(XMLGET(XMLGET(Period.value,'PeriodHeader'),'FiscalPeriod'),'@Year')::INTEGER AS "Year"
       ,GET(XMLGET(PeriodFilings.value,'PeriodFiling'),'@PeriodType')::STRING AS "FilingPeriodType"
       ,GET(XMLGET(PeriodFilings.value,'PeriodFiling'),'@PeriodEndDate')::STRING AS "FilingPeriodEndDate"
       ,GET(XMLGET(PeriodFilings.value,'PeriodFiling'),'@StatementDate')::STRING AS "StatementDate2"
       ,GET(XMLGET(XMLGET(PeriodFiling.value,'PeriodFilingHeader'),'FinalFiling'),'$')::INTEGER AS "FinalFiling"
       ,GET(XMLGET(XMLGET(PeriodFiling.value,'PeriodFilingHeader'),'OriginalAnnouncement'),'$')::TIMESTAMP AS "OriginalAnnouncement"
       ,GET(XMLGET(XMLGET(PeriodFiling.value,'PeriodFilingHeader'),'Currencies'),'@RepToConvExRate')::FLOAT AS "FilingCurRepToConvExRate"
       ,GET(XMLGET(XMLGET(PeriodFiling.value,'PeriodFilingHeader'),'Currencies'),'@Reported')::STRING AS "FilingCurReported"
       ,GET(XMLGET(XMLGET(PeriodFiling.value,'PeriodFilingHeader'),'Currencies'),'@ConvertedTo')::STRING AS "FilingCurConvertedTo"
       ,GET(XMLGET(XMLGET(PeriodFiling.value,'PeriodFilingHeader'),'Units'),'@Reported')::STRING AS "UnitsReported"
       ,GET(XMLGET(XMLGET(PeriodFiling.value,'PeriodFilingHeader'),'Units'),'@ConvertedTo')::STRING AS "UnitsConvertedTo"
       ,GET(XMLGET(XMLGET(PeriodFiling.value,'PeriodFilingHeader'),'Auditor'),'@Name')::STRING AS "AuditorName"
       ,GET(XMLGET(XMLGET(PeriodFiling.value,'PeriodFilingHeader'),'Auditor'),'@Code')::STRING AS "AuditorCode" 
       ,GET(XMLGET(XMLGET(PeriodFiling.value,'PeriodFilingHeader'),'Auditor'),'@Opinion')::STRING AS "AuditorOpinion"
       ,GET(XMLGET(XMLGET(PeriodFiling.value,'PeriodFilingHeader'),'Auditor'),'@OpinionCode')::STRING AS "AuditorOpinionCode"
       ,GET(XMLGET(XMLGET(PeriodFiling.value,'PeriodFilingHeader'),'InterimDetails'),'@Type')::STRING AS "InterimDetailsType"
       ,GET(XMLGET(XMLGET(PeriodFiling.value,'PeriodFilingHeader'),'InterimDetails'),'@Number')::STRING AS "InterimDetailsNumber" 
       ,GET(Statement.value,'@Type')::STRING AS "StatementType"
       ,GET(Statement.value,'@PeriodType')::STRING AS "StatementPeriodType"
       ,GET(Statement.value,'@PeriodEndDate')::DATE AS "StatementPeriodEndDate"
       ,GET(Statement.value,'@StatementDate')::DATE AS "StatementDate"
       ,GET(XMLGET(XMLGET(Statement.value,'StatementHeader'),'CompleteStatement'),'$')::STRING AS "CompleteStatement"
       ,GET(XMLGET(XMLGET(Statement.value,'StatementHeader'),'Flash'),'$')::STRING AS "Flash"
       ,GET(XMLGET(XMLGET(Statement.value,'StatementHeader'),'PeriodLength'),'$')::STRING AS "PeriodLength"
       ,GET(XMLGET(XMLGET(Statement.value,'StatementHeader'),'PeriodLength'),'@Type')::STRING AS "PeriodLengthType"
       ,GET(XMLGET(XMLGET(Statement.value,'StatementHeader'),'PeriodLength'),'@Code')::STRING AS "PeriodLengthCode" 
       ,GET(XMLGET(XMLGET(Statement.value,'StatementHeader'),'UpdateType'),'$')::STRING AS "UpdateType"
       ,GET(XMLGET(XMLGET(Statement.value,'StatementHeader'),'UpdateType'),'@Code')::STRING AS "UpdateTypeCode" 
       ,GET(XMLGET(XMLGET(Statement.value,'StatementHeader'),'Source'),'$')::STRING AS "Source"
       ,GET(XMLGET(XMLGET(Statement.value,'StatementHeader'),'Source'),'@Date')::DATE AS "SourceDate" 
       ,GET(XMLGET(XMLGET(Statement.value,'StatementHeader'),'SourceFiling'),'@ID')::STRING AS "SourceFilingID"
       ,GET(XMLGET(XMLGET(Statement.value,'StatementHeader'),'SourceFiling'),'@IsInternal')::STRING AS "IsInternal" 
       ,GET(XMLGET(XMLGET(Statement.value,'StatementHeader'),'OriginalAnnouncement'),'$')::STRING AS "StatementOriginalAnnouncement"
       ,GET(XMLGET(XMLGET(Statement.value,'StatementHeader'),'Document'),'@ID')::STRING AS "DocumentID"
       ,GET(XMLGET(XMLGET(Statement.value,'StatementHeader'),'Document'),'@IsInternal')::STRING AS "DocumentIsInternal"
       ,GET(XMLGET(XMLGET(Statement.value,'StatementHeader'),'SystemDate'),'$')::STRING AS "SystemDate"
       ,GET(XMLGET(XMLGET(Statement.value,'StatementHeader'),'StatementLastUpdated'),'$')::STRING AS "StatementLastUpdated"
       ,GET(XMLGET(XMLGET(Statement.value,'StatementHeader'),'ReportedAccountingStandard'),'$')::STRING AS "ReportedAccountingStandard"
       ,GET(XMLGET(XMLGET(Statement.value,'StatementHeader'),'Consolidated'),'$')::STRING AS "Consolidated"
       ,GET(XMLGET(XMLGET(Statement.value,'StatementHeader'),'RestatementReason'),'$')::STRING AS "RestatementReason"
       ,GET(XMLGET(XMLGET(Statement.value,'StatementHeader'),'RestatementReason'),'@Type')::STRING AS "RestatementReasonType"
       ,GET(FinancialValues.value,'@LineID')::STRING AS "LineID"
       ,GET(FinancialValues.value,'$')::FLOAT AS "Amount"
FROM REFINITIV.STAGING.CSF_QUANT_ANN_DATA_XML t
     ,LATERAL FLATTEN(XML,'$',RECURSIVE => TRUE) FinancialStatements   
     ,LATERAL FLATTEN(FinancialStatements.value,'$',RECURSIVE => TRUE) Period
     ,LATERAL FLATTEN(Period.value,RECURSIVE => TRUE) PeriodFilings
     ,LATERAL FLATTEN(PeriodFilings.value,RECURSIVE => TRUE) PeriodFiling
     ,LATERAL FLATTEN(PeriodFiling.value,RECURSIVE => TRUE) Statement
     ,LATERAL FLATTEN(Statement.value,RECURSIVE => TRUE) FinancialValues
WHERE
GET(FinancialStatements.value, '@') = 'FinancialStatements' AND
GET(Period.value, '@') = 'Period' AND
GET(PeriodFilings.value,'@') = 'PeriodFilings' AND
GET(PeriodFiling.value,'@') = 'PeriodFiling' AND
GET(Statement.value,'@') = 'Statement' AND
GET(FinancialValues.value,'@') = 'FV';


CREATE TABLE REFINITIV.STAGING.CSF_QUANT_ANN_DATA
AS
SELECT
 A."RepNo"
,A."CompanyName"
,A."CompanyType"
,A."ActiveStatus"
,A."OtherLastModified"
,A."FinancialsLastModified"
,A."LastInterimFinancialsDate"
,A."LastAnnualFinancialsDate"       
,A."ReportingCurrency"
,A."ExchangeRateDate"
,A."USDToReportingCurrencyExchangeRate"
,A."CurrentAuditorName"
,A."CurrentAuditorCode"     
,A."COAType"
,A."BalanceSheetDisplay"
,A."CashFlowMethod"
,A."AnnualCASCode"
,A."InterimsCode"
,A."InterimINCCode"
,A."InterimBALCode"
,A."InterimCASCode" 
,A."PeriodType"
,A."PeriodEndDate"
,A."InterimNumber" 
,A."InterimType"
,A."IsHybrid" 
,A."FiscalMonth" 
,A."Year"
,A."FilingPeriodType"
,A."FilingPeriodEndDate"
,A."StatementDate2"
,A."FinalFiling"
,A."OriginalAnnouncement"
,A."FilingCurRepToConvExRate"
,A."FilingCurReported"
,A."FilingCurConvertedTo"
,A."UnitsReported"
,A."UnitsConvertedTo"
,A."AuditorName"
,A."AuditorCode" 
,A."AuditorOpinion"
,A."AuditorOpinionCode"
,A."InterimDetailsType"
,A."InterimDetailsNumber" 
,A."StatementType"
,A."StatementPeriodType"
,A."StatementPeriodEndDate"
,A."StatementDate"
,A."CompleteStatement"
,A."Flash"
,A."PeriodLength"
,A."PeriodLengthType"
,A."PeriodLengthCode" 
,A."UpdateType"
,A."UpdateTypeCode" 
,A."Source"
,A."SourceDate" 
,A."SourceFilingID"
,A."IsInternal" 
,A."StatementOriginalAnnouncement"
,A."DocumentID"
,A."DocumentIsInternal"
,A."SystemDate"
,A."StatementLastUpdated"
,A."ReportedAccountingStandard"
,A."Consolidated"
,A."RestatementReason"
,A."RestatementReasonType"
,A."LineID"
,A."Amount"
,B."Description"
,B."LayoutType"
,B."STDLineID"
,B."COA"
,B."Display" 
FROM "REFINITIV"."STAGING"."CSF_QUANT_ANN_DATA_FV" A
INNER JOIN "REFINITIV"."STAGING"."CSF_QUANT_ANN_DATA_LAYOUT" B
ON A."RepNo" = B."RepNo" AND 
   A."StatementType" = B."LayoutType" AND
   A."LineID" = B."LineID";
                         
