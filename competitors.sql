CREATE TABLE REFINITIV.STAGING.COMPETITORS
AS
SELECT REPNO AS "RepNo", 
       COMPANYNAME AS "CompanyName",
       IRSNO AS "IRSNo",
       CIKNO AS "CIKNo",
       ORGANIZATIONPERMID AS"OrganizationPermID",
       GET(XMLGET(Competitor.value,'CompID',0),'$')::STRING AS "CompetitorRepNo",
       GET(XMLGET(Competitor.value,'CompID',1),'$')::STRING AS "CompetitorOrganizationPermID",
       GET(XMLGET(Competitor.value,'CompID',2),'$')::STRING AS "CompetitorCompanyName",
       GET(XMLGET(Competitor.value,'CompID',3),'$')::STRING AS "CompetitorTicker",
       GET(XMLGET(Competitor.value,'CompCoType',0),'$')::STRING AS "CompetitorCompanyType",
       GET(XMLGET(Competitor.value,'CompCoStatus', 0),'$')::STRING AS "CompetitorCompanyStatus",
       CURRENT_TIMESTAMP AS "DataWarehouseInsertTime"
FROM (SELECT * 
      FROM (SELECT xml, 
                  GET(CoIDs.value,'@Type')::STRING AS "Name",
                  GET(CoIDs.value,'$')::STRING AS "Value"
            FROM REFINITIV.STAGING.COMPETITORS_F_XML,
                 LATERAL FLATTEN(GET(xml,'$'), RECURSIVE => TRUE) CoIDs
            WHERE GET(CoIDs.value, '@') = 'CoID')
            PIVOT(MAX("Value") FOR "Name" IN ('RepNo', 'CompanyName', 'IRSNo', 'CIKNo','OrganizationPermID')) AS p (xml,REPNO,COMPANYNAME,IRSNO,CIKNO,ORGANIZATIONPERMID)),
     LATERAL FLATTEN(GET(xml,'$'), RECURSIVE => TRUE) Competitor
WHERE
   GET(Competitor.value, '@') = 'Competitor';
