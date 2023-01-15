CREATE TABLE REFINITIV.STAGING.OFFICERS_AND_DIRECTORS_CERTIFICATION
AS
SELECT 
 "RepNo"
,"OrganizationPermID"
,"CompanyName"
,"Production"
,"OfficerPermID"
,"Active"
,"Status"
,"Rank"
, "ID"
,"PersonID"
,"PersonPermID"
,"PersonActive"
,"LastModified"
,"CertificateID"
,"Certificate"
,current_timestamp AS "DataWareHouseInsertTime"
FROM (
  SELECT
  GET(XMLGET(XML,'RepNo'),'$')::STRING AS "RepNo"
 ,GET(XMLGET(XML,'OrganizationPermID'),'$')::STRING AS "OrganizationPermID"
 ,GET(XMLGET(XML,'CompanyName'),'$')::STRING AS "CompanyName"
 ,GET(XMLGET(XML,'Production'),'@Date')::DATETIME AS "Production"
 ,GET(Officer.value,'@OfficerPermID')::STRING AS "OfficerPermID"
 ,GET(Officer.value,'@Active')::STRING AS "Active"
 ,GET(Officer.value,'@Status')::STRING AS "Status"
 ,GET(Officer.value,'@Rank')::STRING AS "Rank"
 ,GET(Officer.value,'@ID')::STRING AS "ID"
 ,regexp_count(officer.path,'\\.|\\[') +1 as l
 ,GET(XMLGET(Officer.value,'Person'),'@ID')::STRING AS "PersonID"
 ,GET(XMLGET(Officer.value,'Person'),'@PersonPermID')::STRING AS "PersonPermID"
 ,GET(XMLGET(Officer.value,'Person'),'@Active')::STRING AS "PersonActive"
 ,GET(XMLGET(PersonInformation.value,'LastModified'),'@Date')::STRING AS "LastModified"
 ,GET(Certificate.value,'@ID')::STRING AS "CertificateID"
 ,GET(Certificate.value,'$')::STRING AS "Certificate"
FROM REFINITIV.STAGING.OFFICERS_AND_DIRECTORS_XML,
LATERAL FLATTEN (input => XML, recursive => true) as Officer,
LATERAL FLATTEN (input => Officer.value, recursive => true) as PersonInformation,
LATERAL FLATTEN (input => PersonInformation.value, recursive => true) as Certifications,
LATERAL FLATTEN (input => Certifications.value, recursive => true) as Certificate     
WHERE GET(officer.value,'@') = 'Officer' AND (L=4 OR L=5) AND
      GET(PersonInformation.value,'@') = 'PersonInformation' AND
      GET(Certifications.value,'@') = 'Certifications' AND
      GET(Certificate.value,'@') = 'Certificate');
