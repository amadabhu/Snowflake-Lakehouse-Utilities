CREATE TABLE REFINITIV.STAGING.OFFICERS_AND_DIRECTORS_EDUCATION
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
,"DegreeOrder"
,"CollegeID"
,"College"
,"GraduationYear"
,"DegreeID"
,"Degree"
,"MajorID"
,"Major"
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
 ,GET(Degree.value,'@Order')::STRING AS "DegreeOrder"
 ,GET(XMLGET(Degree.value,'College'),'@ID')::STRING AS "CollegeID"
 ,GET(XMLGET(Degree.value,'College'),'$')::STRING AS "College"
 ,GET(XMLGET(Degree.value,'Graduation'),'@Year')::STRING AS "GraduationYear"
 ,GET(XMLGET(Degree.value,'Degree'),'@ID')::STRING AS "DegreeID" 
 ,GET(XMLGET(Degree.value,'Degree'),'$')::STRING AS "Degree" 
 ,GET(XMLGET(Degree.value,'Major'),'@ID')::STRING AS "MajorID" 
 ,GET(XMLGET(Degree.value,'Major'),'$')::STRING AS "Major"  
 ,regexp_count(Degree.path,'\\.|\\[') +1 as l2
FROM REFINITIV.STAGING.OFFICERS_AND_DIRECTORS_XML,
LATERAL FLATTEN (input => XML, recursive => true) as Officer,
LATERAL FLATTEN (input => Officer.value, recursive => true) as PersonInformation,
LATERAL FLATTEN (input => PersonInformation.value, recursive => true) as EducationHistory,
LATERAL FLATTEN (input => EducationHistory.value, recursive => true) as Degree      
WHERE GET(officer.value,'@') = 'Officer' AND (L=4 OR L=5) AND
      GET(PersonInformation.value,'@') = 'PersonInformation' AND
      GET(EducationHistory.value,'@') = 'EducationHistory' AND
      GET(Degree.value,'@') = 'Degree' AND (L2=2 OR L2=3));
