CREATE TABLE REFINITIV.STAGING.GEN_INFO_CONTACT_INFO AS
WITH 
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
 SELECT * FROM contact;
