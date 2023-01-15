CREATE TABLE REFINITIV.STAGING.GEN_INFO_PHONE_NUMBERS AS
SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING                    AS "RepNo"
     , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING       AS "OrganizationPermID"
     , GET(XMLGET(XML, 'CompanyName'), '$')::STRING              AS "CompanyName"
     , GET(XMLGET(XML, 'CompanyName'), '@Type')::STRING          AS "CompanyNameType"
     , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME         AS "ProductionDate"
     , GET(Phone.value, '@Type')::STRING                         AS "PhoneType"
     , GET(XMLGET(Phone.value, 'CountryPhoneCode'), '$')::STRING AS "CountryPhoneCode"
     , GET(XMLGET(Phone.value, 'CityAreaCode'), '$')::STRING     AS "CityAreaCode"
     , GET(XMLGET(Phone.value, 'Number'), '$')::STRING           AS "Number"
FROM REFINITIV.STAGING.GI_COMPANIES_XML
   , LATERAL FLATTEN(xml, RECURSIVE => TRUE) ContactInfo
   , LATERAL FLATTEN(ContactInfo.value, RECURSIVE => TRUE) PhoneNumbers
   , LATERAL FLATTEN(PhoneNumbers.value, RECURSIVE => TRUE) Phone
WHERE GET(ContactInfo.value, '@') = 'ContactInfo'
  AND GET(PhoneNumbers.value, '@') = 'PhoneNumbers'
  AND GET(Phone.value, '@') = 'Phone';
