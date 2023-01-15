CREATE OR REPLACE TABLE REFINITIV.STAGING.OFFICERS_AND_DIRECTORS_CONTACT_INFO AS
SELECT "RepNo"
     , "OrganizationPermID"
     , "CompanyName"
     , "CompanyNameType"
     , "ProductionDate"
     , "OfficerPermID"
     , "EMail"
     , "Website"
     , "PhoneType"
     , "CountryPhoneCode"
     , "CityAreaCode"
     , "Number"
     , current_timestamp AS "DataWareHouseInsertTime"
FROM (
         SELECT GET(XMLGET(XML, 'RepNo'), '$')::STRING                                   AS "RepNo"
              , GET(XMLGET(XML, 'OrganizationPermID'), '$')::STRING                      AS "OrganizationPermID"
              , GET(XMLGET(XML, 'CompanyName'), '$')::STRING                             AS "CompanyName"
              , GET(XMLGET(XML, 'CompanyName'), '@Type')::STRING                         AS "CompanyNameType"
              , GET(XMLGET(XML, 'Production'), '@Date')::DATETIME                        AS "ProductionDate"
              , GET(officer.value, '@OfficerPermID')::STRING                             AS "OfficerPermID"
              , regexp_count(officer.path, '\\.|\\[') + 1                                as L
              , GET(XMLGET(contact.value,'EMail'), '$')::STRING              AS "EMail"
              , GET(XMLGET(contact.value,'Website'), '$')::STRING            AS "Website"
              , GET(XMLGET(phone.value,'Phone'), '@Type')::STRING            AS "PhoneType"
              , GET(XMLGET(XMLGET(phone.value,'Phone'), 'CountryPhoneCode'),'$')::STRING           AS "CountryPhoneCode"
              , GET(XMLGET(XMLGET(phone.value,'Phone'), 'CityAreaCode'),'$')::STRING               AS "CityAreaCode"
              , GET(XMLGET(XMLGET(phone.value,'Phone'), 'Number'),'$')::STRING                     AS "Number"
              FROM REFINITIV.STAGING.OFFICERS_AND_DIRECTORS_XML,
              LATERAL FLATTEN(input => XML, recursive => true) as officer,
              LATERAL FLATTEN(input => officer.value, recursive => true) as contact,
              LATERAL FLATTEN(input => contact.value, recursive => true) as phone
              WHERE GET(officer.value, '@') = 'Officer'
              AND (L = 4 OR L = 5)
              AND GET(contact.value, '@') = 'ContactInformation'
              AND GET(phone.value, '@') = 'PhoneNumbers');
