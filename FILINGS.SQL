//get s3 bucket file names using query id and result_scan

alter table "REFINITIV"."STAGING"."FILINGS_OBJECTS_KEYS" add column "PermID" Varchar(10), "CommonID" Varchar(8);


update "REFINITIV"."STAGING"."FILINGS_OBJECTS_KEYS" set "PermID" = trim(get(split("name",'_'),1),'\'')::string, "CommonID" =  get(split("name",'_'),2)::string;


select get(split("name",''),1)::string as "PermID", get(split("name",'_'),2)::string as "CommonID" from "REFINITIV"."STAGING"."FILINGS_OBJECTS_KEYS" limit 20;
