copy into "REFINITIV"."STAGING"."OWN_SECURITY_HISTORY"
from @OwnershipData pattern='.*tfo.security_history.*' file_format = (type = csv field_delimiter = '|' skip_header = 0) on_error = 'Continue';


