copy into "REFINITIV"."STAGING"."OWN_ALL_HOLDINGS_QTR1"
from @OwnershipData pattern='.*all_holdings_qtr1\.csv' file_format = (type = csv field_delimiter = '|' skip_header = 0) on_error = 'Continue';
copy into "REFINITIV"."STAGING"."OWN_ALL_HOLDINGS_QTR2"
from @OwnershipData pattern='.*all_holdings_qtr2\.csv' file_format = (type = csv field_delimiter = '|' skip_header = 0) on_error = 'Continue';
copy into "REFINITIV"."STAGING"."OWN_ALL_HOLDINGS_QTR3"
from @OwnershipData pattern='.*all_holdings_qtr3\.csv' file_format = (type = csv field_delimiter = '|' skip_header = 0) on_error = 'Continue';
copy into "REFINITIV"."STAGING"."OWN_ALL_HOLDINGS_QTR4"
from @OwnershipData pattern='.*all_holdings_qtr4\.csv' file_format = (type = csv field_delimiter = '|' skip_header = 0) on_error = 'Continue';
copy into "REFINITIV"."STAGING"."OWN_ALL_HOLDINGS_QTR5"
from @OwnershipData pattern='.*all_holdings_qtr5\.csv' file_format = (type = csv field_delimiter = '|' skip_header = 0) on_error = 'Continue';
copy into "REFINITIV"."STAGING"."OWN_ALL_HOLDINGS_QTR6"
from @OwnershipData pattern='.*all_holdings_qtr6\.csv' file_format = (type = csv field_delimiter = '|' skip_header = 0) on_error = 'Continue';
copy into "REFINITIV"."STAGING"."OWN_ALL_HOLDINGS_QTR7"
from @OwnershipData pattern='.*all_holdings_qtr7\.csv' file_format = (type = csv field_delimiter = '|' skip_header = 0) on_error = 'Continue';
copy into "REFINITIV"."STAGING"."OWN_ALL_HOLDINGS_QTR8"
from @OwnershipData pattern='.*all_holdings_qtr8\.csv' file_format = (type = csv field_delimiter = '|' skip_header = 0) on_error = 'Continue';
