import pandas as pd
from lxml import etree
import os
import sys
from datetime import datetime
import SnowflakeUpload as SF


class data_parser:
    def file_to_tree():
        inputdir = str(input("Type Input Directory Here"))
        outputdir = str(input("Type Output Directory here"))
        #columns for each table being output
        ref_info_cols = [
                        'RepNo',
                        'CompanyName',
                        'ActiveStatus',
                        'EntityType',
                        'OrganizationPERMID',
                        'Last_modified_Date',
                        'Most_Recent_Annual_Date',
                        'Most_recent_Interim_Date',
                        'Currency_Type_Code',
                        'Exchange_Rate',
                        'Exchange_Rate_date',
                        'Currency_Name',
                        'Auditor_Code',
                        'Auditor_Name',
                        'Issue_ID',
                        'Issue_Type_ID',
                        'Issue_Desc',
                        'Issue_Order',
                        'Issue_Status',
                        'Issue_Name',
                        'Issue_ISIN',
                        'Issue_RIC',
                        'Issue_Display_RIC',
                        'Issue_Instrument_ID',
                        'Issue_Quote_ID',
                        'Issue_Instrument_Perm_ID',
                        'Issue_Quote_Perm_ID',
                        'Exchange_Name',
                        'Exchange_Code',
                        'Exchange_Country',
                        'Exchange_Region',
                        'Authorized_Shares',
                        'Floating_Shares',
                        'Outstanding_Shares',
                        'COA_Type',
                        'COA_Name',
                        'Availability_Business_Segment',
                        'Availability_Geographic_Segment'
                        ]
        fin_info_cols = ['RepNo',
                         'Period_UUID',
                         'PeriodEndDate',
                         'FiscalPeriod',
                         'Statement_Type',
                         'Statement_Date',
                         'FinalFiling',
                         'Original_Announcement',
                         'Currency_Converted',
                         'Currency_Reported',
                         'Currency_Conversion',
                         'Currency_Units_ConvertedTo',
                         'Currency_Units_Reported',
                         'Auditor_Code',
                         'Auditor_Name',
                         'Opinion_Code',
                         'Opinion_Name',
                         'Consolidated',
                         'PeriodLength',
                         'PeriodCode',
                         'PeriodType',
                         'Update_Code',
                         'Update_Type',
                         'Source_Date',
                         'Source_Name',
                         'Source_Filing_ID',
                         'Document_ID',
                         'System_Date']
        segment_details_cols = ["RepNo",
                                'Segment_UUID',
                                'Period_UUID',
                                'Statement_Date',
                                'Segment_Name',
                                'Segment_Order',
                                'Segment_Code',
                                'Segment_COA',
                                'Segment_Values'
                                ]
        # create tables here
        ref_info_table = createbasictable(ref_info_cols)
        fin_info_table = createbasictable(fin_info_cols)
        #map_item_table = createbasictable(map_item_cols)
        segment_details_table = createbasictable(segment_details_cols)

        total_file_count = 0
        #looping through input files
        for root, dirs, files in os.walk(inputdir, topdown=False):
            total_file_count += len(files)

        crnt_file_count = 0
        for root, dirs, files in os.walk(inputdir, topdown=False):
            for name in files:
                crnt_file_count += 1
                print('\rProcessing "{}" ({} of {})...'.format(name, crnt_file_count, total_file_count), end='')
                sys.stdout.flush()

                fp = os.path.join(inputdir, name)

                tree = etree.parse(fp)
                root = tree.getroot()
                childls = root.getchildren()
                table_ls = get_ref_info(childls, outputdir, ref_info_cols, fin_info_cols,segment_details_cols, ref_info_table, fin_info_table,segment_details_table)
                ref_info_table = table_ls[0]
                fin_info_table = table_ls[1]
                segment_details_table = table_ls[2]

        print('Done.')
        return None

    #getting reference information and calling other functions as appropriate
    def get_ref_info(ls, path, ref_cols, fin_cols, seg_cols, table1, table2, table4):
        size_table = len(table1)
        #print('get_ref_info = {}'.format(len(ls)))
        #row_ref = row_bio = pd.Series(data=None, index=ref_cols, dtype=object)
        table_ref = dict()
        for col in ref_cols:
            table_ref[col] = []
        row_ref = dict()
        ls_tables = []
        repno = ''

        for item in ls:
            itemtag = item.tag
            if "RepNo" in itemtag:
                repno = item.text
                row_ref['RepNo'] = repno
            elif "ReferenceInformation" in itemtag:
                refls  = item.getchildren()
                #print('refls = {}'.format(len(refls)))
                for task in refls:
                    tasktag = task.tag
                    if "CompanyInformation" in tasktag:
                        comp_ls = task.getchildren()
                        #print('comp_ls = {}'.format(len(comp_ls)))
                        for value in comp_ls:
                            valuetag = value.tag
                            #need value.getchildren and another for loop
                            if "Company" == valuetag:
                                row_ref['CompanyName'] = value.attrib['Name'] if 'Name' in value.attrib else ''
                                row_ref['ActiveStatus'] = value.attrib['ActiveStatus'] if 'ActiveStatus' in value.attrib else ''
                                row_ref['EntityType'] = value.attrib['Type'] if 'Type' in value.attrib else ''
                            elif "CompanyXref" in valuetag:
                                xref_type = value.attrib['Type']
                                if "OrganizationPermID" in xref_type:
                                    permid = value.text if value.text is not None else ''
                                    row_ref['OrganizationPERMID'] = permid
                                else:
                                    continue
                            elif "LastModified" in valuetag:
                                lastmod = value.attrib['Financials'] if 'Financials' in value.attrib else ''
                                row_ref['Last_modified_Date'] = lastmod
                            elif 'LatestFinancials' in valuetag:
                                annualdate = value.attrib['Annual'] if 'Annual' in value.attrib else ''
                                row_ref['Most_Recent_Annual_Date'] = annualdate
                            elif 'ReportingCurrency' in valuetag:
                                curr_code = value.attrib['Code'] if 'Code' in value.attrib else ''
                                exch_rate = value.attrib['USDToRepExRate'] if 'USDToRepExRate' in value.attrib else ''
                                exch_date = value.attrib['ExRateDate'] if 'ExRateDate' in value.attrib else ''
                                curr_type = value.text if value.text is not None else ''
                                row_ref['Currency_Type_Code'] = curr_code
                                row_ref['Exchange_Rate'] = exch_rate
                                row_ref['Exchange_Rate_date'] = exch_date
                                row_ref['Currency_Name'] = curr_type
                            elif 'CurrentAuditor' in valuetag:
                                audit_code = value.attrib['Code'] if 'Code' in value.attrib else ''
                                audit_name = value.attrib['Name'] if 'Name' in value.attrib else ''
                                row_ref['Auditor_Code'] = audit_code
                                row_ref['Auditor_Name'] = audit_name
                            else:
                                continue

                    elif "Issues" in tasktag:
                        issuesls = task.getchildren()
                        issue = issuesls[0]
                        issue_ID = issue.attrib['ID'] if 'ID' in issue.attrib else ''
                        issue_type = issue.attrib['Type'] if 'Type' in issue.attrib else ''
                        issue_desc = issue.attrib['Desc'] if 'Desc' in issue.attrib else ''
                        issue_order = issue.attrib['Order'] if 'Order' in issue.attrib else ''
                        issue_status = issue.attrib['IssueActiveStatus'] if 'IssueActiveStatus' in issue.attrib else ''
                        row_ref['Issue_ID'] = issue_ID
                        row_ref['Issue_Type_ID'] = issue_type
                        row_ref['Issue_Desc'] = issue_desc
                        row_ref['Issue_Order'] = issue_order
                        row_ref['Issue_Status'] = issue_status
                        issue_sub = issue.getchildren()
                        #Data has been checked for multiple issues which don't exist
                        for issue in issue_sub:
                            issuetag = issue.tag
                            issuexref = issue.attrib['Type'] if 'Type' in issue.attrib else ''

                            if issuexref is not None:
                                if "Name" in issuexref:
                                    row_ref['Issue_Name'] = issue.text if issue.text is not None else ''

                                elif "ISIN" in issuexref:
                                    row_ref['Issue_ISIN'] = issue.text if issue.text is not None else ''

                                elif "RIC" in issuexref:
                                    row_ref['Issue_RIC'] = issue.text if issue.text is not None else ''

                                elif "DisplayRIC" in issuexref:
                                    row_ref['Issue_Display_RIC'] = issue.text if issue.text is not None else ''

                                elif "InstrumentPI" in issuexref:
                                    row_ref['Issue_Instrument_ID'] = issue.text if issue.text is not None else ''

                                elif "QuotePI" in issuexref:
                                    row_ref['Issue_Quote_ID'] = issue.text if issue.text is not None else ''

                                elif "InstrumentPermID" in issuexref:
                                    row_ref['Issue_Instrument_Perm_ID'] = issue.text if issue.text is not None else ''

                                elif "QuotePermID" in issuexref:
                                    row_ref['Issue_Quote_Perm_ID'] = issue.text if issue.text is not None else ''

                                elif "Exchange" in issuetag:
                                    row_ref['Exchange_Name'] = issue.text if issue.text is not None else ''
                                    row_ref['Exchange_Code'] = issue.attrib['Code'] if 'Code' in issue.attrib else ''
                                    row_ref['Exchange_Country'] = issue.attrib['Country'] if 'Country' in issue.attrib else ''
                                    row_ref['Exchange_Region'] = issue.attrib['Region'] if 'Region' in issue.attrib else ''

                                elif "IssueDetails" in issuetag:
                                    row_ref['Authorized_Shares'] = issue.attrib['ShsAuthorized'] if 'ShsAuthorized' in issue.attrib else ''
                                    row_ref['Floating_Shares'] = issue.attrib['Float'] if 'Float' in issue.attrib else ''
                                    row_ref['Outstanding_Shares'] = issue.attrib['ShsOut'] if 'ShsOut' in issue.attrib else ''

                                else:
                                    continue
                            else:
                                continue

                    elif "StatementInfo" in tasktag:
                        taskls = task.getchildren()
                        for elem in taskls:
                            elemtag = elem.tag
                            if 'COAType' in elemtag:
                                COA_Code = elem.attrib['Code'] if 'Float' in elem.attrib else ''
                                COA_name = elem.text if elem.text is not None else ''
                                #may have to add COA_name
                                row_ref['COA_Type'] = COA_Code
                                row_ref['COA_Name'] = COA_name
            elif "FinancialInformation" in itemtag:
                fin_ls = item.getchildren()
                for fin in fin_ls:
                    fintag = fin.tag
                    if "Availability" in fintag:
                        avail_ls = fin.getchildren()
                        #print('avail_ls = {}'.format(len(avail_ls)))
                        for avail in avail_ls:
                            availtag = avail.tag
                            if "BusinessSegment" in availtag:
                                biz_seg = avail.attrib['Code'] if 'Code' in avail.attrib else ''
                                row_ref['Availability_Business_Segment'] = biz_seg

                            elif "GeographicSegment" in availtag:
                                geo_seg = avail.attrib['Code']
                                row_ref['Availability_Geographic_Segment'] = geo_seg
                        if size_table >= 500:
                        # df to csv
                            filename = 'Finance_Ref_Info'
                            now = datetime.now()
                            timestamp = str(datetime.timestamp(now))
                            csv = '.csv'
                            batch = filename + timestamp + csv
                            output = os.path.join(path, batch)
                            for key in table_ref.keys():
                                if key in row_ref.keys():
                                    table_ref[key].append(row_ref[key])
                                else:
                                    table_ref[key].append('')
                            row_ref = dict()
                            table1 = table1.append(pd.DataFrame.from_dict(table_ref, dtype=str), sort=False)
                            table1.to_csv(output, index=False)
                            del table1
                            output = ''
                            table1 = createbasictable(ref_cols)
                            row_ref = dict()
                            table_ref = dict()
                            for col in ref_cols:
                                table_ref[col] = []
                            size_table = 0
                        else:
                            for key in table_ref.keys():
                                if key in row_ref.keys():
                                    table_ref[key].append(row_ref[key])
                                else:
                                    table_ref[key].append('')
                            row_ref = dict()
                            size_table += 1
                            continue
                    #from here out this function is using elif case statements to
                    #call other functions which do similar xml parsing to the above

                    elif "FinancialStatements" in fintag:
                        fin_st_ls = fin.getchildren()
                        table2 = get_fin_st_rows(fin_st_ls,repno,table2,fin_cols,path,seg_cols,table4)

            else:
                continue
        table1 = table1.append(pd.DataFrame.from_dict(table_ref, dtype=str), sort=False)
        ls_tables.append(table1)
        ls_tables.append(table2)
        ls_tables.append(table4)

        return ls_tables

    #gets financial statement info
    def get_fin_st_rows(ls, value, table, cols, path, cols2, table2):
        size_table = len(table)
        #print('get_fin_st_rows = {}'.format(len(ls)))
        repno = value
        pipe = '|'
        #row = pd.Series(data=None,index=cols)
        row = dict()
        table_dict = dict()
        for col in cols:
            table_dict[col] = []
        for period in ls:
            periodend = period.attrib['PeriodEndDate'] if 'PeriodEndDate' in period.attrib else ''
            periodtype = period.attrib['PeriodType'] if 'PeriodType' in period.attrib else ''
            period_uuid = repno+pipe+periodend+pipe+periodtype
            row['Period_UUID'] = repno+pipe+periodend+pipe+periodtype
            row['RepNo'] = repno
            row['PeriodEndDate'] = periodend
            row['FiscalPeriod'] = period.attrib['PeriodType'] if 'PeriodType' in period.attrib else ''

            statementls = period.getchildren()
            for statement in statementls:
                statementtag = statement.tag
                if "Statements" in statementtag:
                    statement_deets = statement.getchildren()
                    #print('statement_deets = {}'.format(len(statement_deets)))
                    for deet in statement_deets:
                        statement_type = deet.attrib['Type']
                        statement_date = deet.attrib['StatementDate']
                        row['Statement_Type'] = deet.attrib['Type'] if 'Type' in deet.attrib else ''
                        row ['Statement_Date'] = deet.attrib['StatementDate'] if 'StatementDate' in deet.attrib else ''

                        deet_ls = deet.getchildren()
                        for struct in deet_ls:
                            deettag = struct.tag
                            if "StatementHeader" in deettag:
                                headerls = struct.getchildren()
                                #print('headerls = {}'.format(len(headerls)))
                                for header in headerls:
                                    headertag = header.tag
                                    if "FinalFiling" in headertag:
                                        finalfile = header.text
                                        row['FinalFiling'] = finalfile
                                        finalfile = ''
                                    elif "OriginalAnnouncement" in headertag:
                                        ogann = header.text
                                        row['Original_Announcement'] = ogann
                                        ogann = ''
                                    elif "Currencies" in headertag:
                                        row['Currency_Converted'] = header.attrib['ConvertedTo'] if 'ConvertedTo' in header.attrib else ''
                                        row['Currency_Reported'] = header.attrib['Reported'] if 'Reported' in header.attrib else ''
                                        row['Currency_Conversion'] = header.attrib['RepToConvExRate'] if 'RepToConvExRate' in header.attrib else ''


                                    elif "Units" in headertag:
                                        row['Currency_Units_ConvertedTo'] = header.attrib['ConvertedTo'] if 'ConvertedTo' in header.attrib else ''
                                        row['Currency_Units_Reported'] = header.attrib['Reported'] if 'Reported' in header.attrib else ''

                                    elif "Auditor" in headertag:
                                        row['Auditor_Code'] = header.attrib['Code'] if 'Code' in header.attrib else ''
                                        row['Auditor_Name'] = header.attrib['Name'] if 'Name' in header.attrib else ''
                                        row['Opinion_Code'] = header.attrib['OpinionCode'] if 'OpinionCode' in header.attrib else ''
                                        row['Opinion_Name'] = header.attrib['Opinion'] if 'Opinion' in header.attrib else ''


                                    elif "Consolidated" in headertag:
                                        consol = header.text
                                        row['Consolidated'] = consol
                                        consol = ''

                                    elif "Periodlength" in headertag:
                                        row['PeriodType'] = header.attrib['Type'] if 'Type' in header.attrib else ''
                                        row['PeriodCode'] = header.attrib['Code'] if 'Code' in header.attrib else ''
                                        row['PeriodLength'] = header.text if header.text is not None else ''


                                    elif "UpdatedType" in headertag:
                                        row['Update_Code'] = header.attrib['Code'] if 'Code' in header.attrib else ''
                                        row['Update_Type'] = header.text if header.text is not None else ''


                                    elif "Source" in headertag:
                                        row['Source_Date'] = header.attrib['Date'] if 'Date' in header.attrib else ''
                                        row['Source_Name'] = header.text if header.text is not None else ''

                                    elif "SourceFiling" in headertag:
                                        row['Source_Filing_ID'] = header.attrib['ID'] if 'ID' in header.attrib else ''

                                    elif "Document" in headertag:
                                        row['Document_ID'] = header.attrib['ID'] if 'ID' in header.attrib else ''

                                    elif "SystemDate" in headertag:
                                        sys_date = header.text
                                        row['System_Date'] = sys_date
                                        sys_date = ''

                            elif "FinancialValues" in deettag:
                                segment_ls = struct.getchildren()
                                table2 = get_seg_rows(segment_ls, repno, table2, cols2, period_uuid, statement_date, path)
                elif "PeriodHeader" in statementtag:
                    continue
                else:
                    continue
            #size_table = len(table)
            #size_table = len(table_dict)
            if size_table >= 500:
                # df to csv
                filename = 'Financial_Statement'
                now = datetime.now()
                timestamp = str(datetime.timestamp(now))
                csv = '.csv'
                batch = filename + timestamp + csv
                output = os.path.join(path, batch)

                for key in table_dict.keys():
                    if key in row.keys():
                        table_dict[key].append(row[key])
                    else:
                        table_dict[key].append('')
                table = table.append(pd.DataFrame.from_dict(table_dict, dtype=str), sort=False)
                table.to_csv(output, index=False)
                del table
                output = ''
                table = createbasictable(cols)

                row = dict()
                table_dict = dict()
                for col in cols:
                    table_dict[col] = []
                size_table = 0
            else:

                for key in table_dict.keys():
                    if key in row.keys():
                        table_dict[key].append(row[key])
                    else:
                        table_dict[key].append('')
                row = dict()
                size_table += 1
                continue
        table = table.append(pd.DataFrame.from_dict(table_dict, dtype=str), sort=False)
        return table

    #gets segment info
    def get_seg_rows(ls, value, table, cols, uuid, date,path):
        #print('get_seg_rows = {}'.format(len(ls)))
        repno = value
        size_table = len(table)
        table_dict = dict()

        for col in cols:
            table_dict[col] = []
        for item in ls:
            pipe = '|'
            #row = pd.Series(data=None, index=cols)
            row = dict()
            seg_order = item.attrib['Order']
            itemtag = item.tag
            if "SegmentDetails" in itemtag:
                seg_details = item.getchildren()
                #print('seg_details = {}'.format(len(seg_details)))
                for seg in seg_details:
                    segatag = seg.tag
                    if "SegmentCode1" in segatag:
                        seg_code = seg.text
                    elif "SegmentName" in segatag:
                        seg_name = seg.text
                    elif "SegmentValues" in segatag:
                        seg_values = seg.getchildren()
                        for val in seg_values:
                            seg_coa = val.attrib['COA']
                            seg_value = val.text
                            seg_uuid = repno+pipe+seg_order+pipe+seg_coa
                            row['RepNo'] = repno
                            row['Segment_UUID'] = seg_uuid
                            row['Period_UUID'] = uuid
                            row['Statement_Date'] = date
                            row['Segment_Name'] = seg_name
                            row['Segment_Order'] = seg_order
                            row['Segment_Code'] = seg_code
                            row['Segment_COA'] = seg_coa
                            row['Segment_Values'] = seg_value

                            #size_table = len(table)
                            #size_table = len(table_dict)
                            if size_table >= 500:
                                # df to csv
                                filename = 'Segment_value'
                                now = datetime.now()
                                timestamp = str(datetime.timestamp(now))
                                csv = '.csv'
                                batch = filename + timestamp + csv
                                output = os.path.join(path, batch)
                                #table = table.append(row, ignore_index=True)

                                for key in table_dict.keys():
                                    if key in row.keys():
                                        table_dict[key].append(row[key])
                                    else:
                                        table_dict[key].append('')

                                table = table.append(pd.DataFrame.from_dict(table_dict, dtype=str), sort=False)
                                table.to_csv(output, index=False)
                                del table
                                output = ''
                                table = createbasictable(cols)

                                row = dict()
                                table_dict = dict()
                                for col in cols:
                                    table_dict[col] = []
                                size_table = 0
                            else:

                                for key in table_dict.keys():
                                    if key in row.keys():
                                        table_dict[key].append(row[key])
                                    else:
                                        table_dict[key].append('')
                                row = dict()
                                size_table += 1
                                continue
            else:
                continue

        table = table.append(pd.DataFrame.from_dict(table_dict, dtype=str), sort=False)
        return table

    #creates tables with a given column list
    def createbasictable(ls):
        df = pd.DataFrame(None, None, ls)
        return df


if __name__ == '__main__':
    parser = data_parser()    
    parser.file_to_tree()
    #SF.snowflake()
