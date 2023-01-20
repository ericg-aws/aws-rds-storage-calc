#!/usr/bin/env python
# purpose: to pull cloudwatch statistics RDS provisioned instances and calculate io to gp3 savings
# example for days back from current with PPA discount: python main.py -d 7 -r us-east-1 -p 0.19
# account/assume role input file: python main.py -d 7 -r us-east-1 -i input/account_role.csv -p 0.19
# utc start and endtime example: python main.py -s '2022-06-25 02:00:00' -e '2022-07-12 02:00:00'

import argparse
import boto3
import os
import pandas as pd
import sys
import traceback

from classes.getinstanceinfo import Getinstanceinfo
from classes.getdata import Getdata

# parse command-line arguments for region and input file
# csv must have columns: instance,region
def parse_args():
    try:
        parser = argparse.ArgumentParser(description='cloudwatch metric pull script')
        parser.add_argument('-o', '--output_file', help='data and cost output filepath', type=str, required=False)
        parser.add_argument('-d', '--days_back', help='days back to pull cloudwatch data', type=int, required=False)
        parser.add_argument('-s', '--start_time', help='start time for cloudwatch data pull', type=str, required=False)
        parser.add_argument('-e', '--end_time', help='end time for cloudwatch data pull', type=str, required=False)
        parser.add_argument('-r', '--region', help='region', type=str, required=False)
        parser.add_argument('-i', '--input_list', help='account and assume role input list', type=str, required=False)
        parser.add_argument('-p', '--percent_discount', help='public pricing discount', type=float, required=False)
        parser.set_defaults(\
                            days_back = 7,
                            )
        args = parser.parse_args()
        return args
    except Exception as e: 
        print(f'An error occurred during parsing of args')
        traceback.print_exc()

def main():

    getinstanceinfo = Getinstanceinfo()
    getdata = Getdata()
        
    args = parse_args()

    #if args.output_file is not None:
    # read in df: account, region, role_arn
    df_role = pd.read_csv(args.input_list, dtype={'account': str, 'region': str, 'role_arn': str})

    sts = boto3.client('sts')

    for index, account_row in df_role.iterrows():
        print(f"Assuming role: {account_row['role_arn']} in account: {account_row['account']}")
        member_account = sts.assume_role(RoleArn=account_row['role_arn'], RoleSessionName='rds-info-gathering')
    
        # retrieve creds from member account
        access_key = member_account['Credentials']['AccessKeyId']
        secret_key = member_account['Credentials']['SecretAccessKey']
        session_token = member_account['Credentials']['SessionToken']

        # pass the session (with the sts credentials) to create temporary regional keys
        session = boto3.Session(region_name=account_row['region'], aws_access_key_id=access_key, aws_secret_access_key=secret_key, aws_session_token=session_token)

        # gather all rds instances in region 
        print(f"Region is set to: {account_row['region']}, gathering RDS instance list.")
        instance_df = getinstanceinfo.get_instance_list(args, session, account_row)


        # retrieving metrics - FreeStorageSpace, WriteIOPS, ReadIOPS, ReadThroughput, WriteThroughput
        if not instance_df.empty:
            instance_df[['cw_storage_free', 'cw_storage_write_iops', 'cw_storage_read_iops', 'cw_storage_write_throughput', 'cw_storage_read_throughput']] = \
                instance_df.apply (lambda row: getinstanceinfo.get_instance_usage(row, args, session, account_row), axis=1, result_type='expand')

            print(instance_df)

            # pull down bulk price list for region
            rds_pricing_df = getinstanceinfo.get_instance_pricing_data(args)

            # add pricing data for current storage costs
            instance_df['current_monthly_storage_cost'] = \
                instance_df.apply (lambda row: getinstanceinfo.get_current_price(row, rds_pricing_df, args), axis=1, result_type='expand')

            # add pricing for gp3 storage - same paramters as current storage
            instance_df['gp3_monthly_storage_cost'] = \
                instance_df.apply (lambda row: getinstanceinfo.get_future_price(row, rds_pricing_df, args), axis=1, result_type='expand')

            # chnage bytes to gigabytes
            instance_df['gp3_monthly_storage_cost'] = \
                instance_df.apply (lambda row: getinstanceinfo.get_future_price(row, rds_pricing_df, args), axis=1, result_type='expand')

            # add pricing for rightsized gp3 storage
            instance_df['cw_storage_free'] = instance_df['cw_storage_free'].apply(getdata.convert_bytes_to_gb)

            # output to local csv 
            if args.output_file is not None:
                instance_df.to_csv(args.output_file, index=False)
            else:
                if not os.path.exists('data'):
                    os.makedirs('data')
                account_id = getinstanceinfo.get_account_info(args)
                output_file = f"data/{account_row['account']}_{account_row['region']}_rds_output.csv"
                instance_df.to_csv(output_file, index=False)
        else:
            print(f"No RDS instances found in account: {account_row['account']}")
    
if __name__ == "__main__":
    main()