#!/usr/bin/env python
# purpose: to pull cloudwatch statistics RDS provisioned instances and calculate io to gp3 savings
# example for days back from current: python inference-get-metrics.py -d 7 -r us-east-2
# utc start and endtime example: python inference-get-metrics.py -s '2022-06-25 02:00:00' -e '2022-07-12 02:00:00'

from classes.getdata import Getdata
from classes.getinstanceinfo import Getinstanceinfo

import argparse
import traceback

# parse command-line arguments for region and input file
# csv must have columns: instance,region
def parse_args():
    try:
        parser = argparse.ArgumentParser(description='cloudwatch metric pull script')
        parser.add_argument('-o', '--output_file', help='data and cost output filepath', type=str, required=False)
        parser.add_argument('-d', '--days_back', help='days back to pull cloudwatch data', type=int, required=False)
        parser.add_argument('-s', '--start_time', help='start time for cloudwatch data pull', type=str, required=False)
        parser.add_argument('-e', '--end_time', help='end time for cloudwatch data pull', type=str, required=False)
        parser.add_argument('-r', '--region', help='region', type=str, required=True)
        parser.add_argument('-p', '--percent_discount', help='public pricing discount', type=float, required=False)
        parser.set_defaults(\
                            days_back = 7, \
                            )
        args = parser.parse_args()
        return args
    except Exception as e: 
        print(f'An error occurred during parsing of args')
        traceback.print_exc()

def main():

    getinstanceinfo = Getinstanceinfo()
        
    args = parse_args()

    # gather all rds instances in region 
    print(f'Region is set to: {args.region}, gathering RDS instance list.')
    instance_df = getinstanceinfo.get_instance_list(args)

    # retrieving metrics - FreeStorageSpace, WriteIOPS, ReadIOPS, ReadThroughput, WriteThroughput
    instance_df[['cw_storage_free', 'cw_storage_write_iops', 'cw_storage_read_iops', 'cw_storage_write_throughput', 'cw_storage_read_throughput']] = \
        instance_df.apply (lambda row: getinstanceinfo.get_instance_usage(row, args), axis=1, result_type='expand')

    # pull down bulk price list for region
    rds_pricing_df = getinstanceinfo.get_instance_pricing_data(args)

    # add pricing data for current storage costs
    instance_df['current_monthly_storage_cost'] = \
        instance_df.apply (lambda row: getinstanceinfo.get_current_price(row, rds_pricing_df, args), axis=1, result_type='expand')

    # add pricing for gp3 storage - same paramters as current storage
    instance_df['gp3_monthly_storage_cost'] = \
        instance_df.apply (lambda row: getinstanceinfo.get_future_price(row, rds_pricing_df, args), axis=1, result_type='expand')

    # add pricing for rightsized gp3 storage
    
    # output to local csv 
    if args.output_file is not None:
        instance_df.to_csv(args.output_file, index=False)
    else:
        account_id = getinstanceinfo.get_account_info(args)
        output_file = f'data/{account_id}_{args.region}_rds_output.csv'
        instance_df.to_csv(output_file, index=False)
    
if __name__ == "__main__":
    main()