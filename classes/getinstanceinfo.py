import boto3
from botocore.config import Config
import pprint
import pandas as pd
import numpy as np
import math
import traceback
from classes.getdata import Getdata

class Getinstanceinfo(object):

    def round_up(self, n, decimals):
        multiplier = 10 ** decimals
        return math.ceil(n * multiplier) / multiplier

    def get_instance_list(self, args):
        # agther details of RDS instance deployed in the region
        try:
            dtypes = np.dtype(
                [
                    ('instance', str),
                    ('region', str),
                    ('instance_type', str),
                    ('db', str),
                    ('multi_az', bool),
                    ('storage_type', str),
                    ('storage_size', int),
                    ('storage_throughput', int),
                    ('storage_iops', int)
                ]
            )
            instance_df = pd.DataFrame(np.empty(0, dtype=dtypes))

            rds = boto3.client('rds', region_name=args.region)
            paginator = rds.get_paginator('describe_db_instances').paginate()
            for page in paginator:
                for dbinstance in page['DBInstances']:
                    #pprint.pprint(dbinstance)
                    row_dict = {'instance' : dbinstance.get('DBInstanceIdentifier', 'NaN'), \
                        'region' : args.region, \
                        'instance_type' : dbinstance.get('DBInstanceClass'), \
                        'db' : dbinstance.get('DBName', 'NaN'), \
                        'multi_az' : dbinstance.get('MultiAZ', 'NaN'), \
                        'storage_type' : dbinstance.get('StorageType', 'NaN'), \
                        'storage_size' : dbinstance.get('AllocatedStorage', 'NaN'), \
                        'storage_throughput' : dbinstance.get('StorageThroughput', 'NaN'), \
                        'storage_iops' : dbinstance.get('Iops', 'NaN') \
                        }
                    temp_df = pd.DataFrame([row_dict])
                    instance_df = pd.concat([instance_df, temp_df], ignore_index=True)
            return instance_df
        except Exception as e: 
            print(f'An error occurred during instance info gathering')
            traceback.print_exc()

    def get_instance_config(self, row, pricing_df):
        try:
            client = boto3.client('rds', region_name=row.region)
            db_instance = client.describe_db_instances(DBInstanceIdentifier=row.instance)
            instance_type = db_instance['DBInstances'][0]['DBInstanceClass']
            temp_df = pricing_df[pricing_df['InstanceType']==instance_type]
            temp_df['Memory'] = temp_df['Memory'].str.extract('(\d+)', expand=False)
            temp_df['Memory'] = temp_df['Memory'].astype(int)
            temp_df['vCPU'] = temp_df['vCPU'].astype(int)
            vcpu = temp_df['vCPU'].iloc[0]
            memory = temp_df['Memory'].iloc[0]
            return memory, vcpu, instance_type
        except Exception as e: 
            print(f'An error occurred during instance info gathering')
            traceback.print_exc()


    def get_instance_usage(self, row, args):
        try:
            getdata = Getdata()
        
            # boto3 client config
            config = Config(
                retries = dict(
                    max_attempts = 10
                )
            )

            cw_client = boto3.client('cloudwatch', region_name=row.region, config=config)

            metric_list = ['storage_free', 'storage_write_iops', 'storage_read_iops', 'storage_write_throughput', 'storage_read_throughput']
            metric_dict = [
                {
                    'metric_name':'FreeStorageSpace',
                    'namespace': 'AWS/RDS',
                    'instance_name': 'DBInstanceIdentifier',
                    'stat':'Minimum',
                    'period':86400
                },
                {
                    'metric_name':'WriteIOPS',
                    'namespace': 'AWS/RDS',
                    'instance_name': 'DBInstanceIdentifier',
                    'stat':'p97.00',
                    'period':86400
                },
                {
                    'metric_name':'ReadIOPS',
                    'namespace': 'AWS/RDS',
                    'instance_name': 'DBInstanceIdentifier',
                    'stat':'p97.00',
                    'period':86400
                },
                {
                    'namespace': 'AWS/RDS',
                    'instance_name': 'DBInstanceIdentifier',
                    'metric_name':'ReadThroughput',
                    'stat':'p97.00',
                    'period':86400
                },
                {
                    'namespace': 'AWS/RDS',
                    'instance_name': 'DBInstanceIdentifier',
                    'metric_name':'WriteThroughput',
                    'stat':'p97.00',
                    'period':86400
                }
            ]
            result_list = []

            # FreeStorageSpace, WriteIOPS, ReadIOPS, ReadThroughput, WriteThroughput

            for item in metric_dict:
                val = getdata.cw_rds_pull_metric(cw_client, item['metric_name'], item['namespace'], item['instance_name'], row.instance, item['stat'], item['period'], args)
                result_list.append(val)
            return result_list[0], result_list[1], result_list[2], result_list[3], result_list[4]
        except Exception as e: 
            print(f'An error occurred during instance info gathering')
            traceback.print_exc()
            pass
    
    def get_instance_pricing_data(self, args):
        try:
            pricing_csv = f'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonRDS/current/{args.region}/index.csv'
            pricing_df = pd.read_csv(pricing_csv, skiprows=5)
            pricing_df.columns = pricing_df.columns.str.replace(' ', '')
            return pricing_df
        except Exception as e: 
            print(f'An error occurred during bulk pricing pull')
            traceback.print_exc()


    def calc_io_costs(self, row, rds_pricing_df, storage_gb, storage_iops, args):
        # calculate monthly storage GB costs 
        temp_df = rds_pricing_df[rds_pricing_df['usageType'].str.contains(storage_gb)]
        per_unit = float(temp_df['PricePerUnit'].iat[0])
        gb_monthly_cost = float(row.storage_size) * per_unit

        # calculate monthly storage IOPS costs 
        temp_df = rds_pricing_df[rds_pricing_df['usageType'].str.contains(storage_iops)]
        per_unit = float(temp_df['PricePerUnit'].iat[0])
        iops_monthly_cost = float(row.storage_iops) * per_unit

        storage_cost = gb_monthly_cost + iops_monthly_cost

        if args.percent_discount is not None:
            storage_cost = ((gb_monthly_cost + iops_monthly_cost) * (1.0 - args.percent_discount))
            storage_cost = self.round_up(storage_cost, 2)
        else:
            storage_cost = gb_monthly_cost + iops_monthly_cost

        return storage_cost

    def get_current_price(self, row, rds_pricing_df, args):
        try:
            if row.storage_type == 'io1':
                # future - need to add functionality for Multi-AZ deployment with two readable standby instances
                if row.multi_az == True:
                    return self.calc_io_costs(row, rds_pricing_df, ':Multi-AZ-PIOPS-Storage', ':Multi-AZ-PIOPS', args)
                if row.multi_az == False:
                    return self.calc_io_costs(row, rds_pricing_df, ':PIOPS-Storage', ':PIOPS', args)
            else:
                return 'NaN'
        except Exception as e: 
            print(f'An error occurred during io1 cost calculation')
            traceback.print_exc()
            return 'NaN'

    def calc_gp3_costs(self, row, rds_pricing_df, storage_gb, storage_iops, storage_throughput, args):
        # calculate monthly storage GB costs 
        temp_df = rds_pricing_df[rds_pricing_df['usageType'].str.contains(storage_gb)]
        per_unit = float(temp_df['PricePerUnit'].iat[0])
        gb_monthly_cost = float(row.storage_size) * per_unit

        # calculate monthly storage IOPS costs 
        temp_df = rds_pricing_df[rds_pricing_df['usageType'].str.contains(storage_iops)]
        per_unit = float(temp_df['PricePerUnit'].iat[0])
        # consider 3000 built in for baseline cost
        iops_monthly_cost = float((row.storage_iops) - 3000) * per_unit

        storage_cost = gb_monthly_cost + iops_monthly_cost

        if args.percent_discount is not None:
            storage_cost = ((gb_monthly_cost + iops_monthly_cost) * (1.0 - args.percent_discount))
            storage_cost = self.round_up(storage_cost, 2)
        else:
            storage_cost = gb_monthly_cost + iops_monthly_cost

        return storage_cost

    def get_future_price(self, row, rds_pricing_df, args):
        try:
            if row.storage_type == 'io1':
                # future - need to add functionality for Multi-AZ deployment with two readable standby instances
                if row.multi_az == True:
                    return self.calc_gp3_costs(row, rds_pricing_df, ':Multi-AZ-GP3-Storage', ':Multi-AZ-GP3-PIOPS', ':Multi-AZ-GP3-Throughput', args)
                if row.multi_az == False:
                    return self.calc_gp3_costs(row, rds_pricing_df, ':GP3-Storage', ':GP3-PIOPS', ':GP3-Throughput', args)
            else:
                return 'NaN'
        except Exception as e: 
            print(f'An error occurred during future gp3 cost calculation')
            traceback.print_exc()
            return 'NaN'