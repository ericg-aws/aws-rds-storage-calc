import boto3
from botocore.config import Config
import logging
import pandas as pd
import numpy as np
import math
import traceback
from classes.getdata import Getdata

class Getinstanceinfo(object):

    def get_account_info(self, args):
        try:
            account_id = boto3.client('sts').get_caller_identity().get('Account')
            account_arn = boto3.client('sts').get_caller_identity().get('Arn')
            gen = boto3.client('sts').get_caller_identity()
            return account_id, account_arn
        except Exception as e: 
            logging.error(f'An error occurred getting account ID and Role ARN')
            traceback.print_exc()
            return 'unknown-account'

    def round_up(self, n, decimals):
        try:
            multiplier = 10 ** decimals
            return math.ceil(n * multiplier) / multiplier
        except Exception as e: 
            logging.error(f'An error occurred with math rounding')
            traceback.print_exc()

    def get_instance_list(self, args, session, region):
        # gather details of RDS instance deployed in the region
        try:
            dtypes = np.dtype(
                [
                    ('instance', str),
                    ('region', str),
                    ('instance_type', str),
                    ('db', str),
                    ('engine', str),
                    ('multi_az', bool),
                    ('storage_type', str),
                    ('storage_size', int),
                    ('storage_throughput', int),
                    ('storage_iops', int)
                ]
            )
            instance_df = pd.DataFrame(np.empty(0, dtype=dtypes))

            rds = session.client('rds', region_name=region)
            paginator = rds.get_paginator('describe_db_instances').paginate()
            for page in paginator:
                for dbinstance in page['DBInstances']:
                    logging.info(f'Found instance: {dbinstance.get("DBInstanceIdentifier")}')
                    if 'DBClusterIdentifier' in dbinstance:
                        logging.info(f'Skipping as instance is part of Multi-AZ Cluster or Aurora')
                        continue
                    row_dict = {'instance' : dbinstance.get('DBInstanceIdentifier', 'NaN'), \
                        'region' : args.region, \
                        'instance_type' : dbinstance.get('DBInstanceClass'), \
                        'db' : dbinstance.get('DBName', 'NaN'), \
                        'engine' : dbinstance.get('Engine', 'NaN'), \
                        'multi_az' : dbinstance.get('MultiAZ', 'NaN'), \
                        'storage_type' : dbinstance.get('StorageType', 'NaN'), \
                        'storage_size' : dbinstance.get('AllocatedStorage', 'NaN'), \
                        'storage_throughput' : dbinstance.get('StorageThroughput', 'NaN'), \
                        'storage_iops' : dbinstance.get('Iops', 'NaN') \
                        }
                    temp_df = pd.DataFrame([row_dict])
                    instance_df = pd.concat([instance_df, temp_df], ignore_index=True)
            logging.debug(f'RDS Instance list {instance_df}')
            return instance_df
        except Exception as e: 
            logging.error(f'An error occurred during instance info gathering')
            traceback.print_exc()

    def get_instance_usage(self, row, args, session, region):
        try:
            getdata = Getdata()
        
            # boto3 client config
            config = Config(
                retries = dict(
                    max_attempts = 10
                )
            )

            cw_client = session.client('cloudwatch', region_name=region, config=config)

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
                    'stat':'p98.00',
                    'period':86400
                },
                {
                    'metric_name':'ReadIOPS',
                    'namespace': 'AWS/RDS',
                    'instance_name': 'DBInstanceIdentifier',
                    'stat':'p98.00',
                    'period':86400
                },
                {
                    'namespace': 'AWS/RDS',
                    'instance_name': 'DBInstanceIdentifier',
                    'metric_name':'WriteThroughput',
                    'stat':'p98.00',
                    'period':86400
                },
                {
                    'namespace': 'AWS/RDS',
                    'instance_name': 'DBInstanceIdentifier',
                    'metric_name':'ReadThroughput',
                    'stat':'p98.00',
                    'period':86400
                }
            ]
            result_list = []

            for item in metric_dict:
                val = getdata.cw_rds_pull_metric(cw_client, item['metric_name'], item['namespace'], item['instance_name'], row.instance, item['stat'], item['period'], args)
                result_list.append(val)

            return result_list[0], result_list[1], result_list[2], result_list[3], result_list[4]
        except Exception as e: 
            logging.error(f'An error occurred during instance info gathering')
            traceback.print_exc()
    
    def get_instance_pricing_data(self, region):
        try:
            pricing_csv = f'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonRDS/current/{region}/index.csv'
            pricing_df = pd.read_csv(pricing_csv, skiprows=5)
            pricing_df.columns = pricing_df.columns.str.replace(' ', '')
            return pricing_df
        except Exception as e: 
            logging.error(f'An error occurred during bulk pricing pull')
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

    def calc_io1_throughput(self, row, rds_pricing_df, args):
        try:
            logging.debug(f'Calculating throughput for based on io1 iops')
            if row.storage_type == 'io1':
                # assume 32K per IOP
                io1_throughput = int((row.storage_iops * 32) / 1024)
                return io1_throughput
            else:
                return 'NaN'
        except Exception as e: 
            logging.error(f'An error occurred during io1 throughput calculation')
            traceback.print_exc()
            return 'NaN'

    def get_current_price(self, row, rds_pricing_df, args):
        try:
            logging.debug(f'Found instance with storage type of: {row.storage_type}')
            if row.storage_type == 'io1':
                # future - need to add functionality for Multi-AZ deployment with two readable standby instances
                if row.multi_az == True:
                    logging.debug(f'Found multi AZ instance')
                    return self.calc_io_costs(row, rds_pricing_df, ':Multi-AZ-PIOPS-Storage', ':Multi-AZ-PIOPS', args)
                if row.multi_az == False:
                    logging.debug(f'Found single Az instance')
                    return self.calc_io_costs(row, rds_pricing_df, ':PIOPS-Storage', ':PIOPS', args)
            else:
                return 'NaN'
        except Exception as e: 
            logging.error(f'An error occurred during io1 cost calculation')
            traceback.print_exc()
            return 'NaN'

    def gp3_adjustments(self, row):
        try:
            logging.info(f'Database engine: {row.engine}, storage size: {row.storage_size}, storage iops: {row.storage_iops}')
            if row.engine in ['postgres', 'mysql', 'mariadb']:
                if row.storage_size < 400:
                    if row.storage_iops < 3000:
                        return 3000, 125
                    else:
                        if row.storage_throughput >= 125:
                            return row.storage_iops, row.storage_throughput
                        else:
                            return row.storage_iops, 125
                else:
                    if row.storage_iops <= 12000:
                        return 12000, 500
                    elif 12000 < row.storage_iops <= 64000:
                        return row.storage_iops, row.storage_throughput
                    else: 
                        return 'NaN', 'NaN'
            elif 'sqlserver' in row.engine:
                if row.storage_iops < 3000:
                    return 3000, 125
                else:
                    return row.storage_iops, row.storage_throughput
            elif 'oracle' in row.engine:
                if row.storage_size < 200:
                    if row.storage_iops < 3000:
                        return 3000, 125
                    else:
                        return row.storage_iops, row.storage_throughput
                else:
                    if row.storage_iops < 12000:
                        return 12000, 500
                    elif 12000 < row.storage_iops <= 64000:
                        return row.storage_iops, row.storage_throughput
                    else: 
                        return 'NaN', 'NaN'
            else:
                return 'NaN'
        except Exception as e: 
            logging.error(f'An error occurred during gp3 io and adjustments')
            traceback.print_exc()
            return 'NaN'

    def calc_gp3_costs(self, row, rds_pricing_df, storage_gb, storage_iops, storage_throughput, args):

        # make adjustments for 
        storage_iops_adjusted, storage_throughput_adjusted = self.gp3_adjustments(row)

        logging.debug(f'Adjusted storage iops and throughput numbers: {storage_iops_adjusted}, {storage_throughput_adjusted}')

        # calculate monthly storage GB costs 
        temp_df = rds_pricing_df[rds_pricing_df['usageType'].str.contains(storage_gb)]
        per_unit = float(temp_df['PricePerUnit'].iat[0])
        gb_monthly_cost = float(row.storage_size) * per_unit
        logging.debug(f'GB monthly cost: {gb_monthly_cost}')

        # calculate monthly storage IOPS costs 
        temp_df = rds_pricing_df[rds_pricing_df['usageType'].str.contains(storage_iops)]
        per_unit = float(temp_df['PricePerUnit'].iat[0])
        # consider 3000 built in for baseline cost
        iops_monthly_cost = float(int(storage_iops_adjusted) - 3000) * per_unit
        logging.debug(f'IOPS monthly cost: {gb_monthly_cost}')

        # calculate monthly storage Throughput costs 
        temp_df = rds_pricing_df[rds_pricing_df['usageType'].str.contains(storage_throughput)]
        per_unit = float(temp_df['PricePerUnit'].iat[0])
        # consider 125 MB/sec built in for baseline cost
        if isinstance(storage_throughput_adjusted, int):
            throughput_monthly_cost = float(storage_throughput_adjusted - 125) * per_unit
            logging.debug(f'Throughput monthly cost: {throughput_monthly_cost}')

            if args.percent_discount is not None:
                storage_cost = ((gb_monthly_cost + iops_monthly_cost + throughput_monthly_cost) * (1.0 - args.percent_discount))
                storage_cost = self.round_up(storage_cost, 2)
            else:
                storage_cost = gb_monthly_cost + iops_monthly_cost + throughput_monthly_cost
        else:
            storage_cost = 'NaN'

        return storage_cost

    def get_future_price(self, row, rds_pricing_df, args):
        try:
            if row.storage_type == 'io1':
                # future - need to add functionality for Multi-AZ deployment with two readable standby instances
                if row.multi_az == True:
                    logging.debug(f'Calculating gp3 costs based on multi-az storage')
                    return self.calc_gp3_costs(row, rds_pricing_df, ':Multi-AZ-GP3-Storage', ':Multi-AZ-GP3-PIOPS', ':Multi-AZ-GP3-Throughput', args)
                if row.multi_az == False:
                    logging.debug(f'Calculating gp3 costs based on single-az storage')
                    return self.calc_gp3_costs(row, rds_pricing_df, ':GP3-Storage', ':GP3-PIOPS', ':GP3-Throughput', args)
            else:
                logging.debug(f'Instance is using gp3 future pricing calculation does not apply')
                return 'NaN'
        except Exception as e: 
            logging.error(f'An error occurred during future gp3 cost calculation')
            traceback.print_exc()
            return 'NaN'

    def gen_summary_statistics(self, instance_df):
        try:
            logging.debug(f'Summary statistics calculation')
            instance_df.drop(instance_df[instance_df['storage_type'] != 'io1'].index, inplace = True)
            instance_df.drop(instance_df[instance_df['current_monthly_storage_cost'] == 'NaN'].index, inplace = True)
            instance_df.drop(instance_df[instance_df['gp3_monthly_storage_cost'] == 'NaN'].index, inplace = True)

            old_cost = instance_df['current_monthly_storage_cost'].sum()
            new_cost = instance_df['gp3_monthly_storage_cost'].sum()

            total_savings = old_cost - new_cost
            logging.info(f'Prior io1 total cost: {old_cost}, converted gp3 total cost: {new_cost}')
            percent_reduction = int(abs((100 * float(new_cost)/float(old_cost))-100))
            logging.info(f'Total gp3 savings in USD: {total_savings}, gives a percent reduction of: {percent_reduction}')
            return instance_df
        except Exception as e: 
            logging.error(f'An error occurred during summary statistics calculation')
            traceback.print_exc()
