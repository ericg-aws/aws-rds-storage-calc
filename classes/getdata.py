import boto3
from datetime import datetime, timedelta
import pandas as pd
from random import randrange
import traceback

class Getdata(object):
    
    def cw_rds_pull_metric(self, cw_client, metric_name, namespace, instance_name, instance, stat, period, args):
            try:
                print(f'Pulling cloudwatch data for instance: {instance} and metric: {metric_name}')
                id_name = f'rdsmetricpull{randrange(1000000)}'
                
                if args.start_time and args.end_time is not None:
                    start = args.start_time
                    end = args.end_time
                    # add in period_seconds calc
                else:
                    start = ((datetime.utcnow().replace(microsecond=0, second=0, minute=0) - timedelta(hours=1)) - timedelta(days=args.days_back))
                    end = (datetime.utcnow().replace(microsecond=0, second=0, minute=0) - timedelta(hours=1))
                    timespan = end - start 
                    period_seconds = (60 * int(timespan.total_seconds() / 60 ))

                cw_response = cw_client.get_metric_data(
                    MetricDataQueries=[
                        {
                            'Id': id_name,
                            'MetricStat': {
                                'Metric': {
                                    'Namespace': namespace,
                                    'MetricName': metric_name,
                                    'Dimensions': [
                                        {
                                            'Name': instance_name,
                                            'Value': instance
                                        },
                                    ]
                                },
                                'Period': period_seconds,
                                'Stat': stat,
                            },
                            'ReturnData': True
                        }
                    ],
                    StartTime=start,
                    EndTime=end,
                    ScanBy='TimestampDescending'
                )

                df_temp = pd.DataFrame(cw_response['MetricDataResults'][0])
                if df_temp.empty:
                    return 'NaN'
                else: 
                    return df_temp['Values'].iloc[0].round(decimals = 0)
            except Exception as e: 
                print(f'An error occurred cloudwatch metric pull for {instance}')
                traceback.print_exc()
                return 'NaN'