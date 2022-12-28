## Purpose

To estimate AWS cost savings by moving RDS storage from io1 to gp3 volume types.

**Steps performed**
- gather all rds instances in region 
- retrieving usage metrics from CloudWatch
- pull down RDS bulk price list for region
- determine current storage costs
- where io1 storage is used, determine gp3 equivilent pricing 
- output results to local csv 

**Data Dictionary**
- instance - RDS instance name
- region - AWS region 
- instance_type - instance type 
- db - database name 
- multi_az - multi-az deployment type 
- storage_type - storage type (e.g. io1, gp3)
- storage_size - provisioned storage in GB
- storage_throughput - throughput if gp3 storage is used
- storage_iops - configured IOPS for io1 or gp2
- cw_storage_free - storage available (from CloudWatch)
- cw_storage_write_iops - max storage write IOPS (from CloudWatch) (97 percentile)
- cw_storage_read_iops - max storage read IOPS (from CloudWatch) (97 percentile)
- cw_storage_write_throughput - max storage write throughput (from CloudWatch) (97 percentile)
- cw_storage_read_throughput - max storage read throughput (from CloudWatch) (97 percentile)
- current_monthly_storage_cost - currently monthly storage cost (with discount if specified)
- gp3_monthly_storage_cost - potential gp3 monthly storage cost (with discount if specified)

## Setup and Usage

- install libraries 
  ```py
  pip install -r requirements.txt
  ```
- specify days back from current time while considering a 19% PPA discount
  ```py
  python main.py -d 7 -r us-east-2 -p 0.19
  ```

<p align="right">(<a href="#readme-top">back to top</a>)</p>


## Roadmap

- [ ] Add ability for specific time range for looking at Cloudwatch data
- [ ] Add ability for rightsizing gp3 recommendations based on usage of provisioned IOPS storage

See the [open issues](https://somerepo.com) for a full list of proposed features (and known issues).

<p align="right">(<a href="#readme-top">back to top</a>)</p>


## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement". Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/UsefulFeature`)
3. Commit your Changes (`git commit -m 'Add some UsefulFeature'`)
4. Push to the Branch (`git push origin feature/UsefulFeature`)
5. Open a Pull Request

<p align="right">(<a href="#readme-top">back to top</a>)</p>


## Contact

Eric Garcia - grmeri@amazon.com





