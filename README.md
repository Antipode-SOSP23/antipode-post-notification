# Installation

## Standalone
1. Make you have `docker` and `python3` installed
2. Install requiremnts `pip3 install -r requirements.txt`
3. Install AWS cli tools `aws` and `sam`
4. Copy your credentials to your home path `mkdir -p ~/.aws/ && cp credentials ~/.aws/credentials`

## Docker version
Due to a bug with aws sam client this deployment is currently not working

1. Build Docker image
> docker build -t antipode-lambda .

2. Run docker container
> docker run --name antipode-lambda --rm -ti -v "$(pwd):/app" antipode-lambda bash


# How to run
1. You start by building the setup: `./antipode_lambda build --post-storage mysql --notification-storage sns --writer eu --reader us`
If you have antipode add `-ant` to your options

2. Then you run a certain number of requests: `./antipode_lambda run -r 5000`

3. Then you gather results with an optional tag: `./antipode_lambda gather -t debug`

4. Finally you clean your experiment in a strong way to remove deployed lambda `./antipode_lambda clean --strong`

As an alternative method, you can run our eval script (all regions, all combinations): `./eval`


# AWS Configurations

## SQS EVAL QUEUE
1. Go to us-east-1 zone and to the AWS SQS dashboard
2. Create queue with the following parameters:
    - Standard type
    - name: antipode-lambda-notifications

## VPC
As a tip use the same name for all objects, its easier to track. We use 'antipode-mq'
1. Create a VPC with a unique CIDR block
    - *MAIN CONCERN*: Amazon MQ peering connection WILL NOT WORK ON OVERLAPPING CIDR BLOCS. Hence choose a unique one for each region VPC
2. After creating click on ACTIONS and enable DNS hostnames
3. Create a subnet with the full CIDR block
4. Go to Security Groups and select the default one.
    - Inbound rules: Add 2 rules for ALL TRAFFIC to Any IPv4 (0.0.0.0/0) and IPv6. Make sure you have a rule for the same SG ID
    - Outbout rules: Add 2 rules for ALL TRAFFIC to Any IPv4 (0.0.0.0/0) and IPv6. Make sure you have a rule for the same SG ID
5. Create an Internet Gateway
    - After creating attach it to the VPC
6. Go to the created subnets default Route Table
    - Add an entry for 0.0.0.0/0 to the created internet gateway
7. Go to Endpoints and create an entrypoint for AWS services needed. Make sure you select the correct VPC and Subnet
    - Reader: SNS, SQS
    - Writer: SNS, Dynamo (Gateway), ec2

## Aurora Mysql Global Cluster
- In each of the zones first create a Parameter Group
1. Go to Dashboard, click on "Parameter Groups". Create a new one
2. Although you can let the default parameters stay, later you might want to change max_connections

- Now we setup the cluster
1. Go to eu-central-1 zone
2. Go to RDS dashboard and click on "Create Database"
3. Select "Standard Create"
    - Engine type: Amazon Aurora
    - MySQL compatibility
    - Provisioned
    - Single Master
    - Select a version that supports "Global Database" feature
    - Select PRODUCTION template
    - Cluster name: 'antipode-lambda-eu'
    - Username: 'antipode' / Password: 'antipode'
    - Select lowest memory optimized machine
        - Tick "Include previous generations" for older and cheaper instances
    - Do not create Multi-AZ deployment
    - Public access: YES
    - Choose 'allow-all' VPC group
    - Database port: 3306
    - Disable Encryption
    - Disable Performance Insights
    - Disable Enhanced monitoring
    - Disable auto minor version upgrade
    - Enable delete protection
4. Wait for all the instances to be created
5. Select the top level "Global Database". Click on Actions and "Add AWS region". You will get to a "Add Region" panel where you can setup the new replica:
    - Secondary region: <region_name>
    - Select lowest memory optimized machine
    - Do not create multi-az deployment
    - Select the default VPC
        - DO NOT CHANGE antipode-mq to support RDS by adding more subnets
    - Enable Public access
    - Select the 'allow-all' VPC security group. If its not created, you should create with:
        - ALL TRAFFIC open for all IPv4 and IPv6, in inbound and outbound
        - Rule to allow itself - the security group - in inbound and outbound
    - Select the AZ terminated in a (?? needed)
    - Do not enable "read replica write forwarding"
    - DB instance identifier: antipode-lambda-<region name>-instance
    - DB cluster identifier: antipode-lambda-<region name>
    - Disable Performance Insights
    - Disable Monitoring
    - Disable Auto minor version upgrade

ref: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-global-database.html

## SNS
1. Go to eu-central-1 zone and to the AWS SNS dashboard
2. Go to Topics and create a new one with the following parameters:
    - Standard type
    - name: antipode-lambda-notifications

## S3
1. Go AWS dashboard and create buckets within all zones
    - Name: antipode-lambda-posts-<region>
    - Enabled versioning
2. Go the primary region and create replication from the bucket in that region other buckets
    - Name: to-reader-<secondary region>
    - Rule scope: apply to all objects
    - On Destination click 'Browse S3' and find the bucket named: antipode-lambda-posts-<secondary region>
    - Use the 'antipode-lambda-s3-admin' IAM role
        - This is a rule that gives S3 admin access to operations needed
    - Do not select RTC

NOTE: we should also change the replication priority for each deployment (input on code and wait for changes in dashboard?)

## DYNAMO
1. On each region create for posts, notifications and cscopes
    - For name check the connection_info file
    - Select everything default
2. After created go to dashboard on the primary region and select Tables:
    - For the 3 tables (posts, notifications, cscopes) do the following:
        - Go to Global Tables
        - Create replica to the desired region
        - Double check in secondary region if tables got created

## ELASTICACHE (Redis)
1. Create a global cluster. Start with the primary zone (if you are adding a zone to an existing cluster just go to the dashboard and add zone). The properties are similar for the other zones you add to the cluster
    - Name: antipode-lambda-<region>
    - Port: 6379 (or the one you define in connection_info.yaml)
    - Node type: cache.r5.large
    - Num replicas: 1
    - Create a new Subnet group:
        - Name: antipode-lambda-<region>
        - Select previously created VPC and Subnet group
        - Select the AZ preference to the only AZ that should be there
    - Select the default SG for the choosed VPC
    - Disable backups

**WARN/BUG**: you might have to create an EC2 instance on the zone and perform an initial request to "unlock" the zone for EC

## AMQ
1. Using the previously created VCP, you have to add peering between the reader and writer zone
    - Check the following material for more details:
        - https://docs.aws.amazon.com/vpc/latest/peering/create-vpc-peering-connection.html
        - https://docs.aws.amazon.com/vpc/latest/peering/vpc-peering-routing.html
    - **HUGE WARNING**: WILL NOT WORK WITH VPCs WITH OVERLAPING CIDRS

    - Go to the the secondary zone and create a new Peering Connection
        - Name: antipode-mq-<primary>-<secondary> (e.g. antipode-mq-eu-us)
            - Select the previously created VPC
            - The select the primary zone and paste the previously created VPC id
        - Go to the Peering Connections on the primary zone and accept the pending request (you might want to change the name as well)
        - On both zones go to the Routing Table. We will match the pair the CIDR blocks
            - On zone REGION-A add the entry: <REGION-B CIDR block> -> pcx-id (peering connection)
            - On zone REGION-B add the entry: <REGION-A CIDR block> -> pcx-id (peering connection)
          At the end of the whole setup, primary should have a configuration similar to this one:
          ```
          50.0.0.0/16	local       (self)
          51.0.0.0/16	pcx-id      (peering connection to secondary, e.g. eu-us)
          52.0.0.0/16	pcx-id      (peering connection to secondary, e.g. eu-sg)
          0.0.0.0/0	  igw-id      (internet gateway)
          (you might have more entries from the endpoint configurations)
          ```

          And the secondaries should have a configuration similar to this one:
          ```
          52.0.0.0/16	local       (self)
          50.0.0.0/16	pcx-id      (peering connection to primary, e.g. eu-sg)
          0.0.0.0/0	  igw-id      (internet gateway)
          (you might have more entries from the endpoint configurations)
          ```

2. Go the all the zones and create a broker with the following configuration:
    - Engine: Apache ActiveMQ
    - Single-instance broker
    - Durability optimized
    - Broker name: antipode-lambda-notifications-<region>
    - Username: antipode
    - Password: antipode1antipode
    - Broker engine: 5.16.2
    - Select to create a default configuration
    - Select pre-created VPC config: antipode-mq
    - Select pre-created Security group: antipode-mq
    - Disable maintenance
3. Double check that you CAN access the public broker management dashboard
4. Go the the PRIMARY (writer) zone and edit the created configuration by uncommeting the networkConnectors blocks and replace with this (change the uris as needed):
    ref: https://docs.aws.amazon.com/amazon-mq/latest/developer-guide/amazon-mq-creating-configuring-network-of-brokers.html
    ```xml
    <networkConnectors>
        <networkConnector duplex="true" name="ConnectorEuToUs" uri="static:(ssl://b-6cfdfde0-2f84-4723-94bd-cc9ada66c2a9-1.mq.us-east-1.amazonaws.com:61617)" userName="antipode"/>
        <networkConnector duplex="true" name="ConnectorEuToSg" uri="static:(ssl://b-6cfdfde0-2f84-4723-94bd-cc9ada66c2a9-1.mq.us-east-1.amazonaws.com:61617)" userName="antipode"/>
    </networkConnectors>
    ```

5. Go the broker again and change the REVISION of the configuration file and do APPLY IMMEDEATLY
6. Create a consumer on a secondary region to the primary region (change url):
    ```
    activemq consumer --brokerUrl "ssl://b-20f3cf89-7725-44b0-946b-19e84c03b81e-1.mq.ap-southeast-1.amazonaws.com:61617" \
                    --user antipode \
                    --password antipode1antipode \
                    --destination queue://antipode-notifications
    ```

    - Double check with a producer
    ```
    activemq producer --brokerUrl "ssl://b-8b026a92-1858-4a76-bc7a-7bfb25be209d-1.mq.eu-central-1.amazonaws.com:61617" \
                --user antipode \
                --password antipode1antipode \
                --destination queue://antipode-notifications \
                --persistent true \
                --messageSize 1000 \
                --messageCount 10
    ```

    - Go the the dashboard and you should see 10 messages enqueued and dequeued

7. Create a secret for MQ lambda access on the primary region:
    `aws secretsmanager create-secret --region us-east-1 --name antipode-mq --secret-string '{"username": "antipode", "password": "antipode1antipode"}' `
    - After created edit the secret and replicate to secondary regions