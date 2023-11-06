# Antipode @ Post-Notification AWS Lambda

In this repo you will find found how Antipode fixes the **cross-service inconsistency** described by [Facebook](https://www.usenix.org/conference/hotos15/workshop-program/presentation/ajoux), and simplified in the form of a microbenchmark that runs on top of AWS Lambda, depicted in the following picture.

![Post-Notification](post-notification.jpeg)

In this application, users can upload posts and followers receive notifications.
Internally, the application comprises two key service namely:
- a *Writer* service (comprised of `post-upload` and `post-storage` services) that works as a proxy for the clients and is responsible for storing and processing the contents of posts
- a *Reader* service (comprised of `notifier` and `follower-notify` services) in charge of disseminating notification events which notifies followers of new posts.

In our implementation, we each service corresponds to a Lambda functions, which access off-the-shelf datastores. Each external client request spawns a Writer call, which writes the new post to post-storage, and then creates a new notification in the notifier.\
Meanwhile, a new Reader is spawned when a new notifier replication event is received.\
For the off-the-shelf datastores we used combinations MySQL, DynamoDB, S3, and Redis for storing posts; and SNS, AMQ, and DynamoDB for notification events.

**Cross-service inconsistencies** can occur in this application: followers in Region B can be notified of posts that do not yet exist in that region, in other words, if when reading a post we receive a `object not found` error, then an inconsistency occured.\
Antipode solves this violation by placing a barrier right after the Reader receives the notification replication event.


## Prerequisites
1. Docker
2. Python 3.8
3. Install requiremnts `pip3 install -r requirements.txt`
4. Configure AWS according to instruction below.

### AWS Configurations

Start by configuring your AWS credentials:
1. Install [AWS cli](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) tools `aws` (version 2)
2. Configure your local authentication profile `aws configure`
3. Copy generated credentials to the application path `cp ~/.aws/credentials .`

We assume the following regions:
- For EU we use Europe (Frankfurt) datacenter and the `eu-central-1a` availability zone
- For US we use US East (N. Virginia) datacenter and the `us-east-1a` availability zone
- For AP we use Asia Pacific (Singapore) datacenter and the `ap-southeast-1a` availability zone

For each resource configuration, do not forget to set up the correct endpoints in the corresponding sections (lambda and datastores) in the `connection_info.yaml` file.

**WARNING:** In AWS, go to Service Quotas, AWS Lambda and make sure the apply quota value of concurrent executions is set to 1000 in all regions listed in the following sections.

#### IAM

1. Create a role named `antipode-cloudformation-admin` (name is defined at the end):
    - Trusted Entity Type: `AWS Service`
    - Use Case: search and select `CloudFormation`
    - Next, add the following permission policy: `Administrator Access`
2. Create a role named `antipode-lambda-admin` (name is defined at the end):
    - Trusted Entity Type: `AWS Service`
    - Use Case: `Lambda`
    - Next, add the following permission policies:
        - `Administrator Access`
        - `AmazonDynamoDBFullAccess`
        - `AmazonEC2FullAccess`
        - `AmazonElastiCacheFullAccess`
        - `AmazonSNSFullAccess`
        - `AmazonSQSFullAccess`
        - `AmazonVPCFullAccess`
        - `AmazonMQFullAccess`
        - `AWSLambda_FullAccess`
3. Create a role named `antipode-s3-admin` (name is defined at the end):
    - Trusted Entity Type: `AWS Service`
    - Use Case: search and select `S3`
    - Next, add the following permission policies:
        - `AmazonS3FullAccess`
4. Add the endpoints for the first two roles at the begging of `connection_info.yaml`.

#### Evaluation Queue (AWS SQS)
1. Go to each reader region  (`us-east-1`, `ap-southeast-1`) zone and to the AWS SQS dashboard
2. Create queue with the following parameters:
    - Standard type
    - Name: `antipode-lambda-eval`

#### VPC
As a tip use the same name for all objects, its easier to track. We use `antipode-mq`
1. Create a VPC with a unique IPv4 CIDR block, distinct from the ones used in other regions, as exemplified in the connections info file:
    - eu: `50.0.0.0/16`
    - us: `51.0.0.0/16`
    - ap: `52.0.0.0/16`
    - *MAIN CONCERN*: Amazon MQ peering connection WILL NOT WORK ON OVERLAPPING CIDR BLOCKS ACROSS REGIONS. Hence choose a unique one for each region VPC
2. After creating select the create vpc, click on `ACTIONS`, go to `Edit VPC settings` and enable DNS hostnames
3. Create two subnets, one for each Availability Zone (`a` and `b`). For example:
    - eu: `50.0.0.0/20`, `50.0.16.0/20`
    - us: `51.0.0.0/20`, `51.0.16.0/20`
    - ap: `52.0.0.0/20`, `52.0.16.0/20`
    - *MAIN CONCERN*: Amazon ElastiCache (redis) requires an additional subnet with different AZ for the additional replica.
    - *IMPORTANT REMINDER*: the subnet ids used in connections info file correspond to the first one for each zone
4. Go to Security Groups and select the default one for the created vpc.
    - Inbound rules: Add 2 rules for `ALL TRAFFIC` to Any IPv4 (`0.0.0.0/0`) and IPv6. Make sure you have a rule for the same SG ID
    - Outbout rules: Add 2 rules for `ALL TRAFFIC `to Any IPv4 (`0.0.0.0/0`) and IPv6. Make sure you have a rule for the same SG ID
5. Create an Internet Gateway
    - After creating go to `Actions` and attach it to the VPC
6. Go to Route Tables and select the one created (check the matching vpc id)
    - Go to Edit Routes and add an entry for `0.0.0.0/0` with target to the created internet gateway - select Internet Gateway and the id will appear
7. Go to Endpoints and create an entrypoint for AWS Services needed. Make sure you select the correct VPC, Subnet for the `a` Availability Zone and Security Group:
    - Reader (`eu-central-1`): SQS
    - Writer (`us-east-1`, `ap-southeast-1`): SNS, Dynamo (Gateway)

#### MySQL (AWS RDS)
Before starting, in each of the zones where you will be deploying MySQL, go the AWS RDS dashboard:
1. On the left side, click on `Parameter Groups`. And create a new one (e.g. `aurora-mysql5.7`)
2. Although you can let the default parameters stay, you might want to increase max_connections

Now we setup the cluster per se ([reference](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-global-database.html)):
1. Go to the writer zone `eu-central-1`
2. Go to AWS RDS dashboard and click on `Create Database`
3. Select `Standard Create`
    - Engine type: Amazon Aurora with MySQL compatibility
    - On the filters enable the `Show versions that support the global database feature` filter
    - Select a MySQL version that supports `Global Database` (info on the right side panel), e.g. Aurora MySQL 5.7 v2.11.2
    - Select `Production` template
    - DB cluster identifier: `antipode-lambda-eu`
    - For credentials you can use the following:
        - Master Username: `antipode`
        - Master Password: `antipode`
    - Select a memory optimized machine (e.g. `db.r6.large`). You can tick `Include previous generations` for older and cheaper instances.
    - Do not create Multi-AZ deployment
    - Choose the `Default VPC`. _Warning_: do not try to change the `antipode-mq` VPC to support RDS by adding more subnets -- use a different one.
    - Enable `Public access`
    - Choose the existing `allow-all` VPC security group. If its not created, you should create with:
        - ALL TRAFFIC open for all IPv4 and IPv6, in inbound and outbound
        - Rule to allow itself - the security group - in inbound and outbound
    - Select the AZ terminated in `a` <!-- (is it needed??) -->
    - On `Additional configuration` make sure the database port is `3306`
    - On `Monitoring`
        - Disable `Performance Insights`
        - Disable `Enhanced Monitoring` on the additional configurations
    - On `Additional configuration`:
        - Leave the `Initial database name` blank as we will create later
        - Set the `DB cluster parameter group` and the `DB parameter group` with the previously created parameter group
        - Disable Encryption
        - Disable `Auto minor version upgrade`
4. Wait for the database instances to be created
5. In `Databases`, select the top level entry named `antipode-lambda-eu` with type `Regional cluster`. Click on Actions and `Add AWS region`. You will get to a `Add Region` panel where you can setup the new replica:
    - Global database identifier: `antipode-lambda`
    - Select secondary region, e.g. `US East (N. Virginia)` which would mean that
    - Select the same model of machine selected in the writer zone (e.g. `db.r6.large`)
    - Do not create Multi-AZ deployment
    - Choose the `Default VPC`.
    - Enable `Public access`
    - Select the `allow-all` VPC security group. If its not created, you should create with:
        - ALL TRAFFIC open for all IPv4 and IPv6, in inbound and outbound
        - Rule to allow itself - the security group - in inbound and outbound
    - Select the AZ terminated in `a` <!-- (is it needed??) -->
    - On `Additional configuration` make sure the database port is `3306`
    - Keep `Turn on global write forwarding` disabled
    - On `Additional configuration`:
        - DB instance identifier, e.g. `antipode-lambda-us-instance`
        - DB cluster identifier: `antipode-lambda-us`
        - Disable `Performance Insights`
        - Disable `Enhanced Monitoring`
        - Disable `Auto minor version upgrade`
6. When everything is created run `./antipode_lambda clean` that will automatically create MySQL tables
7. Go to the `connection_info.yaml` file and fill out the cluster endpoints for each zone. To get the endpoints go to the RDS dashboard and on the `Databases` list select the corresponding instance. On the panel below, select the `Connectivity & security` tab and copy the value under `Endpoint`.
    - RDS Writer instance corresponds to the `writer` instance endpoint
    - RDS Reader instance corresponds to the `reader` instance endpoint
8. Finally run `./antipode_lambda clean -n mysql` so we create database and tables


#### SNS
1. Go to `eu-central-1` zone and to the AWS SNS dashboard
2. Go to Topics and create a new one with the following parameters:
    - Standard type
    - name: `antipode-lambda-notifications`

#### S3
1. Go AWS dashboard and create buckets within all zones. Note that names are unique and you will probably need to use a different one
    - Name: `antipode-lambda-posts-<region>`
    - Enabled versioning
2. Go the bucket in the _primary region_. Go to `Management` tab and create replication from the bucket in that region other region's buckets
    - Name: `to-reader-<secondary region>`
    - Rule scope: apply to all objects
    - On Destination click `Browse S3` and find the bucket named: `antipode-lambda-posts-<secondary region>`
    - Use the `antipode-lambda-s3-admin` IAM role. This is a rule that gives S3 admin access to operations needed
    - Do not select RTC
    - When created do not choose `Replicate existing objects`

<!---
NOTE: we should also change the replication priority for each deployment (input on code and wait for changes in dashboard?)
-->

#### DynamoDB
1. On each region create the following tables:
    - `posts`: with `k` as partition key
    - `notifications`: with `k` as partition key
2. Update the `connection_info.yaml` file with the new table names
3. For remaining settings select everything default. In table settings, select customize settings and change `Read/Write` capacity settings to `On-demand`
4. After created go to DynameDB dashboard on the _primary_ region and select `Tables` on the left-hand panel. For each table do the following:
        - Go to `Global Tables`
        - Create replica to the desired region
        - Double check in secondary region if tables got created
5. For the `notifications` tables in the secondary regions, go to `Export` and `Streams` and obtain the stream ARN to be configured in the `connection_info.yaml` file

#### Redis (AWS ElastiCache)
1. Go to AWS ElastiCache dashboard and on the left-side panel select `Global Datastores`.

2. Click on `Create global cluster`. Start with the primary (writer) zone. If you are adding a zone to an existing cluster just go to the dashboard and click on `Add zone`. The properties are similar for the other zones you add to the cluster. Configure each zone in the `antipode-lambda` cluster:
    - Keep `Cluster mode` disabled
    - For the `Global Datastore info` use `antipode-lambda`
    - Create a regional cluster with the region's name, e.g. `antipode-lambda-eu`
    - Set `Engine version` to `6.2` (a different version should not impact results)
    - Set `Port` to `6379` (or the one you define in `connection_info.yaml`)
    - Set node type to e.g. `cache.r5.large`
    - Set `Number of replicas` to 1
    - Create a new Subnet group:
        - Name: `antipode-mq-ec`
        - Select previously created VPC (`antipode-mq`).
    - Confirm that the `Availability Zone placements` are the same as the AZ from the subnet group. Make the primary on the `a` AZ.
    - Disable `Auto upgrade minor versions`
    - Disable backups
    - Select the default Security Group for the chosen VPC
    - If following the AWS form you should create the secondary (reader) zone next with similar configurations as before but in a new zone.

<!-- Make sure the subnet in the `connection_info.yaml` file is the one that the reader instance is using -->
3. Finally, set the the endpoints in the `connection_info.yaml` file. Go back to the AWS ElastiCache dashboard, click on `Redis clusters` on the left-hand panel, click on the regional cluster name, e.g. `antipode-lambda-eu` and the copy the `Primary endpoint` _without_ the port. On the seconda

**WARNING**: We found that for some new accounts using the AWS ElastiCache, you might have to create an EC2 instance on the same zone of your cluster and then perform an initial request to kinda "unlock" the zone for ElastiCache.

#### AMQ
1. Using the previously created VCP, you have to add peering between the reader and writer zone
    - Check the following material for more details:
        - https://docs.aws.amazon.com/vpc/latest/peering/create-vpc-peering-connection.html
        - https://docs.aws.amazon.com/vpc/latest/peering/vpc-peering-routing.html

      **HUGE WARNING**: WILL NOT WORK WITH VPCs WITH OVERLAPING CIDRS

    - Go to the the secondary zone and create a new Peering Connection
        - Name: `antipode-mq-<primary>-<secondary>` (e.g. antipode-mq-eu-us)
            - Select the previously created VPC
            - The select the primary zone and paste the previously created VPC id
        - Go to the Peering Connections on the primary zone and accept the pending request (you might want to change the name as well)
        - On both zones go to the Routing Table. We will match the pair the CIDR blocks
            - On zone `REGION-A` add the entry: `<REGION-B CIDR block> -> pcx-id (peering connection)`
            - On zone `REGION-B` add the entry: `<REGION-A CIDR block> -> pcx-id (peering connection)`
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
          0.0.0.0/0	    igw-id      (internet gateway)
          (you might have more entries from the endpoint configurations)
          ```

2. Go the all the zones and create a broker with the following configuration:
    - Engine: Apache ActiveMQ
    - Single-instance broker
    - Durability optimized
    - Broker name: `antipode-lambda-notifications-<region>`
    - Username: `antipode`
    - Password: `antipode1antipode`
    - Broker engine: 5.16.2
    - Select to create a default configuration
    - Select pre-created VPC config: `antipode-mq`
    - Select pre-created Security group: `antipode-mq`
    - Disable maintenance
3. Double check that you CAN access the public broker management dashboard
4. Go the the PRIMARY (writer) zone and edit the created configuration by uncommenting the networkConnectors blocks and replace with this (change the uris as needed):
    ref: https://docs.aws.amazon.com/amazon-mq/latest/developer-guide/amazon-mq-creating-configuring-network-of-brokers.html
    ```xml
    <networkConnectors>
        <networkConnector duplex="true" name="ConnectorEuToUs" uri="static:(ssl://b-6cfdfde0-2f84-4723-94bd-cc9ada66c2a9-1.mq.us-east-1.amazonaws.com:61617)" userName="antipode"/>
        <networkConnector duplex="true" name="ConnectorEuToSg" uri="static:(ssl://b-6cfdfde0-2f84-4723-94bd-cc9ada66c2a9-1.mq.us-east-1.amazonaws.com:61617)" userName="antipode"/>
    </networkConnectors>
    ```

5. Go the broker again and change the REVISION of the configuration file and do APPLY IMMEDIATLY
6. In your local machine test the queue by creating a consumer on a secondary region to the primary region (change url):
    ```
    activemq consumer --brokerUrl "ssl://b-20f3cf89-7725-44b0-946b-19e84c03b81e-1.mq.us-east-1.amazonaws.com:61617" \
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

    - Go the the dashboard of the created broker in AWS (ActiveMQ Web Console -> Manage ActiveMQ Brocker -> Queues) and you should see 10 messages enqueued and dequeued

7. In your local machine create a secret for MQ lambda access on the primary region:
    `aws secretsmanager create-secret --region us-east-1 --name antipode-mq --secret-string '{"username": "antipode", "password": "antipode1antipode"}' `
    - After created edit the secret and replicate to secondary regions if needed (ap-southeast)



## Usage
For a pair of post-storage and notification-storage backends, for instance mysql and sns respectively, and for two regions as writer and reader, for instance EU and US respectively, do the following:
```zsh
./antipode_lambda build --post-storage dynamo --notification-storage sns --writer eu --reader us
```
To enable Antipode add `-ant` parameter, and to introduce artificial delay before publishing the notification add `--delay <time>` parameter.

Then you run a the build with:
```zsh
./antipode_lambda run -r 1000
```
Which will trigger 1000 writer lambdas

After the run ends you can start gathering the results with:
```zsh
./antipode_lambda gather
```

At the end you need to clean your experiment with:
```zsh
./antipode_lambda clean --strong
```
The `strong` flag will remove the deployed lambdas. If you remove the flag it will just clean storages so you can run again.

As an alternative method, you can run our maestrina script (all regions, all combinations) with `./maestrina`.
Note that, if you find any errors you can always run a single combination using the `antipode_lambda` as described above.


## Plots

At the end, you can build plots for consistency window and delay vs. inconsistency.

Copy the `sample.yml` in plots/configs, renamed it and configure according to your gather traces.

#### Consistency Window

In your new config file, provide the gather paths in `consistency_window` for each post and notification storages directory.

Note that the antipode trace needs to be listed before the original, as exemplified in the sample file

Build the plot:
```zsh
./plot plots/configs/sample.yml --plots consistency_window
```

#### Delay vs Inconsistencies Percentage

In your new config file, provide the gather paths in `delay_vs_per_inconsistencies` for each post and notification storages directory.

Change the combinations as needed and build the plot:
```zsh
./plot plots/configs/sample.yml --plots delay_vs_per_inconsistencies
```

#### Storage Overhead

In your new config file, provide the gather paths in `storage_overhead`. Change the combinations as needed and build the plot:
```zsh
./plot plots/configs/sample.yml --plots storage_overhead
```


## Paper References
João Loff, Daniel Porto, João Garcia, Jonathan Mace, Rodrigo Rodrigues\
Antipode: Enforcing Cross-Service Causal Consistency in Distributed Applications\
To appear.\
[Paper](Download)

Phillipe Ajoux, Nathan Bronson, Sanjeev Kumar, Wyatt Lloyd, Kaushik Veeraraghavan\
Challenges to Adopting Stronger Consistency at Scale\
HotOS 2015.\
[Paper](https://www.usenix.org/system/files/conference/hotos15/hotos15-paper-ajoux.pdf)
&nbsp;&nbsp;&nbsp;
[Presentation](https://www.usenix.org/sites/default/files/conference/protected-files/hotos15_slides_ajoux.pdf)