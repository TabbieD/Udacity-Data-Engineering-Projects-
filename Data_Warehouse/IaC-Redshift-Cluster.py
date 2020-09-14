import boto3
import json
import pandas as pd
import time
from botocore.exceptions import ClientError
import configparser

file = open('dwh.cfg')
config = configparser.ConfigParser()
config.read_file(file)

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH", "DWH_PORT")

DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")


# 1.1 Creating the role
def create_role(iam_client):
    try:
        print("1.1 Creating a new IAM Role")
        dwhRole = iam_client.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)


# Attach Policy.
def attach_policy(iam_client):
    print("1.2 Attaching Policy")
    policy = iam_client.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                                           )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam_client.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    return roleArn


# Creating Redshift Cluster.
def create_cluster(redshift_client, roleArn):
    try:
        response = redshift_client.create_cluster(
            # HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            # Roles (for s3 access)
            IamRoles=[roleArn]
        )
    except Exception as e:
        print(e)


# Describe cluster.
def get_props(props):
    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus",
                  "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k, v in props.items() if k in keysToShow]

    return pd.DataFrame(data=x, columns=["Key", "Value"])


# Get endpoint and roleArn.
def get_endpoint():
    _myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    _ENDPOINT = _myClusterProps['Endpoint']['Address']
    _ROLE_ARN = _myClusterProps['IamRoles'][0]['IamRoleArn']

    return _ENDPOINT, _ROLE_ARN


# Open incoming  TCP port to access the cluster endpoint.
def openTCP_port(ec2_client, _myClusterProps):
    try:
        vpc = ec2_client.Vpc(id=_myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)

        defaultSg.authorize_ingress(
            GroupName='default',
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)


# Delete resources
def delete_cluster(redshift_client):
    redshift_client.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)
    print("Deleting Cluster...")
    time.sleep(600)

    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)


if __name__ == "__main__":
    import boto3

    ec2 = boto3.resource('ec2',
                         region_name="us-east-1",
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET
                         )

    iam = boto3.client('iam',
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

    redshift = boto3.client('redshift',
                            region_name="us-east-1",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )

    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )
    print("At the prompt, input 1 or 2:\n")
    print("1] create resources\n")
    print("2] delete resources.")
    print("3] temporal check status")
    user_input = int(input("--> "))
    if user_input == 1:
        create_role(iam)
        DWH_ROLE_ARN = attach_policy(iam)
        create_cluster(redshift, DWH_ROLE_ARN)
        print("Provisioning...")

    elif user_input == 3:
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        cluster_properties = [(k, v) for k, v in myClusterProps.items() if k == "ClusterStatus"]
        while cluster_properties[0][1] != "available" and cluster_properties[0][1] != "deleting":
            time.sleep(60)
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            cluster_properties = [(k, v) for k, v in myClusterProps.items() if k == "ClusterStatus"]

        if cluster_properties[0][1] == "available":
            # openTCP_port(ec2, myClusterProps)
            config = configparser.ConfigParser()
            DWH_ENDPOINT, DWH_ROLE_ARN = get_endpoint()
            config.add_section('CLUSTER')
            config.set('CLUSTER', 'DWH_ENDPOINT', DWH_ENDPOINT)
            config.set('CLUSTER', 'DWH_ROLE_ARN', DWH_ROLE_ARN)
            file = open('dwh.cfg', 'a')
            config.write(file)
            file.close()
            print("Cluster Ready")

    elif user_input == 2:
        delete_cluster(redshift)

    else:
        print("Invalid input parameter.")
