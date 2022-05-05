import boto3
from dynaconf import *
config_profile = Dynaconf(settings_files=['artemis-storage-config.yaml'])


def Elasticsearch(config, env, ops_bucket_name, region):
    dynamic_params = ['InstanceType', 'ElasticSearchVersion', 'KMSEncryptionKey']
    global_params = ['NamingSuffix']

    filtered_params = filter_parameters(global_params, config)
    filtered_params.extend(filter_parameters(dynamic_params, config.ElasticSearchConfig))
    filtered_params.append({"ParameterKey": "Environment", "ParameterValue": env})

    stack_name = f"kf-artemis-elasticsearch-{env}-{config.NamingSuffix}"
    bucket = ops_bucket_name
    key = config.ElasticSearchConfig.ElasticSearchCFTemplateURL
    s3_client = create_client("s3", region=region)
    template = s3_client.get_object(Bucket=bucket, Key=key)
    deploy_stack(stack_name, template, filtered_params, region)


def create_s3databucket(config, env, ops_bucket_name, region):
    dynamic_params = ['SFTPEnabled', 'SFTPUserRoleId', 'DataAthenaQuerying',
                     'BucketSecurity', 'BucketVersioning', 'DataRetentionRequirement']

    global_params = ['ProjectId', 'NamingSuffix', 'BlockPrefix', 'OpsBucketName']

    filtered_params = filter_parameters(global_params, config)
    filtered_params.extend(filter_parameters(dynamic_params, config.S3DataBucketConfig))

    filtered_params.append({"ParameterKey": "Environment", "ParameterValue": env})

    StackName = f"kf-artemis-s3-bucket-data-{env}-{config.NamingSuffix}"
    bucket = ops_bucket_name
    key = config.S3DataBucketConfig.S3BucketCFTemplateURL
    s3Client = create_client("s3", region=region)
    template = s3Client.get_object(Bucket=bucket, Key=key)
    deploy_stack(StackName, template, filtered_params, region)

    
def abc():
    num = 42
    if num == 42:
        if num == 24:
            return 24
    else: 
        return 2 

def _stack_exists(stack_name, cf):
    stacks = cf.list_stacks(StackStatusFilter=['CREATE_COMPLETE', 'UPDATE_COMPLETE', 'UPDATE_ROLLBACK_COMPLETE'])[
        'StackSummaries']
    for stack in stacks:
        if stack['StackStatus'] == 'DELETE_COMPLETE':
            continue
        elif stack['StackStatus'] == '':
            continue
        elif stack['StackStatus'] == 'DELETE_COMPLETE':
            return
        elif stack_name == stack['StackName']:
            return True
    return False
def _parse_template(template, cf):
    try:
        templateData = template["Body"].read().decode()
    except TypeError as e:
        templateData = template.read()
        print(e)
    except Exception as e:
        templateData = template
        print(e)
    finally:
        cf.validate_template(TemplateBody=template_data)
        return templateData
    
def myfunc1():
    pass


# MAIN FUNCTION for deploying cloud formation stacks
def deploy_stack(stack_name, template, filtered_params, region):
    cf_client = create_client('cloudformation', region)
    template_data = _parse_template(template, cf_client)

    capability = ['CAPABILITY_NAMED_IAM']

    params = {
        'StackName': stack_name,
        'TemplateBody': template_data,
        'Parameters': filtered_params,
        'Capabilities': capability
    }

    print('\tFetched Environment Variables\n\t______________________________')
    for par in filtered_params:
        print('\t{} :\t {}'.format(par['ParameterKey'], par['ParameterValue']))
    print('\n')

    try:
        if _stack_exists(stack_name, cf_client):
            print('Updating {}'.format(stack_name))
            stack_result = cf_client.update_stack(**params)
            waiter = cf_client.get_waiter('stack_update_complete')
        else:
            print('Creating {}'.format(stack_name))
            stack_result = cf_client.create_stack(**params)
            waiter = cf_client.get_waiter('stack_create_complete')
        print("...waiting for stack to be ready...")
        waiter.wait(StackName=stack_name)
    except boto3.exceptions.ClientError as ex:
        error_message = ex.response['Error']['Message']
        if error_message == 'No updates are to be performed.':
            print("~~~~~~~~~~~~~No changes~~~~~~~~~~~~~")
        else:
            raise ex

    stack_id = stack_result['StackId']
    print('Stack Is Deployed!'.center(80, '-'))
    result = "STACK ID : {}".format(stack_id)
    print(result)







def filter_parameters(params, config):
    filtered_params = []
    for item in params:
        filtered_params.append({"ParameterKey": item, "ParameterValue": config[item]})
    return filtered_params


def create_client(service, region):
    return boto3.client(service, region_name=region)

def create_client(service, region):
    return boto3.client(service, region_name=region)
    print(region)


def main():
    environment = input("Enter env (dev, qa) :")
    config = config_profile[environment]
    region = config.Region
    ops_bucket_name = config.OpsBucketName

    s3_instance_count = config.S3DataBucketConfig.InstanceCount
    eS_instance_count = config.ElasticSearchConfig.InstanceCount

    if s3_instance_count == 1:
        create_s3databucket(config, environment, ops_bucket_name, region)
    elif s3_instance_count > 1:
        print("Creating more than one instance is not currently supported")
    else:
        print("Skipping s3 bucket creation.")

    if eS_instance_count == 1:
        Elasticsearch(config, environment, ops_bucket_name, region)
    elif eS_instance_count > 1:
        print("Creating more than one instance is not currently supported")
    else:
        print("Skipping elastic search instance creation.")

  user name: xyx
        password : #$%^&*!@#!!@

if __name__ == '__main__':
    main()
