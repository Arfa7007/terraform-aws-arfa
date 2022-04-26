import json
import boto3
client = boto3.client('dynamodb')
def lambda_handler(event, context):
    data = client.get_item(
        TableName = 'kinetic_block_installation_info',
        Key = {
            'reference_id' :
            {
                'S': event['reference_id']
            },
           
            'block_name' :
            {
                'S': event['block_name']
            }
        }
    
)

    response = {
        'reference_id':data['Item']['reference_id'],
        'block_name' : data['Item']['block_name'],
        'status' : data['Item']['status']
}
    return {
    'statusCode': 200,
    'body': json.dumps(response)
}