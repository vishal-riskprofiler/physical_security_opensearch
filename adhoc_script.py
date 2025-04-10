import os,json
from utils.dynamodb import DynamoDbWrapper
from boto3.dynamodb.conditions import Key, Attr

dynamodb = DynamoDbWrapper()
DynamoDBTableOrganizationTyposquattingDomains = os.environ("OrganizationTyposquattingDomains")


def handler(event, context):

    organizations = dynamodb.query(
        table_name=os.environ['DynamoDBTableOrganizations'],
        index_name=os.environ['DynamoDBIndexOrganizationLicenseIndex'],
        query=Key('license_type').eq('enterprise')
    )

    for org in organizations:
        my_domains = dynamodb.query(
            table_name=os.environ['DynamoDBTableOrganizationMyDomains'],
            query=Key('organization_id').eq(org['id'])
        )

        keys_to_delete = []
        for typosquat in my_domains:
            keys_to_delete.append({
                'organization_id': org['id'],
                'typosquatting_domain': typosquat['typosquatting_domain']
            })

        if keys_to_delete:
            try:
                dynamodb.batch_delete(
                    table_name=DynamoDBTableOrganizationTyposquattingDomains,
                    keys_to_delete=keys_to_delete
                )
            except Exception:
                return {
                    'statusCode': 400,
                    'body': json.dumps('"Failed to delete typosquaat_domains for organisation"')
                }

    return {
        'statusCode': 200,
        'body': json.dumps('Typosquatting domains cleanup completed.')
    }