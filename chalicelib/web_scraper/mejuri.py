import pickle
import requests
import boto3
from . import BUCKET

s3 = boto3.client('s3', region_name='us-east-2')


def run():
    url = 'https://mejuri.com/api/v1/taxon/collections-by-categories/USD/type'
    response = requests.get(url)
    data = response.json()
    result = []
    for category in data:
        for item in category['products']:
            res = dict(
                id=item['id'],
                name=item['name'],
                material=item['material_name'],
                price=item['price']['amount'],
                price_currency=item['price']['currency'],
                images=[image['attachment_url_original'] for image in item['variant_images']],
                image=item['variant_images'][0]['attachment_url_original']
            )
            result.append(res)
    # response = s3.get_object(Bucket=BUCKET, Key=f'mejuri')
    # previous_data = pickle.loads(response['Body'].read())
    s3.put_object(Body=pickle.dumps(result), Bucket=BUCKET, Key=f'mejuri')



if __name__ == "__main__":
    run()