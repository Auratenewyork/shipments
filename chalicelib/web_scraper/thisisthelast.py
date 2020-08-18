import pickle
import requests
import boto3
from . import BUCKET


s3 = boto3.client('s3', region_name='us-east-2')


def run():
    page = 1
    result = []

    while True:
        url = f'https://thisisthelast.com/collections/shop?page={page}&view=product-data'
        response = requests.get(url)
        data = response.json()
        products = data['products']
        if not products:
            break
        for item in products:
            res = dict(
                id=item['id'],
                name=item['title'],
                price=item['price']/100,
                # images=item['images'],
                image=item['image']
            )
            result.append(res)
        page += 1
        if page > 50:
            raise Exception("Something went wrong!")
    s3.put_object(Body=pickle.dumps(result), Bucket=BUCKET, Key='thisisthelast')


if __name__ == "__main__":
    run()