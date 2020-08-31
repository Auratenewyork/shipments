import pickle
import requests
import boto3
# from . import BUCKET
BUCKET = 'aurate-scraper'


s3 = boto3.client('s3', region_name='us-east-2')


def run():
    page = 1
    result = []

    while True:
        url = f'https://www.stoneandstrand.com/collections/all?view=json&page={page}&sort_by=created-descending'
        response = requests.get(url)
        products = response.json()
        if not products:
            break
        for item in products:
            res = dict(
                id=item['id'],
                name=item['title'],
                price=item['price']/100,
                images=item['images'],
                image=item['images'][0]
            )
            result.append(res)
        page += 1
        if page > 50:
            raise Exception("Something went wrong!")
    s3.put_object(Body=pickle.dumps(result), Bucket=BUCKET, Key='stoneandstrand')


if __name__ == "__main__":
    run()