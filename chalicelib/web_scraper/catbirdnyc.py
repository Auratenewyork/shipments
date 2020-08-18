import pickle
import requests
import boto3
from . import BUCKET
from lxml import html


s3 = boto3.client('s3', region_name='us-east-2')


def run():
    page = 1
    result = []

    while True:
        url = f'https://www.catbirdnyc.com/jewelry-catbird.html?p={page}'
        response = requests.get(url)
        tree = html.fromstring(response.content)
        products_grid = tree.xpath('//div[@class="products wrapper grid products-grid"]')
        if products_grid:
            products_grid = products_grid[0]
        else:
            break
        products = products_grid.xpath('.//div[@class="product-item-info"]')

        def get_value(element, path):
            val = item.xpath(path)
            if val:
                return val[0].strip()

        for item in products:
            p = get_value(item, './/span[@class="price"]/text()')
            res = dict(
                price=p[1:],
                designer=get_value(item, './/div[@class="product-designer"]/text()'),
                image=get_value(item, './/img[@class="product-image-photo"]/@src'),
                link=get_value(item, './/a[@class="product-item-link"]/@href'),
                name=get_value(item, './/a[@class="product-item-link"]/text()'),
            )
            result.append(res)
        page += 1
        if page > 50:
            raise Exception("Something went wrong!")
    s3.put_object(Body=pickle.dumps(result), Bucket=BUCKET, Key='catbirdnyc')


if __name__ == "__main__":
    run()