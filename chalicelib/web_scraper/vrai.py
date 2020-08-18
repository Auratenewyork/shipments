import pickle
import requests
import boto3
# from . import BUCKET
BUCKET = 'aurate-scraper'

from lxml import html

s3 = boto3.client('s3', region_name='us-east-2')


def run():
    page = 1
    result = []

    while True:
        url = f'https://vrai.com/collections/jewelry?page={page}'
        response = requests.get(url)
        tree = html.fromstring(response.content)
        products_grid = tree.xpath('//div[@class="grid grid-uniform flex-grid-collection"]')
        if products_grid:
            products_grid = products_grid[0]
        else:
            break
        products = products_grid.xpath('.//div[contains(@class,"grid__item")]')

        def get_value(element, path):
            val = item.xpath(path)
            if val:
                return val[0].strip()

        for item in products:
            p = item.xpath('.//p[@class="product-card__price"]//text()')[-1].strip()
            price = p.split('|')[-1].strip()
            try:
                price = int(''.join(c for c in price if c.isdigit()))
            except ValueError:
                pass
            res = dict(
                price=price,
                image=get_value(item, './/img[@class="product-card__image product-card__image--top"]/@src'),
                link=get_value(item, './/p[contains(@class, "product-title")]/a/@href'),
                name=get_value(item, './/p[contains(@class, "product-title")]/a/text()'),
                variant=get_value(item, './/span[@class="product-variant_title"]/text()')
            )
            result.append(res)
        page += 1
        if page > 50:
            raise Exception("Something went wrong!")
    s3.put_object(Body=pickle.dumps(result), Bucket=BUCKET, Key='vrai')


if __name__ == "__main__":
    run()