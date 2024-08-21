import os
import scrapy
import pymysql
from scrapy.cmdline import execute

from QC.items import QcItem_amz, QcItem
import QC.db_config as db

class AmzLocalSpider(scrapy.Spider):
    name = "amz_local"

    def __init__(self, start,end, **kwargs):
        super().__init__(**kwargs)
        self.page_save_pdp = f'D:/pankaj_page_save/{db.delivery_date}/amz/HTMLS/'
        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password, database=db.db_name)
        self.cursor = self.con.cursor()
        self.start = start
        self.end = end

    def start_requests(self):
        query = f"SELECT * FROM `page_save_{db.delivery_date}` WHERE status = 'pending' and id between {self.start} and {self.end}"

        self.cursor.execute(query)
        results = self.cursor.fetchall()

        for result in results:
            page_save_id = result[6]  # Assuming page_save_id is at index 6
            file_path = os.path.join(self.page_save_pdp, page_save_id)
            meta = {
                'id' : result[0],
                'comp': result[1],
                'fk_id': result[2],
                'pincode': result[3],
                'url': result[4],
                'page_save_path': result[5],
                'page_save_id': page_save_id
            }
            filename =  result[5]  +page_save_id
            if os.path.exists(filename):
                yield scrapy.Request(
                    # url=data[1],
                    url=f'file:///{filename}',
                    meta=meta)

    def parse(self, response):

        # item = QcItem_amz()

        item = QcItem()
        item["comp"] = response.meta['comp']
        item['fk_id'] = response.meta['fk_id']
        item['pincode'] = response.meta['pincode']
        item['url'] = response.meta['url']
        # item['page_save_path'] = response.meta['page_save_path']
        # item['page_save_id'] = response.meta['page_save_id']

        print(response.meta['id'])

        store_availability_status = response.xpath('//span[contains(text(), "This store is not available")]/text()').get()

        if store_availability_status is not None:
            item['name'] = ''
            item['mrp'] = ''
            item['price'] = ''
            item['discount'] = ''
            item['availability'] = ''
        else:
            product_name = response.xpath("//meta[@name='title']/@content").get()
            if product_name is None:
                product_name = response.xpath("//*[@id='title']/text()").get()
            try:
                product_name = product_name.split(" : Amazon.in: ")[0] if product_name else ''
                if "Amazon.com:" in product_name:
                    product_name = product_name.strip("Amazon.com:")
            except:
                product_name = ''
            item['name'] = product_name

            mrp = response.xpath(
                "//span[contains(text(),'MRP:')]/following-sibling::span/span/text() | //td[contains(text(),'M.R.P.:')]/following-sibling::td/span/span/text() | //span[contains(text(),'M.R.P.:')]//span/text()").get()
            item['mrp'] = mrp

            try:
                price = response.xpath(
                    "//span[contains(@class,'buyingPrice')]/span/text() | //td[contains(text(),'Deal') or contains(text(),'Price')]/following-sibling::td/span/span/text() | //span[contains(@class,'priceToPay')]/span/text()").get(
                    '').strip()
            except:
                price = ''
            if price == '':
                price = response.xpath("//span[contains(@class,'priceToPay')]//span[@class='a-price-whole']//text()").get()
                # price = "".join(price).strip()

                if price == None:
                    price = response.xpath('//span[@id="priceblock_ourprice"]/span/text()').get('')
                    # price = "".join(price).strip()

            item['price'] = price

            raw_discount = response.xpath("//div[contains(@class,'discount-sticker')]/p//text()").getall()
            discount = None

            if raw_discount:
                for i in raw_discount:
                    if '%' in i:
                        discount = i.strip()
                        break
                if not discount:
                    discount = response.xpath("//span[contains(@class,'savingsPercentage')]//text()").get()

            if not discount:
                raw_discount = response.xpath(
                    "//td[contains(@class,'a-color-price')]//span[contains(@class,'a-color-price') ]/text()").getall()
                if not raw_discount:
                    raw_discount = response.xpath("//*[contains(@class,'savingsPercentage')]/text()").getall()
                for dis in raw_discount:
                    if '%' in dis:
                        discount = dis.strip().replace('(', ''). replace(')', '')
                        break
            item['discount'] = discount

            raw_stock = response.xpath("//div[contains(@id,'availability')]/span/text()").get()
            try:
                if raw_stock.strip() != '':
                    if 'in stock' or 'स्टॉक में है' in raw_stock.lower().strip() or raw_stock.lower().strip() == 'in stock' or 'left in stock' in raw_stock.lower().strip():
                        item['availability'] = True
                    else:
                        item['availability'] = False
            except:
                item['availability'] = False

            if raw_stock is None or raw_stock.strip() == '':
                raw_stock = response.xpath("//div[contains(@id,'buybox')]//*[contains(text(),'Buy Now')]/text()").get()
                if raw_stock:
                    item['availability'] = True
                else:
                    item['availability'] = False

        yield item

        update_query = f"""
                        UPDATE `page_save_{db.delivery_date}` SET status = 'Done' WHERE id = %s
                    """
        self.cursor.execute(update_query, (response.meta["id"],))

if __name__ == '__main__':
    execute(f"scrapy crawl amz_local".split())