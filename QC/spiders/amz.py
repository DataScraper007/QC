import base64
import json
import os

import QC.db_config as db
import scrapy
import pymysql
from scrapy.cmdline import execute
from QC.items import QcItem, QcItem_amz


class AmzSpider(scrapy.Spider):
    name = "amz1"

    def __init__(self, pincode, **kwargs):
        super().__init__(**kwargs)
        self.pincode = pincode
        self.pids = json.loads(open('D:/myProjects/2024/july/QC_SCRAPER/QC/QC/cookies/pid_picodes.json', 'r').read())[
            str(pincode)]
        self.cookies = \
            json.loads(open('D:/myProjects/2024/july/QC_SCRAPER/QC/QC/cookies/amazon_cookies.json', 'r').read())[
                str(pincode)]
        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password, database=db.db_name)
        self.cursor = self.con.cursor()
        self.page_save_pdp = f'D:/pankaj_page_save/{db.delivery_date}/amz/HTMLS/'

    def start_requests(self):
        self.cursor.execute(
            f"select fkg_pid, amz_url from input where amz_url != 'NA' and fkg_pid in (select pid from pid_pincode where pincode='{self.pincode}')")
        # f"select fkg_pid, amz_url from input where amz_url != 'NA' and fkg_pid in (select pid from pid_pincode where pincode='781011') and FKG_PID = 'CAFFZ448T8G9GVRS'")
        results = self.cursor.fetchall()

        for result in results:
            fkg_pid = result[0]
            amz_url = result[1]

            meta = {
                'fkg_pid': fkg_pid,
                'amz_url': amz_url,
                'proxy': "https://scraperapi:64a773e99ca0093e4f80e217a71f821b@proxy-server.scraperapi.com:8001"
            }
            headers = {
                'accept-language': 'en-US,en;q=0.9',
                'user_agent': 'Mozilla/5.0 (Linux; U; Android 4.3; en-us; SM-N900T Build/JSS15J) AppleWebKit/534.30 (KHTML, like Gecko) Version/16.0 Mobile Safari/534.30',
            }
            yield scrapy.Request(
                url=amz_url,
                headers=headers,
                cookies=self.cookies,
                meta=meta
            )
            # break

    def parse(self, response, **kwargs):

        item = QcItem()

        # item = QcItem_amz()

        item["comp"] = "AmazonFresh"
        meta = response.meta
        item['pincode'] = self.pincode
        item['fk_id'] = meta['fkg_pid']
        item['url'] = meta['amz_url']
        # item['page_save_path'] = self.page_save_pdp
        # item['page_save_id'] = meta['fkg_pid'] + self.pincode + ".html"
        store_availability_status = response.xpath(
            '//span[contains(text(), "This store is not available")]/text()').get()
        if not os.path.exists(self.page_save_pdp):
            os.makedirs(self.page_save_pdp)
        # hash_id = hashlib.sha256(page_name.encode()).hexdigest()
        open(self.page_save_pdp + meta['fkg_pid'] + self.pincode + ".html", "w", encoding="utf-8").write(response.text)

        if store_availability_status is not None:
            item['name'] = ''
            item['mrp'] = ''
            item['price'] = ''
            item['discount'] = ''
            item['availability'] = ''

            yield item

        else:
            product_name = response.xpath("//meta[@name='title']/@content").get()
            if product_name == None:
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
                price = response.xpath(
                    "//span[contains(@class,'priceToPay')]//span[@class='a-price-whole']//text()").get()
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
                        discount = dis.strip().replace('(', '').replace(')', '')
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

            if raw_stock == None or raw_stock.strip() == '':
                raw_stock = response.xpath("//div[contains(@id,'buybox')]//*[contains(text(),'Buy Now')]/text()").get()
                if raw_stock:
                    item['availability'] = True
                else:
                    item['availability'] = False

            yield item


if __name__ == '__main__':
    execute(f"scrapy crawl amz -a pincode=781011".split())
