import json
import os
import random

import QC.db_config as db
import scrapy
import pymysql
from scrapy.cmdline import execute
from QC.items import QcItem


class DmtSpider(scrapy.Spider):
    name = "dmt"
    is_debug = False
    debug_pid = "MSCEXBUWKSPUGF6S"
    custom_settings = {
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_impersonate.ImpersonateDownloadHandler",
            "https": "scrapy_impersonate.ImpersonateDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "CONCURRENT_REQUESTS": 1,
    }

    def __init__(self, pincode, **kwargs):
        super().__init__(**kwargs)
        self.pincode = pincode
        # self.pids = json.loads(open('D:/myProjects/2024/july/QC_SCRAPER/QC/QC/cookies/pid_picodes.json', 'r').read())[
        #     str(pincode)]
        self.cookies = json.loads(open(r'C:\Users\Admin\PycharmProjects\QC\QC\cookies\dmart_cookies.json', 'r').read())[
            str(pincode)]
        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password, database=db.db_name)
        self.cursor = self.con.cursor()

        self.page_save_pdp = fr'C:\Users\Admin\PycharmProjects\page_save{db.delivery_date}\dmt\HTMLS'

    def start_requests(self):
        store_id = self.cookies
        if store_id:
            self.cursor.execute(
                f"select fkg_pid, dmt_url, dmt_var from input where dmt_url != 'NA' and fkg_pid in (select pid from pid_pincode where pincode='{self.pincode}')")
            results = self.cursor.fetchall()

            for result in results:
                fkg_pid = result[0]
                dmt_url = result[1]
                # dmt_url = 'https://www.dmart.in/product/colgate-maxfresh-spicy-fresh-toothpaste'
                dmt_var = result[2]
                meta = {
                    'fkg_pid': fkg_pid,
                    'dmt_url': dmt_url.split('#')[0] if '#' in dmt_url else dmt_url,
                    'dmt_var': dmt_var,
                }
                headers = {
                    # 'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept-language': 'en-US,en;q=0.9',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
                }

                slug = dmt_url.split('/product/')[-1].split('#')[0]

                url = meta['dmt_url']
                browsers = [
                    "chrome110",
                    "edge99",
                    "safari15_5"
                ]
                meta["impersonate"] = random.choice(browsers)
                yield scrapy.Request(
                    url=url,
                    headers=headers,
                    meta=meta
                )

    def parse(self, response, **kwargs):
        if not os.path.exists(self.page_save_pdp):
            os.makedirs(self.page_save_pdp)
        # hash_id = hashlib.sha256(page_name.encode()).hexdigest()
        open(self.page_save_pdp + response.meta['fkg_pid'] + self.pincode + ".html", "w", encoding="utf-8").write(
            response.text)

        item = QcItem()
        item['comp'] = "Dmart"
        raw_data = response.xpath("//script[@id='__NEXT_DATA__']//text()").get()
        data = json.loads(raw_data)
        meta = response.meta
        item['pincode'] = self.pincode
        item['fk_id'] = meta['fkg_pid']
        item['url'] = meta['dmt_url']
        not_var = True
        product_data = data['props']['pageProps']['pdpData']['dynamicPDP']['data']['productData']

        if product_data:
            skus = product_data['sKUs']
            try:
                for sku in skus:
                    try:
                        if str(meta['dmt_var']) == str(sku['skuUniqueID']):
                            not_var = False
                            try:
                                item['name'] = sku['name']
                            except:
                                item['name'] = ''
                            try:
                                item['mrp'] = sku['priceMRP']
                            except:
                                item['mrp'] = ''
                            try:
                                item['price'] = sku['priceSALE']
                            except:
                                item['price'] = ''
                            try:
                                item['discount'] = sku['savingPercentage']
                            except:
                                item['discount'] = ''
                            try:
                                if sku['invType'] == 'A':
                                    item['availability'] = True
                                else:
                                    item['availability'] = False
                            except:
                                pass
                            yield item
                    except:
                        yield item

                if not_var:
                    for sku in skus:
                        try:
                            item['name'] = sku['name']
                        except:
                            item['name'] = ''
                        try:
                            item['mrp'] = sku['priceMRP']
                        except:
                            item['mrp'] = ''
                        try:
                            item['price'] = sku['priceSALE']
                        except:
                            item['price'] = ''
                        try:
                            item['discount'] = sku['savingPercentage']
                        except:
                            item['discount'] = ''
                        try:
                            if sku['invType'] == 'A':
                                item['availability'] = True
                            else:
                                item['availability'] = False
                        except:
                            pass
                        yield item
                        break
            except:
                yield item


if __name__ == '__main__':
    execute(f"scrapy crawl dmt -a pincode=388355".split())
