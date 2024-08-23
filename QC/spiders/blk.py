import base64
import json
import os
import random

import QC.db_config as db
import scrapy
import pymysql
from scrapy.cmdline import execute
from QC.items import QcItem


class BlkSpider(scrapy.Spider):
    name = "blk"
    custom_settings = {
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_impersonate.ImpersonateDownloadHandler",
            "https": "scrapy_impersonate.ImpersonateDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    }

    def __init__(self, pincode, **kwargs):
        super().__init__(**kwargs)
        self.pincode = pincode
        # self.pids = json.loads(open('D:/myProjects/2024/july/QC_SCRAPER/QC/QC/cookies/pid_picodes.json', 'r').read())[
        #     str(pincode)]
        self.cookies = \
            json.loads(open(r'C:\Users\Admin\PycharmProjects\QC\QC\cookies\blinkit_cookies.json', 'r').read())[
                str(pincode)]
        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password, database=db.db_name)
        self.cursor = self.con.cursor()

        # self.page_save_pdp = f'D:/pankaj_page_save/{db.delivery_date}/blk/HTMLS/'

    def start_requests(self):
        self.cursor.execute(
            f"select fkg_pid, blk_url, blk_var from input where blk_url != 'NA' and fkg_pid in (select pid from pid_pincode where pincode='{self.pincode}')")
        results = self.cursor.fetchall()

        for result in results:
            fkg_pid = result[0]
            blk_url = result[1]
            blk_var = result[2]

            meta = {
                'fkg_pid': fkg_pid,
                'blk_url': blk_url,
                'blk_var': blk_var,
            }
            browsers = [
                "chrome110",
                "edge99",
                "safari15_5"
            ]
            meta["impersonate"] = random.choice(browsers)

            cookies = {
                'gr_1_lat': str(self.cookies['latitude']),
                'gr_1_lon': str(self.cookies['longitude']),
            }

            headers = {
                'accept': '*/*',
                'accept-language': 'en-US,en;q=0.9',
                'app_client': 'consumer_web',
                'app_version': '52434332',
                'auth_key': 'c761ec3633c22afad934fb17a66385c1c06c5472b4898b866b7306186d0bb477',
                'content-type': 'application/json',
                'device_id': '553ec948-521b-4193-8562-3f3db34459c9',
                'lat': str(self.cookies['latitude']),
                'lon': str(self.cookies['longitude']),
                'priority': 'u=1, i',
                'referer': 'https://blinkit.com/prn/lays-indias-magic-masala-potato-chips-40-g/prid/240092',
                'rn_bundle_version': '1009003012',
                'session_uuid': 'a095e315-da48-4d9b-b6f0-23da8fec0ffa',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
                'web_app_version': '1008010016',
            }
            pid = blk_url.split('/')[-1]

            yield scrapy.Request(
                url=f"https://blinkit.com/v6/product/{pid}?current_screen=pdp",
                headers=headers,
                cookies=cookies,
                meta=meta
            )
            break

    def parse(self, response, **kwargs):

        # if not os.path.exists(self.page_save_pdp):
        #     os.makedirs(self.page_save_pdp)
        # # hash_id = hashlib.sha256(page_name.encode()).hexdigest()
        # open(self.page_save_pdp + response.meta['fkg_pid'] + self.pincode + ".html", "w", encoding="utf-8").write(
        #     response.text)

        print()
        item = QcItem()
        meta = response.meta
        item['pincode'] = self.pincode
        item['comp'] = "BlinkIt"
        item['fk_id'] = meta['fkg_pid']
        item['url'] = meta['blk_url']
        blk_var = meta['blk_var']
        data = json.loads(response.text)

        for variant in data['data']['variants_info']:
            if (blk_var == variant['product_id']) or (str(variant['product_id']) in response.url):
                try:
                    item['name'] = variant['name']
                except:
                    item['name'] = ''
                try:
                    item['mrp'] = variant['mrp']
                except:
                    item['mrp'] = ''
                try:
                    item['price'] = variant['price']
                except:
                    item['price'] = ''
                try:
                    item['discount'] = variant['offer']
                except:
                    item['discount'] = ''
                try:
                    invtr = variant['inventory']
                except:
                    invtr = None
                if invtr:
                    item['availability'] = True
                else:
                    item['availability'] = False
                yield item


if __name__ == '__main__':
    execute(f"scrapy crawl blk -a pincode=110020 -s CONCURRENT_REQUESTS=1".split())
