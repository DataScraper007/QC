import json
import os
import random
import time

import scrapy
import pymysql
from scrapy.cmdline import execute

import QC.db_config as db
from QC.items import QcItem


class SwiggyInstamartSpider(scrapy.Spider):
    name = "swiggy_instamart"
    custom_settings = {
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_impersonate.ImpersonateDownloadHandler",
            "https": "scrapy_impersonate.ImpersonateDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "CONCURRENT_REQUESTS": 1
    }

    def __init__(self, pincode):
        super().__init__()
        self.pincode = pincode
        self.cookies = \
            json.loads(open(r'C:\Users\Admin\PycharmProjects\QC\QC\cookies\swiggy_instamart_cookies.json', 'r').read())[
                str(pincode)]
        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password, database=db.db_name)
        self.cursor = self.con.cursor()

        self.page_save_pdp = fr'C:\Users\Admin\PycharmProjects\page_save\{db.delivery_date}\swi\HTMLS'

    def start_requests(self):
        self.cursor.execute(
            f"select fkg_pid, swi_url from input_swi where swi_url != 'NA' and fkg_pid in (select pid from pid_pincode where pincode='{self.pincode}') and page_status='ERROR'")
        results = self.cursor.fetchall()

        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'max-age=0',
            # 'cookie': 'deviceId=s%3Ad93a20b4-215e-454f-8ace-14b22988d466.dkRu5C3JqC%2BS%2FQiDSfD2Bh07H5buUwtECtSlxd1Y6Ow; tid=s%3Adb3d4e5f-c72a-4678-81c6-38ecce8e354c.HXVUMjB6vqFEGrwVnvbeRPcZnPmVno8yFRQFNRxDksk; sid=s%3Afhr15cee-0c28-4a16-aa90-8ffd75358121.2MssMQZAzPXZxji1eoFegQolhbwLQT7MvMpsiFoM5LQ; versionCode=1200; platform=web; subplatform=dweb; statusBarHeight=0; bottomOffset=0; genieTrackOn=false; ally-on=false; isNative=false; strId=; openIMHP=false; webBottomBarHeight=0; _gcl_au=1.1.187506760.1723455013; _fbp=fb.1.1723455013882.98531931156674354; addressId=s%3A.4Wx2Am9WLolnmzVcU32g6YaFDw0QbIBFRj2nkO7P25s; LocSrc=s%3AswgyUL.Dzm1rLPIhJmB3Tl2Xs6141hVZS0ofGP7LGmLXgQOA7Y; __SW=B1NuPQFq4EbWIVDYWSgZbeexnQVoxp8N; _guest_tid=eeb6e0f7-bcfa-48bb-bff5-3d98b1a16705; _device_id=928b8642-447c-0911-82c2-9f9434bd5b17; _sid=fhrd3dd3-262a-4535-850d-dfe8dc04f560; fontsLoaded=1; _gid=GA1.2.1737479161.1723455149; _ga=GA1.1.427057644.1723455014; dadl=true; lat=s%3A12.9765944.fBxObcmWnJwnT%2FoZ%2BGoqGdD3T1VPilgFPtdd99CDI1A; lng=s%3A77.5992708.aSPVQXrDnNTLtpT8i9Tn23Wzvw5uz%2FX6qGw55glvbCM; address=s%3ABengaluru%2C%20Karnataka%20560001%2C%20India.tGCQDqKHP3Lz%2Bnr0zyqKYCpFVvAP43%2FBUm8UrtYdS1U; userLocation=%7B%22address%22%3A%22Bengaluru%2C%20Karnataka%20560001%2C%20India%22%2C%22lat%22%3A12.9765944%2C%22lng%22%3A77.5992708%2C%22id%22%3A%22%22%2C%22annotation%22%3A%22%22%2C%22name%22%3A%22%22%7D; _ga_34JYJ0BCRN=GS1.1.1723455149.1.1.1723455178.0.0.0; imOrderAttribution={%22entryId%22:null%2C%22entryName%22:null%2C%22entryContext%22:null%2C%22hpos%22:null%2C%22vpos%22:null%2C%22utm_source%22:null%2C%22utm_medium%22:null%2C%22utm_campaign%22:null}; _ga_8N8XRG907L=GS1.1.1723455013.1.1.1723455395.0.0.0',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        }

        for result in results:
            fkg_pid = result[0]
            swi_url = result[1]

            meta = {
                'fkg_pid': fkg_pid,
                'swi_url': swi_url,
            }
            browsers = [
                "chrome110",
                "edge99",
                "safari15_5"
            ]
            meta["impersonate"] = random.choice(browsers)

            yield scrapy.Request(
                url=swi_url,
                headers=headers,
                cookies=self.cookies,
                callback=self.parse,
                dont_filter=True,
                meta=meta
            )

    def parse(self, response, **kwargs):
        try:
            if not os.path.exists(self.page_save_pdp):
                os.makedirs(self.page_save_pdp)
            # hash_id = hashlib.sha256(page_name.encode()).hexdigest()
            open(self.page_save_pdp + response.meta['fkg_pid'] + self.pincode + ".html", "w", encoding="utf-8").write(
                response.text)

            item = QcItem()
            meta = response.meta

            json_data = response.xpath('//script[contains(text(),"window.___INITIAL_STATE___")]/text()').get()

            product_data = self.clean_json(json_data)['instamart']['cachedProductItemData']

            item['comp'] = 'Swiggy Instamart'
            item['url'] = response.url
            item['fk_id'] = meta['fkg_pid']
            item['pincode'] = self.pincode
            if product_data:
                product_data = self.clean_json(json_data)['instamart']['cachedProductItemData']['lastItemState']
                variations = product_data['variations']
                data = variations[0]
                item['name'] = data['display_name'] + ' ' + data['sku_quantity_with_combo']
                item['price'] = data['price']['offer_price']
                item['mrp'] = data['price']['mrp']
                item['discount'] = data['price']['offer_applied']['product_description']
                if data['inventory']['in_stock'] == True:
                    item['availability'] = True
                else:
                    item['availability'] = False
                self.cursor.execute("update input_swi set page_status='SUCCESS' where fkg_pid=%s", meta['fkg_pid'])
                self.con.commit()
                yield item

            else:
                error_msg = response.xpath(
                    '//div[contains(text(),"Our best minds are on it. You may retry or check back soon")]/text()').get()

                if error_msg:
                    self.cursor.execute("update input_swi set page_status='ERROR' where fkg_pid=%s", meta['fkg_pid'])
                    self.con.commit()
                else:
                    item['name'] = ""
                    item['price'] = ""
                    item['mrp'] = ""
                    item['discount'] = ""
                    item['availability'] = False
                    self.cursor.execute("update input_swi set page_status='SUCCESS' where fkg_pid=%s", meta['fkg_pid'])
                    self.con.commit()
                    yield item

        except Exception as e:
            print("ERROR: ", e)

    def clean_json(self, raw_data):
        json_data = raw_data.replace('window.___INITIAL_STATE___ = ', '')
        start_idx = json_data.find(f'var App = {{')
        result = json_data[:start_idx].strip()

        return json.loads(result.strip(';'))


if __name__ == '__main__':
    execute(f"scrapy crawl swiggy_instamart -a pincode=110020".split())
