import base64
import json
import os

import QC.db_config as db
import scrapy
import pymysql
from scrapy.cmdline import execute
from QC.items import QcItem


class BbSpider(scrapy.Spider):
    name = "bb"
    is_debug = False
    debug_pid = "ARDFA6WQ8GP9KSV8"

    def __init__(self, pincode, **kwargs):
        super().__init__(**kwargs)
        self.pincode = pincode
        self.pids = json.loads(open('D:/myProjects/2024/july/QC_SCRAPER/QC/QC/cookies/pid_picodes.json', 'r').read())[
            str(pincode)]
        self.cookies = \
        json.loads(open(r'C:\Users\Admin\PycharmProjects\QC\QC\cookies\bigbasket_cookies.json', 'r').read())[
            str(pincode)]
        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password, database=db.db_name)
        self.cursor = self.con.cursor()
        self.page_save_pdp = f'C:/Users/Admin/PycharmProjects/page_save/{db.delivery_date}/bb_2/HTMLS/'

    def start_requests(self):

        self.cursor.execute(
            f"select fkg_pid, bb_url, bb_var from input where bb_url != 'NA' and fkg_pid in (select pid from pid_pincode where pincode='{self.pincode}')")
        results = self.cursor.fetchall()

        for result in results:
            fkg_pid = result[0]
            bb_url = result[1]
            bb_var = result[2]

            bytes_data = f"{self.cookies['latitude']}|{self.cookies['longitude']}".encode('utf-8')
            base64_encoded = base64.b64encode(bytes_data).decode()

            cookies = {
                '_bb_lat_long': base64_encoded,
                '_bb_pin_code': str(self.pincode),
            }
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-US,en;q=0.9',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
            }
            meta = {
                'fkg_pid': fkg_pid,
                'bb_url': bb_url,
                'bb_var': bb_var,
            }

            yield scrapy.Request(
                url=bb_url,
                headers=headers,
                cookies=cookies,
                meta=meta,
            )
            break

    def parse(self, response, **kwargs):
        if not os.path.exists(self.page_save_pdp):
            os.makedirs(self.page_save_pdp)
        # hash_id = hashlib.sha256(page_name.encode()).hexdigest()
        open("1.html", "w", encoding="utf-8").write(
            response.text)

        raw_data = response.xpath("//script[@id='__NEXT_DATA__']//text()").get()
        # if raw_data is not None:
        data = json.loads(raw_data)
        meta = response.meta
        item = QcItem()
        item['comp'] = "BigBasket"
        item['pincode'] = self.pincode
        item['fk_id'] = meta['fkg_pid']
        item['url'] = meta['bb_url']
        for children in data['props']['pageProps']['productDetails']['children']:
            if str(meta['bb_var']) == str(children['id']):
                try:
                    brand = children['brand']['name']
                except:
                    brand = ''
                try:
                    desc = children['desc']
                except:
                    desc = ''
                try:
                    pack_desc = children['pack_desc']
                except:
                    pack_desc = ''
                try:
                    w = children['w']
                except:
                    w = ''
                item['name'] = f"{brand} {desc}, {w} {pack_desc}".strip()
                try:
                    item['price'] = children['pricing']['discount']['prim_price']['sp']
                except:
                    item['price'] = ''
                try:
                    item['mrp'] = children['pricing']['discount']['mrp']
                except:
                    item['mrp'] = ''
                try:
                    item['discount'] = children['pricing']['discount']['d_text']
                except:
                    item['discount'] = ''

                if children['availability']['avail_status'] == '001':
                    item['availability'] = True
                else:
                    item['availability'] = False

                yield item


if __name__ == '__main__':
    execute(f"scrapy crawl bb -a pincode=812001 -s CONCURRENT_REQUESTS=6".split())
