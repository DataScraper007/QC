import json
import os
import random

import QC.db_config as db
import scrapy
import pymysql
from scrapy.cmdline import execute
from QC.items import QcItem


class JmtSpider(scrapy.Spider):
    name = "jmt"

    def __init__(self, pincode, **kwargs):
        super().__init__(**kwargs)
        self.pincode = pincode
        # self.pids = json.loads(open('D:/myProjects/2024/july/QC_SCRAPER/QC/QC/cookies/pid_picodes.json', 'r').read())[
        #     str(pincode)]
        self.cookies = \
            json.loads(open(r'C:\Users\Admin\PycharmProjects\QC\QC\cookies\jio_mart_cookies.json', 'r').read())[
                str(pincode)]
        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password, database=db.db_name)
        self.cursor = self.con.cursor()

        self.page_save_pdp = fr'C:\Users\Admin\PycharmProjects\page_save{db.delivery_date}\jmt\HTMLS'

    def start_requests(self):
        self.cursor.execute(
            f"select fkg_pid, jmt_url from input_jmt where jmt_url != 'NA' and fkg_pid in (select pid from pid_pincode where pincode='{self.pincode}')")
        # f"select fkg_pid, jmt_url from input where jmt_url != 'NA' and fkg_pid in (select pid from pid_pincode where pincode=834002) and FKG_PID = 'AFSF7P7BPAZYF3KE'")
        results = self.cursor.fetchall()

        for result in results:
            fkg_pid = result[0]
            jmt_url = result[1]
            # jmt_url = 'https://www.jiomart.com/p/groceries/lizol-5-litre-lavender-disinfectant-surface-floor-cleaner-liquid-suitable-for-all-floor-cleaner-mops-kills-99-9-germs-india-s-1-floor-cleaner/607030534'
            meta = {
                'fkg_pid': fkg_pid,
                'jmt_url': jmt_url,
            }

            # cookies = {
            #     'nms_mgo_pincode': str(self.pincode),
            # }

            headers = {
                'accept': 'application/json, text/javascript, */*; q=0.01',
                'accept-language': 'en-US,en;q=0.9',
                'pin': str(self.pincode),
                # 'referer': 'https://www.jiomart.com/p/groceries/fortune-kachi-ghani-mustard-oil-5-l-jar/490012710',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 OPR/111.0.0.0 (Edition cdf)',
                'x-requested-with': 'XMLHttpRequest',
            }
            pid = jmt_url.split('/')[-1]
            # pid = '606421216'
            yield scrapy.Request(
                url=f'https://www.jiomart.com/catalog/productdetails/get/{pid}',
                cookies=self.cookies,
                headers=headers,
                meta=meta
            )
            # break

    def parse(self, response, **kwargs):
        try:
            if not os.path.exists(self.page_save_pdp):
                os.makedirs(self.page_save_pdp)
            # hash_id = hashlib.sha256(page_name.encode()).hexdigest()
            open(self.page_save_pdp + response.meta['fkg_pid'] + self.pincode + ".html", "w", encoding="utf-8").write(
                response.text)

            data = json.loads(response.text)
            item = QcItem()
            item["comp"] = "JioMart"
            meta = response.meta
            item['pincode'] = self.pincode
            item['fk_id'] = meta['fkg_pid']
            item['url'] = meta['jmt_url']

            if data.get('data'):
                try:
                    item['name'] = data['data']['gtm_details']['name']
                except:
                    item['name'] = ''
                try:
                    item['price'] = data['data']['selling_price']
                except:
                    item['price'] = ''
                try:
                    item['mrp'] = data['data']['mrp']
                except:
                    item['mrp'] = ''
                try:
                    item['discount'] = data['data']['discount_pct']
                except:
                    item['discount'] = ''
                try:
                    if data['data']['availability_status'] == 'A':
                        item['availability'] = True
                    else:
                        item['availability'] = False
                except:
                    item['availability'] = False
                yield item
            else:
                item['availability'] = False
                yield item
        except Exception as e:
            query = "update input set JMT_STATUS = 'ERROR' where FKG_PID=%s;"
            self.cursor.execute(query, response.meta['fkg_pid'])
            self.con.commit()
            print("ERROR: ", e)


if __name__ == '__main__':
    execute(f"scrapy crawl jmt -a pincode=141001".split())
