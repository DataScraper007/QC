import json
import os

import QC.db_config as db
import pymysql
import scrapy
from scrapy.cmdline import execute
from QC.items import QcItem


class FkgSpider(scrapy.Spider):
    name = "fkg"
    is_debug = False
    debug_pid = "ARDFA6WQ8GP9KSV8"

    def __init__(self, pincode, **kwargs):
        super().__init__(**kwargs)
        self.pincode = pincode
        self.pids = json.loads(open('D:/myProjects/2024/july/QC_SCRAPER/QC/QC/cookies/pid_picodes.json', 'r').read())[str(pincode)]
        self.cookies = json.loads(open('D:/myProjects/2024/july/QC_SCRAPER/QC/QC/cookies/flipkart_cookies.json', 'r').read())[str(pincode)]
        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password)
        self.cursor = self.con.cursor()

        self.page_save_pdp = f'D:/pankaj_page_save/{db.delivery_date}/fkg/HTMLS/'


    def start_requests(self):

        for pid in self.pids:
            if self.is_debug:
                pid = self.debug_pid

            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
            }
            meta = {
                'fk_pid' : pid
            }
            yield scrapy.Request(
                url=f'https://www.flipkart.com/p/p/i?pid={pid}&marketplace=GROCERY',
                headers=headers,
                cookies=self.cookies,
                meta=meta
            )
            if self.is_debug:
                break

    def parse(self, response, **kwargs):

        if not os.path.exists(self.page_save_pdp):
            os.makedirs(self.page_save_pdp)
        # hash_id = hashlib.sha256(page_name.encode()).hexdigest()
        open(self.page_save_pdp + response.meta['fk_pid'] + self.pincode + ".html", "w", encoding="utf-8").write(response.text)

        item = QcItem()

        item["platform_name"] = "fkg"
        item['pincode'] = self.pincode
        item['fk_id'] = response.meta['fk_pid']
        item['url'] = response.url
        try:
            raw_data = response.xpath("//script[contains(text(),'__INITIAL_STATE__')]").get()
            raw_data = raw_data.split('__INITIAL_STATE__ = ')[-1].split(';</script>')[0]
            data = json.loads(raw_data)

            page_context_json = data['pageDataV4']['page']['pageData']['pageContext']

            try:
                if page_context_json['pricing']:
                    try:
                        item['price'] = page_context_json['pricing']['finalPrice']['decimalValue']
                    except:
                        item['price'] = ''
                    try:
                        item['discount'] = page_context_json['pricing']['totalDiscount']
                    except:
                        item['discount'] = ''
                    try:
                        item['mrp'] =  page_context_json['pricing']['mrp']
                    except:
                        item['mrp'] = ''
            except Exception as e:
                print(e)

            try:
                title = page_context_json['titles']['title']
            except:
                title = ''
            try:
                subtitle = page_context_json['titles']['subtitle']
            except:
                subtitle = ''

            item['name'] = f"{title} {subtitle}"

            try:
                productstatus = data['pageDataV4']['page']['pageData']['pageContext']['trackingDataV2']['productStatus']
            except:
                productstatus = ""

            if "Currently out of stock for" in response.text:
                availability =  False
            elif "Coming Soon" in response.text:
                availability =  False
            elif productstatus == "Out of Stock":
                availability =  False
            elif productstatus == "current":
                availability =  True
            else:
                availability =  False

            item['availability'] = availability

            yield item

        except Exception as e:
            print(f"ERROR:  {item['fk_id']}", e)
            yield item



if __name__ == '__main__':
    execute(f"scrapy crawl fkg -a pincode=110020".split())