import json

from curl_cffi import requests
session = requests.Session()

headers = {
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    # 'cookie': 'AKA_A2=A; _ALGOLIA=anonymous-cf7f4a3a-3e80-40da-9c8c-4777e8e65240; _fbp=fb.1.1723695983869.246013275; _gid=GA1.2.725178315.1723695984; WZRK_G=170161b68f4945b4a0808a30740b0e33; nms_mgo_city=Delhi; nms_mgo_state_code=DL; nms_mgo_pincode=110059; _ga=GA1.2.1932292761.1723695984; _gat_UA-163452169-1=1; WZRK_S_88R-W4Z-495Z=%7B%22p%22%3A4%2C%22s%22%3A1723695982%2C%22t%22%3A1723696210%7D; _ga_XHR9Q2M3VV=GS1.1.1723695984.1.1.1723696213.37.0.0; RT="z=1&dm=www.jiomart.com&si=89d6dff4-494c-4acb-82ce-ee310e285215&ss=lzus2zdm&sl=3&tt=566&obo=1&rl=1&nu=5ewbd7nf&cl=4z5m"',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'referer': 'https://www.jiomart.com/',
    'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest',
}

pincodes = [
    110020, 110059, 141001, 143413, 226003, 231312, 301001, 302017,
    342001, 380015, 388355, 400012, 410203, 411014, 431001, 440013,
    452001, 474001, 482002, 500008, 517501, 518002, 520007, 522646,
    524002, 530016, 560030, 560037, 575001, 580003, 590001, 600031,
    631551, 638056, 641028, 695013, 711107, 713212, 721102, 722107,
    751001, 752023, 781011, 797112, 800001, 812001, 824302, 828117,
    834002, 842001
]

cookies = {}

for pincode in pincodes:
    res = session.get(f"https://www.jiomart.com/mst/rest/v1/5/pin/{pincode}")
    json_data = res.json()

    pin = json_data['result']['pin']
    city = json_data['result']['city']
    state_code = json_data['result']['state_code']

    cookies[pin] = {
        "nms_mgo_city": city,
        "nms_mgo_pincode": pin,
        "nms_mgo_state_code": state_code,
    }

with open(r'C:\Users\Admin\PycharmProjects\QC\QC\cookies\jio_mart_cookies.json','w') as file:
    file.write(json.dumps(cookies))