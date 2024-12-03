from zenrows import ZenRowsClient
from bs4 import BeautifulSoup
import pandas as pd
import traceback
import random
import signal
import os


class VadenScraperZR:
    scraped_list = [
        'https://vaden.com.tr/tr/filtered-products?category_id=40&brand_id=&engine_id=&page=9999',
        'https://vaden.com.tr/tr/filtered-products?category_id=41&brand_id=&engine_id=&page=9999',
        'https://vaden.com.tr/tr/filtered-products?category_id=187&brand_id=&engine_id=&page=9999',
        'https://vaden.com.tr/tr/filtered-products?category_id=170&brand_id=&engine_id=&page=9999',
        'https://vaden.com.tr/tr/filtered-products?category_id=225&brand_id=&engine_id=&page=9999',
        'https://vaden.com.tr/tr/filtered-products?category_id=324&brand_id=&engine_id=&page=9999',
        'https://vaden.com.tr/tr/filtered-products?category_id=43&brand_id=&engine_id=&page=9999',
        'https://vaden.com.tr/tr/filtered-products?category_id=42&brand_id=&engine_id=&page=9999',
        'https://vaden.com.tr/tr/filtered-products?category_id=44&brand_id=&engine_id=&page=9999',
        'https://vaden.com.tr/tr/filtered-products?category_id=45&brand_id=&engine_id=&page=9999',
        'https://vaden.com.tr/tr/filtered-products?category_id=39&brand_id=&engine_id=&page=9999', # 736
    ]

    category_urls = [
        'https://vaden.com.tr/tr/filtered-products?category_id=583&brand_id=&engine_id=&page=9999', # 1808
        'https://vaden.com.tr/tr/filtered-products?category_id=1&brand_id=&engine_id=&page=9999', # 1568
        'https://vaden.com.tr/tr/filtered-products?category_id=753&brand_id=&engine_id=&page=9999' # 4944
    ]

    API_LIST = [
        ''
    ]

    def __init__(self):
        self.list_for_excel = []
        self.request_counter = 0
        self.request_limit = 990
        self.api_index = 0

        self.url = None
        self.starting_page = None
        self.ending_page = None
        self.filename = None
        self.get_user_input()

        self.client = None
        self.create_client(initial_client=True)

        signal.signal(signal.SIGINT, self.signal_handler)  
        

    def main(self):
        try:
            self.category_urls = self.category_urls[0:1]
            for category_url in self.category_urls:
                self.filename = str(category_url).split('filtered-products?')[1].split('&')[0]
                page_urls = self.create_page_urls(category_url=category_url)
                for page_url in page_urls:
                    product_urls = self.get_product_urls(page_url=page_url)

                    product_counter = 1
                    for product_url in product_urls:
                        self.scrape_product_info(product_url=product_url)
                        print(f'Page: {page_url}\n[{product_counter}/16] - Request: {self.request_counter} - List size: {len(self.list_for_excel)}')
                        product_counter += 1           
                self.convert_to_excel(filename=self.filename)
        except Exception as e:
            print(traceback.format_exc())
        finally:
            self.convert_to_excel(filename=self.filename)


    def get_product_urls(self, page_url:str):
        response = self.client.get(url=page_url)
        soup = BeautifulSoup(response.content, 'lxml')
        product_divs = soup.find('div', attrs={'class':'productList'}).find('div', attrs={'class':'row newrow'}).find_all('div')

        # check request counter and change the api key if needed
        self.request_counter += 1
        self.create_client(initial_client=False)

        product_urls = []
        for div in product_divs:
            try:
                product_urls.append(div.find('a').get('href'))
            except:
                pass
        return product_urls
    

    def scrape_product_info(self, product_url:str):
        response = self.client.get(url=product_url)
        soup = BeautifulSoup(response.content, 'lxml')
        self.parse_product_info(soup=soup, category='main product', url=product_url)

        # check request counter and change the api key if needed
        self.request_counter += 1
        self.create_client(initial_client=False)

        sub_navs = soup.find('ul', attrs={'class':'customTabNavs'}).find_all('li')
        sub_navs = [nav.find('a').get('href') for nav in sub_navs]

        if '#repairKits' in sub_navs:
            repair_kit_urls = soup.find('div', attrs={'class':'productList'}).find('div').find_all('div', attrs={'class':'col'})
            repair_kit_urls = [kit.find('a').get('href') for kit in repair_kit_urls]

            for repair_kit_url in repair_kit_urls:
                response = self.client.get(url=repair_kit_url)
                soup = BeautifulSoup(response.content, 'lxml')
                self.parse_product_info(soup=soup, category='repair kit', url=repair_kit_url)

                # check request counter and change the api key if needed
                self.request_counter += 1
                self.create_client(initial_client=False)


    def parse_product_info(self, soup:BeautifulSoup, category:str, url:str):
        product_no = soup.find('div', attrs={'class':'code'}).find('a').find('h2').get_text().rstrip().lstrip()
        title = soup.find('div', attrs={'class':'code'}).find('h3').get_text().lstrip('\n').rstrip('\n').strip()
        info = {}

        try:
            oem_numbers = []
            brand_names = soup.find('div', attrs={'class':'productContent'}).find_all('div', attrs={'class':'card-body px-3'})
            brand_names = [name.find('a').get_text().lstrip('\n').rstrip('\n').strip() for name in brand_names]

            oem_tags = soup.find('div', attrs={'class':'productContent'}).find_all('ul', attrs={'class':'brandOemList'})
            for oem_tag in oem_tags:
                oem_list = oem_tag.find_all('li', attrs={'class':'item lh-lg'})
                oem_list = [oem.find('a').get_text().lstrip('\n').rstrip('\n').strip() for oem in oem_list]
                oem_numbers.append(oem_list)

            mapped_dict = dict(zip(brand_names, oem_numbers))
            for brand, oem_list in mapped_dict.items():
                for oem in oem_list:
                    info = {
                        'Vaden No': product_no,
                        'Ürün Adı': title,
                        'Oem Adı': brand,
                        'Oem No': oem,
                        'Kategori': category,
                        'Url': url
                    }
                    self.list_for_excel.append(info)

            if info == {}:
                info = {
                    'Vaden No': product_no,
                    'Ürün Adı': title,
                    'Oem Adı': '',
                    'Oem No': '',
                    'Kategori': category,
                    'Url': url
                }
                self.list_for_excel.append(info)

            if len(self.list_for_excel) >= 50:
                self.convert_to_excel(filename=self.filename)

        except Exception as e:
            pass


    def create_page_urls(self, category_url:str):
        if self.ending_page == None:
            page_urls = []
            response = self.client.get(url=category_url)
            soup = BeautifulSoup(response.content, 'lxml')
            
            # check request counter and change the api key if needed
            self.request_counter += 1
            self.create_client(initial_client=False)
            self.ending_page = int(soup.find('div', attrs={'class':'pagination'}).find_all('a')[-1].get_text())
            self.starting_page = 1

        if '&page=' in category_url:
            base_url = category_url.split('&page=')[0]
            for page_no in range(self.starting_page, self.ending_page + 1):
                page_urls.append(f'{base_url}&page={page_no}')
        elif '?page=' in category_url:
            base_url = category_url.split('?page=')[0]
            for page_no in range(self.starting_page, self.ending_page + 1):
                page_urls.append(f'{base_url}?page={page_no}')
        else:
            base_url = category_url
            if '?' in base_url:
                for page_no in range(self.starting_page, self.ending_page + 1):
                    page_urls.append(f'{base_url}&page={page_no}')
            else:
                for page_no in range(self.starting_page, self.ending_page + 1):
                    page_urls.append(f'{base_url}?page={page_no}')
        return page_urls
    

    def convert_to_excel(self, filename:str):
        if not filename.endswith('.xlsx'):
            filename = filename + '.xlsx'

        if os.path.exists(filename):
            existing_data = pd.read_excel(filename)
            df = pd.concat([existing_data, pd.DataFrame(self.list_for_excel)], ignore_index=True)
        else:
            df = pd.DataFrame(self.list_for_excel)
        df.to_excel(filename, index=False)
        print(f'\n------------------------\n{filename} adlı dosyaya kaydedildi\n------------------------\n')
        self.list_for_excel = []


    def create_client(self, initial_client:bool=False):
        if initial_client == True:
            self.client = ZenRowsClient(apikey=self.API_LIST[0])
            self.api_index += 1
            self.request_counter = 0
            return self.client
        else:
            if self.request_counter >= self.request_limit:
                print(f'\n----------------------\nAPI KEY DEĞİŞTİRİLİYOR, req: {self.request_counter}\n----------------------\n')
                if self.api_index > (len(self.API_LIST) - 1):
                    print('\nTÜM API KEYLER KULLANILDI, VERİ KAYDEDİLİYOR.')
                    self.convert_to_excel(filename=str(random.randint(1,100000)))
                    raise 'API KEY KALMADI'
                else:
                    self.client = ZenRowsClient(apikey=self.API_LIST[self.api_index + 1])
                    self.api_index += 1
                    self.request_counter = 0
                    return self.client
                

    def get_user_input(self):
        print('''
              \n\t\t------------------------------------------------------------------------------------------\n
              * Tüm ürünlerin verisini almak için link kısmına hiçbir sey yazmadan entera basmanız yeterli
              * Yeni ürünleri almak için ise "new" yazıp entera basabilirsiniz
              * Eğer link olarak "new" verirseniz başlangıç ve bitiş sayfalarını belirtmeniz gerekir
              * Başlangıç ve bitiş sayfalarını boş bırakırsanız ilk sayfadan son sayfaya kadar çekecektir
              \n\t\t------------------------------------------------------------------------------------------\n
              ''')
        self.url = input('Link: ')
        if self.url == '':
            print('\nTüm kategorilerdeki ürünlerin verisi çekilecek.\n')
        elif self.url.lower().strip() == 'new':
            self.url = 'https://vaden.com.tr/tr/newproducts?page='
            self.category_urls = [self.url]
            self.starting_page = str(input('Başlangıç sayfası: '))
            self.ending_page = str(input('Bitiş sayfası: '))
        else:
            self.category_urls = [self.url]
            self.starting_page = str(input('Başlangıç sayfası: '))
            self.ending_page = str(input('Bitiş sayfası: '))

        if self.starting_page == None and self.ending_page == None:
            pass
        elif self.starting_page == '' and self.ending_page == '':
            self.starting_page, self.ending_page = None, None
        else:
            self.starting_page, self.ending_page = int(self.starting_page), int(self.ending_page)


    def signal_handler(self, sig, frame):
        self.convert_to_excel(filename=self.filename)
        print("Program durduruldu. Veriler kaydedildi.")
        exit(0)


if __name__ == '__main__':
    scraper = VadenScraperZR()
    scraper.main()
    