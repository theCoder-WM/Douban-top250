import requests
import threading
from queue import Queue
from lxml import etree
from time import time, sleep
from re import findall
from openpyxl import load_workbook


class DouBanSpider:
    def __init__(self):
        self.base_url = 'https://movie.douban.com/top250'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36'
        }
        self.urls = []
        self.data = Queue()
        self.count = 1
        self.formatted_data = []
        self.write_in = './top250.xlsx'
        self.workbook = load_workbook(self.write_in)
        self.debug = False
        if self.debug:
            print('init')

    def analyze_url(self, url):
        if self.debug:
            print('analyze url')
        if url != self.base_url:
            page = int(findall('https://movie.douban.com/top250\?start=(.*)&filter=', url)[0])
        else:
            page = 0
        # print(page)
        sleep(1 + page / 60)
        res = requests.get(url, headers=self.headers)
        html = etree.HTML(res.content)
        return html

    def get_full_url(self):
        if self.debug:
            print('get url')
        html = self.analyze_url(self.base_url)
        tags = html.xpath('//*[@id="content"]/div/div[1]/div[2]/a')
        suffixes = [i.attrib['href'] for i in tags]
        all_urls = list(map(lambda sfx: self.base_url + sfx, suffixes))
        self.urls.append(self.base_url)
        self.urls = [*self.urls, *all_urls]
        return self.urls

    def write_into_excel(self, path, workbook, x_cdn: int, data: tuple):
        ITEM_NUM = len(data)
        sheet = workbook.active
        y_cdn = None
        cdn = None
        base_asc = ord('A')
        for asc in range(base_asc, base_asc + ITEM_NUM):
            y_cdn = chr(asc)
            cdn = f'{y_cdn}{x_cdn}'
            sheet[cdn].value = data[asc - base_asc]
        workbook.save(path)

    def get_data(self, url):
        if self.debug:
            print('get data')
        html = self.analyze_url(url)
        # print(html.xpath('/html/head/title')[0].text)
        for i in range(25):
            movie_id = html.xpath(f'/html/body/div[3]/div[1]/div/div[1]/ol/li[{i + 1}]/div/div[1]/em')[0].text
            movie_name = html.xpath(f'/html/body/div[3]/div[1]/div/div[1]/ol/li[{i + 1}]/div/div[2]/div[1]/a/span[1]')[
                0].text
            movie_mark = float(
                html.xpath(f'/html/body/div[3]/div[1]/div/div[1]/ol/li[{i + 1}]/div/div[2]/div[2]/div/span[2]')[0].text)
            try:
                movie_info = html.xpath(f'/html/body/div[3]/div[1]/div/div[1]/ol/li[{i + 1}]/div/div[2]/div[2]/p[2]/span')[0].text
            except IndexError:
                movie_info = ''
            self.data.put((int(movie_id), movie_name, movie_mark, movie_info))
            self.count += 1

    def run(self):
        if self.debug:
            print('run')
        self.get_full_url()
        threads = []
        for url in self.urls:
            new_thread = threading.Thread(target=self.get_data, args=(url,))
            new_thread.start()
            # print(f'{new_thread.name} start')
            threads.append(new_thread)
        for thread in threads:
            thread.join()
        while not self.data.empty():
            tmp_data = self.data.get()
            print(tmp_data)
            self.write_into_excel(path='./top250.xlsx', workbook=self.workbook, x_cdn=tmp_data[0] + 1, data=tmp_data)
        del tmp_data
        if self.debug:
            print(douban_spider.urls)


if __name__ == '__main__':
    start_time = time()
    douban_spider = DouBanSpider()
    douban_spider.run()
    end_time = time()
    print(f'{round(end_time - start_time, 3)}s')
