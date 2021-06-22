import requests
from multiprocessing import Pool, Manager
from lxml import etree
from time import time, sleep
from re import findall


class DouBanSpider:
    def __init__(self):
        self.base_url = 'https://movie.douban.com/top250'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36'
        }
        self.urls = []
        self.manager = Manager()
        self.data = self.manager.Queue()
        self.count = 1
        self.formatted_data = []
        self.debug = True
        if self.debug:
            print('init')

    def analyze_url(self, url):
        if self.debug:
            print('analyze url')
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

    def get_data(self, url):
        if self.debug:
            print('get data')
        html = self.analyze_url(url)
        if url != self.base_url:
            page = int(findall('https://movie.douban.com/top250\?start=(.*)&filter=', url)[0])
        else:
            page = 0
        # print(page)
        sleep(1 + page / 60)
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
            self.data.put_nowait(f'{movie_id} {movie_name} {movie_mark} {movie_info}')
            self.count += 1

    def run(self):
        if self.debug:
            print('run')
        self.get_full_url()
        pool = Pool()
        for u in self.urls:
            pool.apply_async(func=self.get_data, kwds={'url': u})
            # TODO: This doesn't work
        pool.close()
        pool.join()
        if self.debug:
            print(self.data.qsize())
        while not self.data.empty():
            print(self.data.get())
        if self.debug:
            print(self.urls)


if __name__ == '__main__':
    start_time = time()
    douban_spider = DouBanSpider()
    douban_spider.run()
    end_time = time()
    print(f'{round(end_time - start_time, 3)}s')
