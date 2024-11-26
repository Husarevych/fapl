
import scrapy

class FaplSpider(scrapy.Spider):
    name = "fapl"
    allowed_domains = ["fapl.ru"]
    start_urls = ["http://fapl.ru/news/"]

    # Счётчик страниц и максимальное количество страниц
    page_count = 0  
    custom_settings = {"max_pages": 10}  # Значение по умолчанию
    results = {}

    def __init__(self, max_pages=10, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_pages = int(max_pages)

    def parse(self, response):
        # Извлечение всех блоков новостей
        news_blocks = response.css('div.block.news')

        for article in news_blocks:
            # Извлечение ссылки на статью
            relative_url = article.css('h3 a::attr(href)').get()
            absolute_url = response.urljoin(relative_url)

            # Извлечение количества комментариев
            comments = article.css('p.f-r a::text').re_first(r'\((\d+)\)') or 0

            # Переход на страницу статьи
            yield response.follow(
                absolute_url,
                callback=self.parse_article,
                cb_kwargs={'comments': int(comments), 'relative_url': relative_url}
            )
        self.page_count += 1

        # Пагинация: ищем ссылку на "Страницу назад"
        if self.page_count < self.max_pages:
            next_page = response.css('div.aln.paging a:contains("Страница назад")::attr(href)').get()
            if next_page:
                # Формируем полный URL и отправляем следующий запрос
                yield response.follow(next_page, callback=self.parse)

    def parse_article(self, response, comments, relative_url):
        # Извлечение данных статьи
        header = response.css('div.block h2::text').get()
        content = [i.replace('\r\n', '').strip() for i in response.css('div.content p::text').getall() if i not in ('\r\n', '20:00')]
        tags = response.css('div.info p.tags a::text').getall()
        visits = int(response.css('p.visits.f-l b::text').get(default='0').strip())
        post_time = response.css('p.date.f-r a::text').get().strip()

        # Сохранение данных в общий словарь
        self.results[relative_url] = {
            'header': header,
            'content': content,
            'tags': tags,
            'visits': visits,
            'comments': comments,
            'post_time': post_time,
        }
