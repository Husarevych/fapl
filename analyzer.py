import pandas as pd
import matplotlib.pyplot as plt

class Analyzer:
    def __init__(self, db_connection):
        """
        Инициализация аналитика с подключением к базе данных.
        :param db_connection: Существующий объект подключения к БД.
        """
        self.connection = db_connection

    def fetch_data(self, table_name):
        """
        Извлекает данные из указанной таблицы в формате DataFrame.
        :param table_name: Название таблицы.
        :return: pandas.DataFrame с данными.
        """
        query = f"SELECT * FROM {table_name}"
        data = pd.read_sql(query, self.connection)
        return data

    def visualize_popularity(self, data):
        """
        Строит график популярности статей по просмотрам.
        :param data: pandas.DataFrame с данными.
        """
        # Оставляем только топ-10 записей и сортируем их в порядке убывания
        top_data = data.sort_values('post_visits', ascending=False).head(20)

        # Добавляем колонку с объединением заголовка и даты публикации
        top_data['header_with_date'] = top_data['header'] + '\n' + '(' + top_data['time'].dt.strftime('%Y-%m-%d') + ')'
        
        # Увеличиваем размер графика
        plt.figure(figsize=(15, 10))

        # Горизонтальные бары для лучшей читаемости
        bars = plt.barh(top_data['header_with_date'], top_data['post_visits'], color='skyblue')

        # Обратный порядок: самый популярный сверху
        plt.gca().invert_yaxis()

        # Добавляем числовые значения просмотров внутрь баров
        for bar in bars:
            plt.text(
                bar.get_width() / 2,  # Располагаем текст центру бара
                bar.get_y() + bar.get_height() / 2,  # Центрируем текст по высоте
                f'{int(bar.get_width())}',  # Преобразуем значения просмотров в целые
                va='center',  # Вертикальное выравнивание
                ha='center',  # Горизонтальное выравнивание вправо
                fontsize=12,
                color='black'  # Черный текст для контраста
            )

        plt.title('Топ-10 популярных статей', fontsize=16)
        plt.xlabel('Просмотры', fontsize=12)
        plt.ylabel('Заголовки', fontsize=12)
        plt.tight_layout()
        plt.show()

    def tags_analysis(self, data):
        """
        Анализирует частоту встречаемости тегов с отображением количества на графике.
        :param data: pandas.DataFrame с данными.
        """
        # Разбиваем теги, подсчитываем их частоту
        tags = data['post_tags'].str.split(',').explode().value_counts()

        # Берем топ-10 тегов
        top_tags = tags[:10]

        # Создаем график
        plt.figure(figsize=(15, 10))
        bars = plt.bar(top_tags.index, top_tags.values, color='lightcoral')

        # Добавляем текст с количеством на каждый бар
        for bar in bars:
            plt.text(
                bar.get_x() + bar.get_width() / 2,  # Центр бара по X
                bar.get_height() / 2,  # Высота текста - центр бара по Y
                str(bar.get_height()),  # Текст - количество упоминаний
                ha='center',  # Горизонтальное выравнивание
                va='center',  # Вертикальное выравнивание
                fontsize=12,
                color='black',  # Черный цвет текста
                fontweight='bold'  # Жирный шрифт для лучшей читаемости
            )

        # Настройки графика
        plt.title('Частота тегов', fontsize=16)
        plt.xlabel('Теги', fontsize=12)
        plt.ylabel('Количество упоминаний', fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.show()

    def analyze_comments_by_tags(self, data):
        """
        Анализирует количество комментариев в разрезе тегов.
        :param data: pandas.DataFrame с данными.
        """
        # Разделение тегов и создание развернутой таблицы
        exploded_tags = data.assign(tag=data['post_tags'].str.split(',')).explode('tag')
        
        # Группировка и суммирование комментариев
        tag_comments = exploded_tags.groupby('tag')['post_comments'].sum().sort_values(ascending=False)
        
        # Оставляем только топ-10 тегов
        top_tags = tag_comments.head(10)
        
        # Построение графика
        plt.figure(figsize=(12, 8))
        bars = plt.barh(top_tags.index, top_tags.values, color='lightgreen')
        plt.gca().invert_yaxis()  # Обратный порядок (самые популярные сверху)
        
        # Добавляем числовые значения комментариев на барчарт
        for bar in bars:
            plt.text(
                bar.get_width() + 2,  # Расположение текста чуть правее конца бара
                bar.get_y() + bar.get_height() / 2,
                f'{int(bar.get_width())}',
                va='center', fontsize=12
            )
        
        plt.title('Количество комментариев по тегам', fontsize=16)
        plt.xlabel('Количество комментариев', fontsize=14)
        plt.ylabel('Теги', fontsize=14)
        plt.tight_layout()
        plt.show()

    def save_to_csv(self, data, filename='output.csv'):
        """
        Сохраняет данные в CSV.
        :param data: pandas.DataFrame с данными.
        :param filename: Имя файла для сохранения.
        """
        data.to_csv(filename, index=False)
        print(f"Данные сохранены в {filename}")
