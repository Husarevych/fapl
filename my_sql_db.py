import mysql.connector

class ConnectToMySql:
    
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None

    def connect_to_database(self):
        try:
            self.conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.cursor = self.conn.cursor()
        except mysql.connector.Error as err:
            print(f"Ошибка подключения к базе данных: {err}")
            self.conn = None
            self.cursor = None

    def use_database(self):
        if self.conn and self.cursor:
            try:
                self.cursor.execute(f"USE {self.database}")
            except mysql.connector.Error as err:
                print(f"Ошибка при переключении базы данных: {err}")

    def create_table(self, table_name):
        """Создает таблицу с заданным именем и индексами."""
        if not self.conn or not self.cursor:
            print("Нет соединения с базой данных.")
            return

        try:
            query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                post_id VARCHAR(255) NOT NULL,
                header TEXT,
                content TEXT,
                time DATETIME,
                post_visits INT,
                post_comments INT,
                post_tags TEXT,
                PRIMARY KEY (post_id)
            );
            """
            self.cursor.execute(query)
            print(f"Таблица '{table_name}' успешно создана или уже существует.")
        except mysql.connector.Error as err:
            print(f"Ошибка при создании таблицы: {err}")
    
    def get_existing_post_ids(self, table_name):
        """Возвращает список существующих post_id в таблице."""
        if not self.conn or not self.cursor:
            print("Нет соединения с базой данных.")
            return []

        try:
            query = f"SELECT post_id FROM {table_name};"
            self.cursor.execute(query)
            return {record[0] for record in self.cursor.fetchall()}
        except mysql.connector.Error as err:
            print(f"Ошибка при получении данных: {err}")
            return set()
    
    def insert_data(self, table_name, data):
        """Добавляет данные в таблицу, если post_id ещё не существует."""
        if not self.conn or not self.cursor:
            print("Нет соединения с базой данных.")
            return

        try:
            # Получение списка существующих post_id
            existing_ids = self.get_existing_post_ids(table_name)

            query = f"""
            INSERT INTO {table_name} 
            (post_id, header, content, time, post_visits, post_comments, post_tags)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """
            for post_id, fields in data.items():
                if post_id not in existing_ids:  # Проверяем, существует ли запись
                    self.cursor.execute(query, (
                        post_id,
                        fields['header'],
                        fields['content'],
                        fields['time'],
                        int(fields['post_visits']),
                        int(fields['post_comments']),
                        fields['post_tags']
                    ))
            print("Данные успешно добавлены в таблицу.")
        except mysql.connector.Error as err:
            print(f"Ошибка при добавлении данных: {err}")

    def fetch_recent_post_ids(self, table_name, limit=100):
        """
        Получает заданное количество последних post_id из таблицы, отсортированных по дате.
        :param table_name: Имя таблицы.
        :param limit: Количество записей для получения (по умолчанию 100).
        :return: Список post_id или пустой список в случае ошибки.
        """
        if not self.conn or not self.cursor:
            print("Нет соединения с базой данных.")
            return []

        try:
            query = f"""
            SELECT post_id FROM {table_name}
            ORDER BY time DESC
            LIMIT %s;
            """
            self.cursor.execute(query, (limit,))
            result = [row[0] for row in self.cursor.fetchall()]
            return result
        except mysql.connector.Error as err:
            print(f"Ошибка при получении данных: {err}")
            return []

    def commit_and_close(self):
        if self.conn:
            try:
                self.conn.commit()
            except mysql.connector.Error as err:
                print(f"Ошибка при фиксации изменений: {err}")
            finally:
                self.cursor.close()
                self.conn.close()
