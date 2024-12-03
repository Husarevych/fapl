import mysql.connector

class ConnectToMySql:
    
    """ A class to handle MySQL database connections and operations. 
    Attributes: 
        host (str): Hostname of the MySQL server. 
        user (str): Username to access the MySQL server. 
        password (str): Password for the MySQL user. 
        database (str): Name of the database to use. 
        conn: Database connection object. 
        cursor: Cursor object to execute queries. """
    
    def __init__(self, host, user, password, database):
        
        """ Initializes the database connection parameters. 
        Args: 
            host (str): Hostname of the MySQL server. 
            user (str): Username to access the MySQL server. 
            password (str): Password for the MySQL user. 
            database (str): Name of the database to use. """
        
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None

    def connect_to_database(self):
        
        """ Establishes a connection to the MySQL database. 
            Sets the conn and cursor attributes. 
            Prints an error message if the connection fails. """
        
        try:
            self.conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.cursor = self.conn.cursor()
        except mysql.connector.Error as err:
            print(f"Database connection error: {err}")
            self.conn = None
            self.cursor = None

    def use_database(self): 
        
        """ Selects the database to use for the connection. 
        Prints an error message if the selection fails. """ 
        
        if self.conn and self.cursor: 
            try: 
                self.cursor.execute(f"USE {self.database}") 
            except mysql.connector.Error as err: 
                print(f"Error switching database: {err}")
    
    def table_exists(self, table_name: str) -> bool:
        
        """
        Checks if a table exists in the database.

        Args:
            table_name (str): The name of the table to check.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        
        # Check if the database connection or cursor does not exist
        if not self.conn or not self.cursor:
            print("No database connection.")  # Print an error message if there is no database connection
            return False  # Return False to indicate the failure of the operation


        query = """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        """
        try:
            self.cursor.execute(query, (self.database, table_name))  # Execute the query with the database and table name
            return self.cursor.fetchone()[0] > 0  # Return True if the table exists, otherwise False
        except Exception as e:
            print(f"Error checking table existence: {e}")  # Print an error message if there is an exception
            return False  # Return False to indicate the table does not exist or an error occurred

    def create_table(self, table_name):
        
        """
        Creates a table with the given name and indices.
        
        Args:
            table_name (str): The name of the table to create.
        """
        
        if not self.conn or not self.cursor:
            print("No database connection.")  # Print an error message if there is no database connection
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
            self.cursor.execute(query)  # Execute the SQL query to create the table
            print(f"Table '{table_name}' created successfully or already exists.")  # Print a success message
        except mysql.connector.Error as err:
            print(f"Error creating table: {err}")  # Print an error message if there is an exception
 
    def get_existing_post_ids(self, table_name):
        
        """Returns a list of existing post_ids in the table."""
        
        if not self.conn or not self.cursor:
            print("No database connection.")  # Print an error message if there is no database connection
            return []

        try:
            query = f"SELECT post_id FROM {table_name};"  # Form the SQL query to select post_id from the specified table
            self.cursor.execute(query)  # Execute the SQL query
            return {record[0] for record in self.cursor.fetchall()}  # Return a set of post_ids from the fetched records
        except mysql.connector.Error as err:
            print(f"Error fetching data: {err}")  # Print an error message if there is an exception
            return set()  # Return an empty set in case of an error
 
    def insert_data(self, table_name, data):
        
        """Inserts data into the table if the post_id does not already exist."""
        
        if not self.conn or not self.cursor:
            print("No database connection.")  # Print an error message if there is no database connection
            return

        try:
            # Get the list of existing post_ids
            existing_ids = self.get_existing_post_ids(table_name)

            query = f"""
            INSERT INTO {table_name} 
            (post_id, header, content, time, post_visits, post_comments, post_tags)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """
            for post_id, fields in data.items():
                if post_id not in existing_ids:  # Check if the record exists
                    self.cursor.execute(query, (
                        post_id,
                        fields['header'],
                        fields['content'],
                        fields['time'],
                        int(fields['post_visits']),
                        int(fields['post_comments']),
                        fields['post_tags']
                    ))
            print("Data successfully added to the table.")  # Print a success message
        except mysql.connector.Error as err:
            print(f"Error inserting data: {err}")  # Print an error message if there is an exception

    def fetch_recent_post_ids(self, table_name, limit=100):
        """
        Fetches a specified number of recent post_ids from the table, sorted by date.
        
        Args:
            table_name (str): The name of the table to query.
            limit (int, optional): The number of records to fetch (default is 100).
        
        Returns:
            list: A list of post_ids or an empty list in case of error.
        """
        if not self.conn or not self.cursor:
            print("No database connection.")  # Print an error message if there is no database connection
            return []

        try:
            query = f"""
            SELECT post_id FROM {table_name}
            ORDER BY time DESC
            LIMIT %s;
            """
            self.cursor.execute(query, (limit,))  # Execute the query with the specified limit
            result = [row[0] for row in self.cursor.fetchall()]  # Fetch the post_ids and store in a list
            return result
        except mysql.connector.Error as err:
            print(f"Error fetching data: {err}")  # Print an error message if there is an exception
            return []  # Return an empty list in case of error

    def commit_and_close(self):
        
        """Commits any pending transactions and closes the database connection."""
        
        if self.conn:
            try:
                self.conn.commit()  # Commit any pending transactions
            except mysql.connector.Error as err:
                print(f"Error committing changes: {err}")  # Print an error message if there is an exception during commit
            finally:
                self.cursor.close()  # Close the cursor
                self.conn.close()  # Close the database connection

