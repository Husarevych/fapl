import os
from dotenv import load_dotenv
from scraper import Scraper
from my_sql_db import ConnectToMySql
from analyzer import Analyzer
from typing import Optional

def main(mode: str = 'incremental') -> None:
    """
    Main function for running the scraper, database operations, and analysis.

    Args:
        mode (str): Determines scraping mode. Defaults to 'incremental'.

    Returns:
        None
    """

    # Load environment variables
    load_dotenv()
    host: Optional[str] = os.getenv('host')
    user: Optional[str] = os.getenv('user')
    password: Optional[str] = os.getenv('password')
    database: Optional[str] = os.getenv('database')
    
    # Initialize database connection
    db: ConnectToMySql = ConnectToMySql(host, user, password, database)  # Create a ConnectToMySql instance with the provided host, user, password, and database
    db.connect_to_database()  # Establish a connection to the database
    db.use_database()  # Select the database to use


    # Check if table exists; create if not
    if not db.table_exists('news'):
        db.create_table('news')
        print("Table 'news' created.")
    else:
        print("Table 'news' already exists.")
    
    # Fetch recent post IDs from the database
    last_posts = db.fetch_recent_post_ids('news')
    
    # Initialize scraper and fetch new data
    scraper: Scraper = Scraper(last_posts, mode)
    data = scraper.scraper()

    # Insert scraped data into the database
    db.insert_data('news', data)

    # Uncomment the following lines for analysis
    analyzer: Analyzer = Analyzer(db.conn)
    fetched_data = analyzer.fetch_data('news')
    analyzer.visualize_popularity(fetched_data)
    # analyzer.tags_analysis(fetched_data) 
    # analyzer.analyze_comments_by_tags(fetched_data)

    # Commit changes and close the connection
    db.commit_and_close()

if __name__ == '__main__':
    main()
    


