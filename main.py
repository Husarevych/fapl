import os
from dotenv import load_dotenv
from scraper import Scraper
from my_sql_db import ConnectToMySql
from analyzer import Analyzer

def main(mode='incremental'):

    load_dotenv()
    host = os.getenv('host')
    user = os.getenv('user')
    password = os.getenv('password')
    database = os.getenv('database')
    
    db = ConnectToMySql(host, user, password, database)
    
    db.connect_to_database()
    db.use_database()

    analyzer = Analyzer(db.conn)

    db.create_table('news')

    last_posts = db.fetch_recent_post_ids('news')
    
    scraper = Scraper(last_posts, mode)
    data = scraper.scraper()

    db.insert_data('news', data)

    # fetched_data = analyzer.fetch_data('news')
    # analyzer.visualize_popularity(fetched_data)
    # analyzer.tags_analysis(fetched_data) 
    # analyzer.analyze_comments_by_tags(fetched_data)


    db.commit_and_close()

if __name__ == '__main__':
    main()
