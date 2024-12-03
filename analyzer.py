import pandas as pd 
import matplotlib.pyplot as plt 
from sqlalchemy.engine import Connection

class Analyzer:
    
    """ A class to analyze and visualize data from a database. 
    Attributes: 
        connection (Connection): An active SQLAlchemy database connection. """
    
    def __init__(self, db_connection: Connection):
        """
        Initializes the analyzer with a database connection.

        Args:
            db_connection (Connection): An active SQLAlchemy database connection.
        """
        self.connection = db_connection

    def fetch_data(self, table_name: str) -> pd.DataFrame:
        """
        Fetches data from the specified table into a pandas DataFrame.

        Args:
            table_name (str): The name of the table to query.

        Returns:
            DataFrame: The data retrieved from the table.
        """
        query = f"SELECT * FROM {table_name}"  # Form the SQL query to select all data from the specified table
        data = pd.read_sql(query, self.connection)  # Execute the SQL query and read the data into a DataFrame
        return data  # Return the DataFrame containing the fetched data

    def visualize_popularity(self, data: pd.DataFrame) -> None:
        """
        Visualizes the popularity of articles by their view counts.

        Args:
            data (DataFrame): A DataFrame containing article data.
        """
        # Keep only the top 20 entries sorted by 'post_visits'
        top_data = data.sort_values('post_visits', ascending=False).head(20)

        # Add a new column combining the title and the publication date
        top_data['header_with_date'] = (
            top_data['header'] + '\n' + '(' + top_data['time'].dt.strftime('%Y-%m-%d') + ')'
        )
        
        # Create a larger figure for better readability
        plt.figure(figsize=(15, 10))

        # Create horizontal bars for better readability
        bars = plt.barh(top_data['header_with_date'], top_data['post_visits'], color='skyblue')

        # Reverse the order so the most popular article is at the top
        plt.gca().invert_yaxis()

        # Adding numerical view counts inside the bars 
        for bar in bars: 
            plt.text(bar.get_width() / 2, # Place text at the center of the bar 
                     bar.get_y() + bar.get_height() / 2, # Center the text vertically 
                     f'{int(bar.get_width())}', # Convert view counts to integers 
                     va='center', # Vertical alignment 
                     ha='center', # Horizontal alignment 
                     fontsize=12, # Font size of the text
                     color='black' # Black text for contrast
            )

        # Chart settings
        plt.title('Top-20 Most Popular Articles', fontsize=16)  # Set the title of the chart with font size 16
        plt.xlabel('View Counts', fontsize=12)  # Label for the x-axis with font size 12
        plt.ylabel('Titles', fontsize=12)  # Label for the y-axis with font size 12
        plt.tight_layout()  # Adjust subplots to fit into the figure area
        plt.show()  # Display the chart

    def tags_analysis(self, data: pd.DataFrame) -> None:
        """
        Analyzes the frequency of tags and displays them on a bar chart.

        Args:
            data (DataFrame): A DataFrame containing article data.
        """
        # Split tags and count their frequency
        tags = data['post_tags'].str.split(',').explode().value_counts()

        # Get the top 10 tags
        top_tags = tags[:10]

        # Create a bar chart
        plt.figure(figsize=(15, 10)) # Set the size of the figure to 15x10 inches
        bars = plt.bar(top_tags.index, top_tags.values, color='lightcoral') # Create vertical bars with light coral color

        # Add text displaying the count on each bar
        for bar in bars:
            plt.text(
                bar.get_x() + bar.get_width() / 2,  # Center of the bar on the X-axis
                bar.get_height() / 2,  # Text height - center of the bar on the Y-axis
                str(bar.get_height()),  # Text - number of mentions
                ha='center',  # Horizontal alignment
                va='center',  # Vertical alignment
                fontsize=12, # Font size of the text
                color='black',  # Black text color
                fontweight='bold'  # Bold font for better readability
            )

        # Chart settings
        plt.title('Tag Frequency', fontsize=16)  # Set the title of the chart with font size 16
        plt.xlabel('Tags', fontsize=12)  # Label for the x-axis with font size 12
        plt.ylabel('Number of Mentions', fontsize=12)  # Label for the y-axis with font size 12
        plt.xticks(rotation=45, ha='right')  # Rotate the x-axis labels by 45 degrees and align them to the right
        plt.tight_layout()  # Adjust subplots to fit into the figure area
        plt.show()  # Display the chart

    def analyze_comments_by_tags(self, data: pd.DataFrame) -> None:
        """
        Analyzes the number of comments for each tag.

        Args:
            data (DataFrame): A DataFrame containing article data.
        """
        # Split tags into separate rows and create a flattened table
        exploded_tags = data.assign(tag=data['post_tags'].str.split(',')).explode('tag')
        
        # Group by tags and sum the comments
        tag_comments = exploded_tags.groupby('tag')['post_comments'].sum().sort_values(ascending=False)
        
        # Keep only the top 10 tags
        top_tags = tag_comments.head(10)
        
        # Create a horizontal bar chart
        plt.figure(figsize=(12, 8)) # Set the size of the figure to 12x8 inches
        bars = plt.barh(top_tags.index, top_tags.values, color='lightgreen') # Create horizontal bars with light green color
        plt.gca().invert_yaxis()  # Reverse order so most popular tags are at the top
        
        # Adding numerical values of comments to the bar chart
        for bar in bars:
            plt.text(
                bar.get_width() + 2, # Position the text just to the right of the end of the bar
                bar.get_y() + bar.get_height() / 2, # Center the text vertically within the bar
                f'{int(bar.get_width())}', # Convert view counts to integers
                va='center', # Vertical alignment
                fontsize=12 # Font size of the text
            )
        
        plt.title('Number of Comments by Tags', fontsize=16)  # Set the chart title with font size 16
        plt.xlabel('Number of Comments', fontsize=14)  # Label for the x-axis with font size 14
        plt.ylabel('Tags', fontsize=14)  # Label for the y-axis with font size 14
        plt.tight_layout()  # Adjust subplots to fit into the figure area
        plt.show()  # Display the chart

    def save_to_csv(self, data, filename='output.csv'):
        """
        Saves data to a CSV file.
        :param data: pandas.DataFrame containing the data.
        :param filename: Name of the file to save the data to.
        """
        data.to_csv(filename, index=False) # Save the DataFrame to a CSV file without the index column
        print(f"Data saved to {filename}")

