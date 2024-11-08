import cx_Oracle
from tabulate import tabulate
from dotenv import load_dotenv
import os
load_dotenv()
username = os.getenv("USN")
password = os.getenv("PSWD")
host = os.getenv("HOST")

# Establish connection
def create_connection():
    try:
        connection = cx_Oracle.connect(username, password, host)
        print("Connected to Oracle Database")
        return connection
    except cx_Oracle.DatabaseError as e:
        print(f"Error connecting to Oracle Database: {e}")
        return None


def execute_user_query(connection, query):
    """
    Executes a user-provided SQL query and displays the results in a formatted table.
    
    Parameters:
    connection (cx_Oracle.Connection): The Oracle database connection.
    query (str): The SQL query to execute.
    
    Returns:
    None
    """
    cursor = connection.cursor()
    try:
        # Determine if it's a SELECT query
        if query.strip().lower().startswith("select"):
            cursor.execute(query)
            # Fetch all rows from the result
            rows = cursor.fetchall()
            # Fetch column names
            col_names = [col[0] for col in cursor.description]
            # Display rows in a table format using tabulate
            print(tabulate(rows, headers=col_names, tablefmt="grid"))
        else:
            # If not a SELECT query, execute without fetching
            cursor.execute(query)
            connection.commit()
            print("Query executed successfully.")
    except cx_Oracle.DatabaseError as e:
        # Handle database errors
        print(f"An error occurred: {e}")
    finally:
        cursor.close()

# Example usage:
def main():
    connection = create_connection()
    if connection:
        # query = input("Enter your SQL query: ")
        query = """
            SELECT * FROM (
                SELECT 
                    b.Title AS "Book Title",
                    m.FirstName || ' ' || m.LastName AS "Borrower Name",
                    (br.ReturnDate - br.BorrowDate) AS "Borrow Duration"
                FROM Borrowing br
                JOIN Book b ON br.ISBN = b.ISBN
                JOIN Member m ON br.MEMBERID = m.MEMBERID
                WHERE br.ReturnDate IS NOT NULL
                AND (br.ReturnDate - br.BorrowDate) = (
                    SELECT MAX(br2.ReturnDate - br2.BorrowDate)
                    FROM Borrowing br2
                    WHERE br2.ISBN = br.ISBN
                )
                ORDER BY "Borrow Duration" DESC
            ) WHERE ROWNUM = 1
        """
        execute_user_query(connection, query)
        connection.close()

if __name__ == "__main__":
    main()
