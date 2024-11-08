import cx_Oracle
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

def drop_table_if_exists(connection, table_name):
    cursor = connection.cursor()
    try:
        cursor.execute(f"""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 THEN -- ORA-00942: table or view does not exist
                    RAISE;
                END IF;
        END;
        """)
    finally:
        cursor.close()


def create_table_author(connection):
    cursor = connection.cursor()
    create_table_sql = """
    CREATE TABLE Author (
        AuthorID NUMBER PRIMARY KEY,
        FirstName VARCHAR2(50),
        LastName VARCHAR2(50)
    )
    """
    cursor.execute(create_table_sql)
    cursor.close()

def create_table_genre(connection):
    cursor = connection.cursor()
    create_table_sql = """
    CREATE TABLE Genre (
        GenreID NUMBER PRIMARY KEY,
        GenreName VARCHAR2(50)
    )
    """
    cursor.execute(create_table_sql)
    cursor.close()

def create_table_book(connection):
    cursor = connection.cursor()
    create_table_sql = """
    CREATE TABLE Book (
        ISBN VARCHAR2(20) PRIMARY KEY,
        Title VARCHAR2(100),
        InStock NUMBER
    )
    """
    cursor.execute(create_table_sql)
    cursor.close()

def create_table_publisher(connection):
    cursor = connection.cursor()
    create_table_sql = """
    CREATE TABLE Publisher (
        PublisherID NUMBER PRIMARY KEY,
        Name VARCHAR2(100),
        Location VARCHAR2(100)
    )
    """
    cursor.execute(create_table_sql)
    cursor.close()

def create_table_member(connection):
    cursor = connection.cursor()
    create_table_sql = """
    CREATE TABLE Member (
        MEMBERID NUMBER PRIMARY KEY,
        FirstName VARCHAR2(50),
        LastName VARCHAR2(50),
        Age NUMBER
    )
    """
    cursor.execute(create_table_sql)
    cursor.close()

def create_table_contact(connection):
    cursor = connection.cursor()
    create_table_sql = """
    CREATE TABLE Contact (
        MEMBERID NUMBER,
        EMAIL VARCHAR2(100), 
        FOREIGN KEY (MEMBERID) REFERENCES Member(MEMBERID),
        PRIMARY KEY (MEMBERID, EMAIL)
    )
    """
    cursor.execute(create_table_sql)
    cursor.close()

def create_table_borrowing(connection):
    cursor = connection.cursor()
    create_table_sql = """
    CREATE TABLE Borrowing (
        MEMBERID NUMBER,
        ISBN VARCHAR2(20),
        BorrowID NUMBER,
        BorrowDate DATE,
        ReturnDate DATE,
        FOREIGN KEY (MEMBERID) REFERENCES Member(MEMBERID), 
        FOREIGN KEY (ISBN) REFERENCES Book(ISBN),
        PRIMARY KEY (MEMBERID, ISBN, BorrowID)
    )
    """
    cursor.execute(create_table_sql)
    cursor.close()

# Association tables for relationships

def create_table_authored_by(connection):
    cursor = connection.cursor()
    create_table_sql = """
    CREATE TABLE AuthoredBy (
        AuthorID NUMBER,
        ISBN VARCHAR2(20),
        FOREIGN KEY (AuthorID) REFERENCES Author(AuthorID),
        FOREIGN KEY (ISBN) REFERENCES Book(ISBN),
        PRIMARY KEY (AuthorID, ISBN)
    )
    """
    cursor.execute(create_table_sql)
    cursor.close()

def create_table_belongs_to_genre(connection):
    cursor = connection.cursor()
    create_table_sql = """
    CREATE TABLE BelongsToGenre (
        ISBN VARCHAR2(20),
        GenreID NUMBER,
        FOREIGN KEY (ISBN) REFERENCES Book(ISBN),
        FOREIGN KEY (GenreID) REFERENCES Genre(GenreID),
        PRIMARY KEY (ISBN, GenreID)
    )
    """
    cursor.execute(create_table_sql)
    cursor.close()

def create_table_published_by(connection):
    cursor = connection.cursor()
    create_table_sql = """
    CREATE TABLE PublishedBy (
        ISBN VARCHAR2(20),
        PublisherID NUMBER,
        Year NUMBER,
        FOREIGN KEY (ISBN) REFERENCES Book(ISBN),
        FOREIGN KEY (PublisherID) REFERENCES Publisher(PublisherID),
        PRIMARY KEY (ISBN, PublisherID)
    )
    """
    cursor.execute(create_table_sql)
    cursor.close()

# def create_table_borrows(connection):
#     cursor = connection.cursor()
#     create_table_sql = """
#     CREATE TABLE Borrows (
#         MemberID NUMBER,
#         ISBN VARCHAR2(20),
#         FOREIGN KEY (MemberID) REFERENCES Member(ID),
#         FOREIGN KEY (ISBN) REFERENCES Book(ISBN),
#         PRIMARY KEY (MemberID, ISBN)
#     )
#     """
#     cursor.execute(create_table_sql)
#     cursor.close()

def main():
    connection = create_connection()
    if connection:
        drop_table_if_exists(connection, 'author')
        drop_table_if_exists(connection, 'genre')
        drop_table_if_exists(connection, 'book')
        drop_table_if_exists(connection, 'publisher')
        drop_table_if_exists(connection, 'member')
        drop_table_if_exists(connection, 'borrowing')
        drop_table_if_exists(connection, 'contact')

        drop_table_if_exists(connection, 'authoredby')
        drop_table_if_exists(connection, 'belongstogenre')
        drop_table_if_exists(connection, 'publishedby')
        # drop_table_if_exists(connection, 'borrows')

        create_table_author(connection)
        create_table_genre(connection)
        create_table_book(connection)
        create_table_publisher(connection)
        create_table_member(connection)
        create_table_contact(connection)
        create_table_borrowing(connection)
        
        create_table_authored_by(connection)
        create_table_belongs_to_genre(connection)
        create_table_published_by(connection)
        # create_table_borrows(connection)
        
        connection.close()
        print("All tables created successfully.")

# Run the main function
if __name__ == "__main__":
    main()
       
