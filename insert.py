import cx_Oracle
import datetime
# from random import randint, choice

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


def clear_table(connection, table_name):
    cursor = connection.cursor()
    try:
        cursor.execute(f"DELETE FROM {table_name}")
        connection.commit()
        print(f"Cleared all records from '{table_name}' table.")
    except cx_Oracle.DatabaseError as e:
        print(f"Error clearing '{table_name}' table: {e}")
    finally:
        cursor.close()

def insert_authors(connection):
    # clear_table(connection, "Author")
    cursor = connection.cursor()
    authors = [
        (1, 'Amber', 'Smith'),
        (2, 'Jane', 'Doe'),
        (3, 'Emily', 'Johnson'),
        (4, 'Michael', 'Brown'),
        (5, 'Jessica', 'Davis'),
        (6, 'Olivia', 'Wilson'),
        (7, 'Sarah', 'Martinez'),
        (8, 'James', 'Anderson'),
        (9, 'Mary', 'Taylor'),
        (10, 'Igris', 'Thomas')
    ]
    cursor.executemany("INSERT INTO Author (AuthorID, FirstName, LastName) VALUES (:1, :2, :3)", authors)
    connection.commit()
    print("Inserted records into 'Author' table.")
    cursor.close()

def insert_genres(connection):
    # clear_table(connection, "Genre")
    cursor = connection.cursor()
    genres = [
        (1, 'Fiction'),
        (2, 'Non-Fiction'),
        (3, 'Science Fiction'),
        (4, 'Fantasy'),
        (5, 'Mystery'),
        (6, 'Horror'),
        (7, 'Romance'),
        (8, 'Thriller'),
        (9, 'Self-Help'),
        (10, 'Comedy')
    ]
    cursor.executemany("INSERT INTO Genre (GenreID, GenreName) VALUES (:1, :2)", genres)
    connection.commit()
    print("Inserted records into 'Genre' table.")
    cursor.close()

def insert_books(connection):
    # clear_table(connection, "Book")
    cursor = connection.cursor()
    books = [
        ('ISBN001', 'Book Title 1', 10),
        ('ISBN002', 'Book Title 2', 5),
        ('ISBN003', 'Book Title 3', 8),
        ('ISBN004', 'Book Title 4', 12),
        ('ISBN005', 'Book Title 5', 7),
        ('ISBN006', 'Book Title 6', 3),
        ('ISBN007', 'Book Title 7', 15),
        ('ISBN008', 'Book Title 8', 6),
        ('ISBN009', 'Book Title 9', 9),
        ('ISBN010', 'Book Title 10', 11),
        ('ISBN011', 'Book Title 11', 2),
        ('ISBN012', 'Book Title 12', 8),
        ('ISBN013', 'Book Title 13', 19),
        ('ISBN014', 'Book Title 14', 12),
        ('ISBN015', 'Book Title 15', 17),
        ('ISBN016', 'Book Title 16', 30),
        ('ISBN017', 'Book Title 17', 10),
        ('ISBN018', 'Book Title 18', 6),
        ('ISBN019', 'Book Title 19', 4),
        ('ISBN020', 'Book Title 20', 1)
    ]
    cursor.executemany("INSERT INTO Book (ISBN, Title, InStock) VALUES (:1, :2, :3)", books)
    connection.commit()
    print("Inserted records into 'Book' table.")
    cursor.close()

def insert_publishers(connection):
    # clear_table(connection, "Publisher")
    cursor = connection.cursor()
    publishers = [
        (1, 'Publisher 1', 'New York'),
        (2, 'Publisher 2', 'Los Angeles'),
        (3, 'Publisher 3', 'Chicago'),
        (4, 'Publisher 4', 'Houston'),
        (5, 'Publisher 5', 'Phoenix'),
        (6, 'Publisher 6', 'Philadelphia'),
        (7, 'Publisher 7', 'San Antonio'),
        (8, 'Publisher 8', 'San Diego'),
        (9, 'Publisher 9', 'Dallas'),
        (10, 'Publisher 10', 'San Jose')
    ]
    cursor.executemany("INSERT INTO Publisher (PublisherID, Name, Location) VALUES (:1, :2, :3)", publishers)
    connection.commit()
    print("Inserted records into 'Publisher' table.")
    cursor.close()

def insert_members(connection):
    # clear_table(connection, "Member")
    cursor = connection.cursor()
    members = [
        (1, 'Alice', 'Johnson', 15),
        (2, 'Bob', 'Smith', 20),
        (3, 'Cathy', 'Brown', 12),
        (4, 'Daniel', 'Davis', 18),
        (5, 'Eva', 'Wilson', 25),
        (6, 'Frank', 'Garcia', 30),
        (7, 'Grace', 'Martinez', 17),
        (8, 'Henry', 'Anderson', 23),
        (9, 'Ivy', 'Thomas', 14),
        (10, 'Jack', 'Taylor', 16),
        (11, 'Kevin', 'Morgan', 30),
        (12, 'Loki', 'Minson', 17),
        (13, 'Miso', 'Garfield', 23),
        (14, 'Noah', 'Warner', 19),
        (15, 'Oliver', 'Blackjack', 16)
    ]
    cursor.executemany("INSERT INTO Member (MemberID, FirstName, LastName, Age) VALUES (:1, :2, :3, :4)", members)
    connection.commit()
    print("Inserted records into 'Member' table.")
    cursor.close()

def insert_contacts(connection):
    # clear_table(connection, "Contact")
    cursor = connection.cursor()
    contacts = [
        (1, 'alice.johnson@example.com'),
        (1, 'alice.johnson@gmail.com'),
        (2, 'bob.smith@example.com'),
        (3, 'cathy.brown@example.com'),
        (4, 'daniel.davis@example.com'),
        (4, 'daniel.davis@yahoo.com'),
        (5, 'eva.wilson@example.com'),
        (6, 'frank.garcia@gmail.com'),
        (6, 'frank.garcia@yahoo.com'),
        (7, 'grace.martinez@example.com'),
        (8, 'henry.anderson@example.com'),
        (8, 'henry.anderson@gmail.com'),
        (8, 'henry.anderson@yahoo.com'),
        (9, 'ivy.thomas@example.com'),
        (10, 'jack.taylor@example.com')
    ]
    cursor.executemany("INSERT INTO Contact (MEMBERID, EMAIL) VALUES (:1, :2)", contacts)
    connection.commit()
    print("Inserted records into 'Contact' table.")
    cursor.close()

def insert_borrowing(connection):
    # clear_table(connection, "Borrowing")
    cursor = connection.cursor()
    # today = datetime.date.today()
    borrowings = [ 
        (2, 'ISBN002', 1, datetime.date(2024, 1, 5), datetime.date(2024, 1, 10)),  
        (12, 'ISBN011', 2, datetime.date(2024, 2, 1), datetime.date(2024, 2, 15)),  
        (9, 'ISBN012', 3, datetime.date(2024, 3, 10), None), 
        (3, 'ISBN003', 4, datetime.date(2024, 4, 10), datetime.date(2024, 4, 30)),    
        (13, 'ISBN014', 5, datetime.date(2024, 5, 5), datetime.date(2024, 5, 20)),  
        (5, 'ISBN005', 6, datetime.date(2024, 5, 20), None),   
        (15, 'ISBN006', 7, datetime.date(2024, 6, 1), datetime.date(2024, 6, 5)),  
        (4, 'ISBN002', 8, datetime.date(2024, 6, 10), None), 
        (5, 'ISBN018', 9, datetime.date(2024, 7, 1), datetime.date(2024, 7, 10)), 
        (11, 'ISBN011', 10, datetime.date(2024, 8, 15), datetime.date(2024, 8, 22)),  
        (14, 'ISBN019', 11, datetime.date(2024, 8, 20), None),
        (2, 'ISBN011', 12, datetime.date(2024, 9, 11), None),   
        (1, 'ISBN007', 13, datetime.date(2024, 10, 5), None),   
        (5, 'ISBN014', 14, datetime.date(2024, 11, 10), datetime.date(2024, 11, 28))
    ]
    cursor.executemany("INSERT INTO Borrowing (MEMBERID, ISBN, BorrowID, BorrowDate, ReturnDate) VALUES (:1, :2, :3, :4, :5)", borrowings)
    connection.commit()
    print("Inserted records into 'Borrowing' table.")
    cursor.close()


# Similar functions can be created for AuthoredBy, BelongsToGenre, and PublishedBy tables.
def insert_authored_by(connection):
    # clear_table(connection, "AuthoredBy")
    cursor = connection.cursor()
    # Creating associations based on available Author and Book data
    authored_by = [
        (1, 'ISBN001'),
        (2, 'ISBN002'),
        (3, 'ISBN003'),
        (4, 'ISBN004'),
        (5, 'ISBN005'),
        (6, 'ISBN006'),
        (7, 'ISBN007'),
        (8, 'ISBN008'),
        (9, 'ISBN009'),
        (10, 'ISBN010'),
        (1, 'ISBN011'),
        (2, 'ISBN012'),
        (3, 'ISBN013'),
        (4, 'ISBN014'),
        (5, 'ISBN015'),
        (6, 'ISBN016'),
        (7, 'ISBN017'),
        (8, 'ISBN018'),
        (9, 'ISBN019'),
        (10, 'ISBN020')
    ]
    cursor.executemany("INSERT INTO AuthoredBy (AuthorID, ISBN) VALUES (:1, :2)", authored_by)
    connection.commit()
    print("Inserted records into 'AuthoredBy' table.")
    cursor.close()

def insert_belongs_to_genre(connection):
    # clear_table(connection, "BelongsToGenre")
    cursor = connection.cursor()
    # Associating each book with a genre
    belongs_to_genre = [
        ('ISBN001', 7),
        ('ISBN002', 5),
        ('ISBN003', 3),
        ('ISBN004', 4),
        ('ISBN005', 6),
        ('ISBN006', 6),
        ('ISBN007', 2),
        ('ISBN008', 8),
        ('ISBN009', 9),
        ('ISBN010', 10),
        ('ISBN011', 7),
        ('ISBN012', 10),
        ('ISBN013', 5),
        ('ISBN014', 1),
        ('ISBN015', 6),
        ('ISBN016', 4),
        ('ISBN017', 10),
        ('ISBN018', 1),
        ('ISBN019', 6),
        ('ISBN020', 3)
    ]
    cursor.executemany("INSERT INTO BelongsToGenre (ISBN, GenreID) VALUES (:1, :2)", belongs_to_genre)
    connection.commit()
    print("Inserted records into 'BelongsToGenre' table.")
    cursor.close()

def insert_published_by(connection):
    # clear_table(connection, "PublishedBy")
    cursor = connection.cursor()
    # Creating associations between books and publishers with publication year
    published_by = [
        ('ISBN001', 1, 2020),
        ('ISBN002', 1, 2019),
        ('ISBN003', 3, 2011),
        ('ISBN004', 4, 2018),
        ('ISBN005', 2, 2022),
        ('ISBN006', 1, 2013),
        ('ISBN007', 7, 2020),
        ('ISBN008', 8, 2017),
        ('ISBN009', 1, 2021),
        ('ISBN010', 10, 2019),
        ('ISBN011', 1, 2020),
        ('ISBN012', 2, 2019),
        ('ISBN013', 3, 2021),
        ('ISBN014', 1, 2018),
        ('ISBN015', 5, 2012),
        ('ISBN016', 6, 2003),
        ('ISBN017', 7, 2020),
        ('ISBN018', 8, 2017),
        ('ISBN019', 9, 2021),
        ('ISBN020', 10, 2020)
    ]
    cursor.executemany("INSERT INTO PublishedBy (ISBN, PublisherID, Year) VALUES (:1, :2, :3)", published_by)
    connection.commit()
    print("Inserted records into 'PublishedBy' table.")
    cursor.close()

# def insert_borrows(connection):
#     # clear_table(connection, "Borrows")
#     cursor = connection.cursor()
#     # Associating members with books they borrowed
#     borrows = [
#         (1, 'ISBN001'),
#         (2, 'ISBN002'),
#         (3, 'ISBN003'),
#         (4, 'ISBN004'),
#         (5, 'ISBN005'),
#         (6, 'ISBN006'),
#         (7, 'ISBN007'),
#         (8, 'ISBN008'),
#         (9, 'ISBN009'),
#         (10, 'ISBN010')
#     ]
#     cursor.executemany("INSERT INTO Borrows (MemberID, ISBN) VALUES (:1, :2)", borrows)
#     connection.commit()
#     print("Inserted records into 'Borrows' table.")
#     cursor.close()

# Running all association table insertions
def populate_association_tables(connection):
    insert_authored_by(connection)
    insert_belongs_to_genre(connection)
    insert_published_by(connection)
    # insert_borrows(connection)


# Running all insertions
def populate_database(connection):
    insert_authors(connection)
    insert_genres(connection)
    insert_books(connection)
    insert_publishers(connection)
    insert_members(connection)
    insert_contacts(connection)
    insert_borrowing(connection)
    # Add functions for other association tables here if needed

# Main function to create, clear, and insert into all tables
if __name__ == "__main__":
    connection = create_connection()
    if connection:
        clear_table(connection, 'author')
        clear_table(connection, 'genre')
        clear_table(connection, 'book')
        clear_table(connection, 'publisher')
        clear_table(connection, 'member')
        clear_table(connection, 'borrowing')
        clear_table(connection, 'contact')

        clear_table(connection, 'authoredby')
        clear_table(connection, 'belongstogenre')
        clear_table(connection, 'publishedby')
        # clear_table(connection, 'borrows')
        populate_database(connection)  # Insert main table data
        populate_association_tables(connection)  # Insert association table data
        connection.close()
