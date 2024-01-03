import sqlite3
from faker import Faker
import random

# Create a Faker instance to generate random names and emails
fake = Faker()

# Connect to SQLite database (or create if not exists)
conn = sqlite3.connect("ticketing_database.db")

# Create a cursor object to execute SQL commands
cursor = conn.cursor()

# Drop existing tables if they exist
cursor.execute("DROP TABLE IF EXISTS transactions")
cursor.execute("DROP TABLE IF EXISTS tickets")
cursor.execute("DROP TABLE IF EXISTS events")
cursor.execute("DROP TABLE IF EXISTS venues")
cursor.execute("DROP TABLE IF EXISTS users")

# Function to print table data
def print_table(table_name):
    print(f"\n{table_name} Table:")
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    for row in rows:
        print(row)

# Create and populate 'users' table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT NOT NULL,
        email TEXT NOT NULL
    )
''')

total_user = 20
users_data = [
    (i, fake.user_name(), fake.email())
    for i in range(1, total_user + 1)
]
cursor.executemany('INSERT INTO users (user_id, username, email) VALUES (?, ?, ?)', users_data)
print_table('users')

# Create and populate 'venues' table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS venues (
        venue_id INTEGER PRIMARY KEY,
        venue_name TEXT NOT NULL,
        location TEXT NOT NULL
    )
''')

venues_data = [
    (1, 'Venue A', 'City A'),
    (2, 'Venue B', 'City B'),
    (3, 'Venue C', 'City C'),
    (4, 'Venue D', 'City D'),
    (5, 'Venue E', 'City E'),
]
cursor.executemany('INSERT INTO venues (venue_id, venue_name, location) VALUES (?, ?, ?)', venues_data)
print_table('venues')

# Create and populate 'events' table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS events (
        event_id INTEGER PRIMARY KEY,
        event_name TEXT NOT NULL,
        venue_id INTEGER,
        event_date DATE,
        FOREIGN KEY (venue_id) REFERENCES venues(venue_id)
    )
''')

events_data = [
    (1, 'Concert 1', random.randint(1, 5), '2025-01-10'),
    (2, 'Sports Event 1', random.randint(1, 5), '2025-02-15'),
    (3, 'Conference A', random.randint(1, 5), '2025-03-20'),
    (4, 'Movie Night', random.randint(1, 5), '2025-04-25'),
    (5, 'Exhibition X', random.randint(1, 5), '2025-05-30'),
    (6, 'Theater Play Y', random.randint(1, 5), '2025-06-15'),
    (7, 'Music Festival', random.randint(1, 5), '2025-07-20'),
    (8, 'Comedy Show', random.randint(1, 5), '2025-08-25'),
    (9, 'Dance Performance', random.randint(1, 5), '2025-09-10'),
    (10, 'Art Expo', random.randint(1, 5), '2025-10-15'),
]

cursor.executemany('INSERT INTO events (event_id, event_name, venue_id, event_date) VALUES (?, ?, ?, ?)', events_data)
print_table('events')

# Create and populate 'transactions' table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        event_id INTEGER,
        transaction_date DATETIME,
        FOREIGN KEY (user_id) REFERENCES users(user_id),
        FOREIGN KEY (event_id) REFERENCES events(event_id)
    )
''')

total_transaction = 30
transactions_data = [
    (i, random.randint(1, 5), random.randint(1, total_user + 1), f'2024-01-{10 + i} 08:30:00')
    for i in range(1, 30)
]
cursor.executemany('INSERT INTO transactions (transaction_id, user_id, event_id, transaction_date) VALUES (?, ?, ?, ?)', transactions_data)
print_table('transactions')

# Create 'tickets' table with a connection to transactions
cursor.execute('''
    CREATE TABLE IF NOT EXISTS tickets (
        ticket_id INTEGER PRIMARY KEY,
        seat_number TEXT NOT NULL,
        price REAL NOT NULL,
        transaction_id INTEGER,
        FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id)
    )
''')
tickets_data = [
    (i, f'Seat{i}', random.randint(10, 300), i)
    for i in range(1, total_transaction)
]
cursor.executemany('INSERT INTO tickets (ticket_id, seat_number, price, transaction_id) VALUES (?, ?, ?, ?)', tickets_data)

print_table('tickets')
conn.commit()
conn.close()
