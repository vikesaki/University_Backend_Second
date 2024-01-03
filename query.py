import sqlite3
import pandas as pd

# Connect to SQLite database
conn = sqlite3.connect("ticketing_database.db")
connect = sqlite3.connect("ticketing_database.db")

conn.commit()
cursor = conn.cursor()
delCursor = connect.cursor()

# 1 - finding each ticket data where the price higher than 150
print("1 - finding each ticket data where the price higher than 150\n")
task1 = pd.read_sql('''
SELECT 
    tickets.ticket_id,
    tickets.seat_number,
    tickets.price,
    users.username,
    events.event_name,
    events.event_date
FROM 
    tickets
JOIN transactions ON tickets.transaction_id = transactions.transaction_id
JOIN users ON transactions.user_id = users.user_id
JOIN events ON transactions.event_id = events.event_id
WHERE 
    tickets.price > 150
ORDER BY 
    tickets.price DESC;
''', conn)
print(task1)
print('\n')

# 2 - finding the buyer with the most ticket bought
print("2 - finding the buyer with the most ticket bought\n")
task2 = pd.read_sql('''
SELECT 
    GROUP_CONCAT(tickets.ticket_id, ', ') AS tickets,
    GROUP_CONCAT(tickets.transaction_id, ', ') AS transactions_id,
    users.username,
    COUNT(tickets.ticket_id) AS tickets_bought,
    COUNT(tickets.transaction_id) AS transactions_done
FROM 
    tickets
JOIN transactions ON tickets.transaction_id = transactions.transaction_id
JOIN users ON transactions.user_id = users.user_id
GROUP BY 
    users.user_id
ORDER BY 
    tickets_bought DESC
LIMIT 1;
''', conn)
print(task2)
print('\n')

# 3 - count number of transaction of each user who bought at least 1 tickets
print("3 - count number of transaction of each user who bought at least 1 tickets")
task3 = pd.read_sql('''
SELECT 
    users.user_id,
    users.username,
    COUNT(transactions.transaction_id) AS transaction_count
FROM 
    users
LEFT JOIN transactions ON users.user_id = transactions.user_id
GROUP BY 
    users.user_id, users.username
HAVING
    transaction_count >= 1
ORDER BY 
    transaction_count DESC;
''', conn)
print(task3)
print('\n')

# 4 - count the average of tickets for each events
print("4 - count the average of tickets for each events")
task4 = pd.read_sql('''
SELECT 
    events.event_id,
    events.event_name,
    AVG(tickets.price) AS avg_ticket_price
FROM 
    events
LEFT JOIN transactions ON events.event_id = transactions.event_id
LEFT JOIN tickets ON transactions.transaction_id = tickets.transaction_id
GROUP BY 
    events.event_id, events.event_name
HAVING
    avg_ticket_price >= 1
ORDER BY 
    avg_ticket_price DESC;
''', conn)
print(task4)
print('\n')

# 5 - find the buyer of certain event tickets"
print("5 - find the buyer of certain event tickets")
task5 = pd.read_sql('''
SELECT
    user_id,
    username
FROM
    users
WHERE
    user_id IN (
        SELECT
            DISTINCT transactions.user_id
        FROM
            transactions
        WHERE
            transactions.event_id = 2
    );
''', conn)
print(task5)
print('\n')

# 6 - event with highest revenue"
print("6 - event with highest revenue")
task5 = pd.read_sql('''
WITH EventRevenues AS (
    SELECT
        events.event_id,
        events.event_name,
        SUM(tickets.price) AS total_revenue
    FROM
        events
    LEFT JOIN transactions ON events.event_id = transactions.event_id
    LEFT JOIN tickets ON transactions.transaction_id = tickets.transaction_id
    GROUP BY
        events.event_id, events.event_name
)
SELECT
    event_id,
    event_name,
    total_revenue
FROM
    EventRevenues
ORDER BY
    total_revenue DESC
LIMIT 1;
''', conn)
print(task5)
print('\n')

# add new event
print("\n7 - add new event")
event_name = 'HoYoFest'
venue_id = 1
event_date = '2024-01-03 18:00:00'

try:
    delCursor.execute('''
    INSERT INTO events 
    (event_name, venue_id, event_date) 
    VALUES (?, ?, ?)
    ''', (event_name, venue_id, event_date))

    connect.commit()
    print("New event added successfully.")

except sqlite3.Error as e:
    # Handle any potential errors
    print(f"Error: {e}")

result = pd.read_sql('''
SELECT * FROM events;
''', connect)
print()
print(result)

delCursor.execute('''
    DELETE FROM events
    WHERE event_name = ?
    ''', (event_name,))

connect.commit()

# delete a ticket and their transaction data
print("\n8 - delete a ticket and their transaction data")
ticket_id_to_delete = 15
try:
    delCursor.execute('''
    DELETE FROM transactions 
    WHERE transaction_id 
    IN (SELECT transaction_id 
        FROM tickets 
        WHERE ticket_id = ?)
    ''', (ticket_id_to_delete,))

    delCursor.execute('''
    DELETE FROM tickets 
    WHERE ticket_id = ?
    ''', (ticket_id_to_delete,))

    connect.commit()
    print(f"Ticket with ID {ticket_id_to_delete} and associated transaction data deleted successfully.")

except sqlite3.Error as e:
    print(f"Error: {e}")

result1 = pd.read_sql('''
SELECT * FROM tickets;
''', connect)
print()
print(result1)
result2 = pd.read_sql('''
SELECT * FROM transactions;
''', connect)
print()
print(result2)
connect.close()
