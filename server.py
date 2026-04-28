DATABASE_KIEN_URL = "postgresql://neondb_owner:npg_4KUPzRgMGrQ0@ep-morning-sunset-akkk8uok-pooler.c-3.us-west-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
DATABASE_ALEX_URL = "postgresql://neondb_owner:npg_6VJkNhPnS3Bb@ep-wispy-glade-anenagz2-pooler.c-6.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

import psycopg2

try:
    kien_con = psycopg2.connect(DATABASE_KIEN_URL)
    kien_cursor = kien_con.cursor()
    print("Connected to Kien's database successfully")
except Exception as e:
    print(f"Failed to connect to Kien's database: {e}")
    kien_con = None
    kien_cursor = None

try:
    alex_con = psycopg2.connect(DATABASE_ALEX_URL)
    alex_cursor = alex_con.cursor()
    print("Connected to Alex's database successfully")
except Exception as e:
    print(f"Failed to connect to Alex's database: {e}")
    alex_con = None
    alex_cursor = None

import socket

# Prompt for port (no hardcoding)
server_port = int(input("Enter port number to listen on: "))

# Create TCP socket, bind, and listen
myTCPSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
myTCPSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
myTCPSocket.bind(('', server_port))
myTCPSocket.listen(5)
print(f"Server listening on port {server_port}...")

# Accept one client connection
incomingSocket, incomingAddress = myTCPSocket.accept()
print(f"Connected by {incomingAddress}")

# Receive and echo messages in a loop
while True:
    data = incomingSocket.recv(1024)
    if not data:
        break  # Client disconnected
    message = data.decode('utf-8')
    print(f"Received: {message}")
    upper_message = message.upper()
    incomingSocket.sendall(upper_message.encode('utf-8'))

incomingSocket.close()
myTCPSocket.close()
print("Connection closed.")
