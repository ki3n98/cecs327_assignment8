import socket
import ipaddress

acceptable_queries = [
    "What is the average moisture inside our kitchen fridges in the past hours, week and month?",
    "What is the average water consumption per cycle across our smart dishwashers in the past hour, week and month?"
    "Which house consumed more electricity in the past 24 hours, and by how much?"
]

def validate_ip(ip):
    try:
        ipaddress.ip_address(ip)
        return True
    except ValueError:
        return False

def validate_port(port_str):
    try:
        port = int(port_str)
        return 1 <= port <= 65535
    except ValueError:
        return False

# Prompt user for server info
server_ip = input("Enter server IP address: ")
server_port = input("Enter server port number: ")

if not validate_ip(server_ip):
    print("Error: Invalid IP address.")
    exit()

if not validate_port(server_port):
    print("Error: Invalid port number.")
    exit()

server_port = int(server_port)

# Create TCP socket and connect to server
myTCPSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    myTCPSocket.connect((server_ip, server_port))
    print(f"Connected to server at {server_ip}:{server_port}")
except Exception as e:
    print(f"Error: Could not connect to server. {e}")
    exit()

print("\nSupported queries:")
for i, q in enumerate(acceptable_queries, 1):
    print(f"{i}. {q}")

# Send messages in a loop
while True:
    message = input("\nEnter your query (or 'quit' to exit): ")
    if message.lower() == 'quit':
        break
    if message not in acceptable_queries:
        print("Sorry, this query cannot be processed. Please try one of the supported queries.")
        continue
    # Send message to server
    myTCPSocket.sendall(message.encode('utf-8'))
    # Wait for server response
    response = myTCPSocket.recv(4096)
    print(f"\nServer response:\n{response.decode('utf-8')}")

myTCPSocket.close()
print("Connection closed.")
