README:

Full instructions of how to run the system:

1.) Construct a .env file inside the server folder with both database URLs
    Ex) DATABASE_KIEN_URL="URL"
        DATABASE_ALEX_URL="URL"
2.) In terminal use the command "cd server" in order to navigate to the server folder as the working directory.
3.) In cd server, employ the command "python3 server.py" if on mac, and if on windows, employ "python server.py".
4.) The system will prompt you to "Enter server IP address to bind:", enter 127.0.0.1
5.) The system will then ask you to "Enter port number:", enter 5000
6.) The server will then claim, "Server listening on 127.0.0.1:5000"
7.) At this point, open a new terminal, and employ the command "python3 client.py" if on mac, and if on windows, employ "python client.py".
8.) The system will prompt you to "Enter server IP address:", enter 127.0.0.1
9.) The system will then prompt you to "Enter server port number:", enter 5000
10.) At this point on the server terminal, it will claim "Connection from: ('127.0.0.1', 50207)".
11.) On the client side it will claim 

"Supported queries:
   What is the average moisture inside our kitchen fridges in the past hours, week and month?
   What is the average water consumption per cycle across our smart dishwashers in the past hour, week and month?
   Which house consumed more electricity in the past 24 hours, and by how much?
   Enter your query (or 'quit' to exit):"

At this point enter 1, 2, 3, or "quit" to exit and the server terminal will receive the appropriate query or if quit is selected, the connection is closed
and the terminals will announce this closed connection.

EXPLANATIONS:

