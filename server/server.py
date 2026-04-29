import os
import socket

from dotenv import load_dotenv
from psycopg2 import OperationalError

from database import Database
from metadata import load_registry
from query_engine import QueryEngine


load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))


def main():
    try:
        db_kien = Database(os.getenv("DATABASE_KIEN_URL"))
        db_alex = Database(os.getenv("DATABASE_ALEX_URL"))
    except OperationalError as e:
        print(f"Cannot connect to a database: {e}")
        return

    print("Connected to both Neon PostgreSQL databases.")
    registry = load_registry(db_kien, db_alex)
    print(f"Loaded device registry: {registry}")
    engine = QueryEngine(db_kien, db_alex, registry)

    while True:
        try:
            host = input("Enter server IP address to bind: ")
            port = int(input("Enter port number: "))
            srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind((host, port))
            srv.listen(5)
            print(f"Server listening on {host}:{port}")
            break
        except OSError as e:
            print(f"Cannot bind to that address ({e}). Try again.")

    try:
        while True:
            client_sock, addr = srv.accept()
            print(f"Connection from: {addr}")
            try:
                while True:
                    data = client_sock.recv(4096)
                    if not data:
                        break
                    query = data.decode("utf-8", errors="replace").strip()
                    print(f"Received query: {query!r}")
                    response = engine.handle(query)
                    client_sock.sendall(response.encode("utf-8"))
            except ConnectionResetError:
                pass
            finally:
                client_sock.close()
                print("Client disconnected. Waiting for next connection...")
    finally:
        srv.close()
        db_kien.close()
        db_alex.close()


if __name__ == "__main__":
    main()
