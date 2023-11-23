# ------------------------------------
# Master node logic
# ------------------------------------
# Accounts (Obtener cuentas)
# sendToMaster(mode=C): MODE
# receiveFromMaster(mode=C): MODE-(ACCOUNTS-AMOUNTS)
#
# Transactions (Actualizar cuenta)
# sendToMater(mode=A): MODE-ID_CLIENT-ORIGIN_ACCOUNT-DESTINY_ACCOUNT-AMOUNT
# receiveFromMaster(mode=A): MODE-ID_CLIENT-ORIGIN_ACCOUNT-ORIGIN_AMOUNT-DESTINY_ACCOUNT-DESTINY_AMOUNT
#
# Miner (Hash anterior + hash de raiz)
# receiveFromMaster(mode=M): MODE-N_ZEROS-HASH_BLOCK_PREV-HASH_ROOT
#
# Validation (Recibir nuevo hash validado)
# sendToMaster(mode=V): MODE-TIME-N_ZEROS-NEW_HASH-PERCENTAGE
#
# Blockchain (Enviar bloque)
# receiveFromMaster(mode=B): MODE-ORIGIN_ACCOUNT-DESTINY_ACCOUNT-AMOUNT-HASH_BLOCK
#
# Error (Recibir error)
# receiveFromMaster(mode=F): MODE-ERROR


# ------------------------------------
# Slave node logic
# ------------------------------------
# Reads (Buscar una cuenta)
# sendToSlave(mode=L): MODE-ID_CLIENT-ORIGIN_ACCOUNT
# receiveFromSlave(mode=L): MODE-ID_CLIENT-ORIGIN_ACCOUNT-ORIGIN_AMOUNT
#
# Transactions (Actualizar cuenta)
# sendToAllSlave(mode=A): MODE-ORIGIN_ACCOUNT-ORIGIN_AMOUNT-DESTINY_ACCOUNT-DESTINY_AMOUNT
#
# Miner (Encontrar el nonce)
# sendToAllSlave(mode=M): MODE-N_ZEROS-HASH_BLOCK_PREV-HASH_ROOT
# receiveFromSlave(mode=M): MODE-NONCE-TIME-N_ZEROS-HASH_BLOCK_PREV-HASH_ROOT-HASH_TOTAL
#
# Validation (Validar hash con el nonce encontrado)
# sendToAllSlave(mode=V): MODE-NONCE-TIME-N_ZEROS-HASH_BLOCK_PREV-HASH_ROOT-HASH_TOTAL
# receiveFromSlave(mode=V): MODE-BOOLEAN
#
# Blockchain (Recibir bloque)
# sendToAllSlave(mode=B): MODE-ORIGIN_ACCOUNT-DESTINY_ACCOUNT-AMOUNT-HASH_BLOCK
#
# Error (Recibir error)
# receiveFromSlave(mode=F): MODE-ERROR


# ------------------------------------
# Needed libraries
# ------------------------------------
import socket
import random
import threading
import select
import time
import queue

# ------------------------------------
# Class to create a client
# ------------------------------------
class ClientConnection:

    # Constructor
    def __init__(self, host, port):
        self.host = host
        self.port = port

    # Connect to a node
    def connect(self):
        self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connection.connect((self.host, self.port))

    # Send a message to the node
    def send(self, message):
        # message = message + '\n'
        self.connection.send(message.encode('utf-8'))

    # Receive a message from the node
    def receive(self):
        return self.connection.recv(1024).decode('utf-8')

    # Close the connection with the node
    def close(self):
        self.connection.close()


# ------------------------------------
# Class to create a server
# ------------------------------------
class ServerCreation:

    # Constructor
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.server = None

    # Listen for incoming connections
    def listen(self, queue_size):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind((self.host, self.port))
        self.server.listen(queue_size)

    # Accept a connection from a client
    def accept(self):
        self.connection, self.address = self.server.accept()
        return self.connection, self.address

    # Send a message to the client
    def send(self, message):
        # message = message + '\n'
        self.connection.send(message.encode('utf-8'))

    # Receive a message from the client
    def receive(self):
        return self.connection.recv(1024).decode('utf-8')

    # Close the connection with the client
    def close(self):
        self.connection.close()

    def fileno(self):
        return self.server.fileno()


# ------------------------------------
# Configuration of the nodes
# ------------------------------------
master_node = {'address':'localhost', 'port':40000}
slave_nodes = [
    {'address':'localhost', 'port':50000}
]
forwarder_node = {'address':'localhost', 'port':60000, 'queue_size':1}


# ------------------------------------
# Queues to handle the messages
# ------------------------------------
queue_reads = queue.Queue()
queue_transactions = queue.Queue()
queue_response = queue.Queue()
clients = {}

# ------------------------------------
# Function to get a random index
# ------------------------------------
# Function receives a size and returns a random index
def get_random_index(size):
    return random.randint(0, size - 1)


# ------------------------------------
# Function to send a message to all slaves
# ------------------------------------
# Function receives a message and a list of connections
# and sends the message to all the connections slaves
def send_all_slaves(message, list_slaves):
    # Get the size of the list of slaves
    size = len(list_slaves)
    # Send the message to all the slaves
    for i in range(size):
        list_slaves[i].send(message)
        print(f"Mensaje enviado al nodo Slave: {str(slave_nodes[i])}")

# ------------------------------------
# Function to find a client socket
# ------------------------------------
def find_client_socket(clients, user_ip, user_port):
    for client_socket, address_client in clients.items():
        if address_client[0] == user_ip and address_client[1] == user_port:
            return client_socket
    return None


# ------------------------------------
# Function to handle the communication with the master node
# ------------------------------------
# Function receives a master connection and a list of connections   
# and handles the communication with the master node
def handle_master_response(master_connection, list_slaves):
    # Loop to receive messages from the master node
    while True:
        # Get message from the master node
        response = master_connection.receive()
        # If the message is not empty
        if response:
            # Master node sends the accounts
            if response.startswith("C"):
                print("[C] Se recibió la lista de cuentas...")
                send_all_slaves(response, list_slaves)
            # Master node sends a transaction
            elif response.startswith("A"):
                print("[A] Se recibió una transacción...")
                queue_response.put(response)
                aux = response.split('-')
                response = f"{aux[0]}-{aux[2]}-{aux[3]}-{aux[4]}"
                send_all_slaves(response, list_slaves)
            # Master node sends the hash
            elif response.startswith("M"):
                print("[M] Se recibió hash anterior y hash de raiz...")
                send_all_slaves(response, list_slaves)
            # Master node sends the validation
            elif response.startswith("B"):
                print("[B] Se recibió un bloque...")
                send_all_slaves(response, list_slaves)
            # Master node sends an error
            elif response.startswith("F"):
                print("[F] Se recibió un error...")
            # Unknown message
            else:
                print(f"Respuesta desconocida: {response}")


# ------------------------------------
# Function to handle the communication with the slave node
# ------------------------------------
# Function receives a slave connection, a master connection and a list of connections
# and handles the communication with the slave node
def handle_slave_response(slave_connection, master_connection, list_slaves):
    message_saved = ''
    # Loop to receive messages from the slave node
    while True:
        # Receive message from the slave node
        response = slave_connection.receive()
        # If the message is not empty
        if response:
            # Slave node sends the account
            if response.startswith("L"):
                print("[L] Se encontro un usuario...")
                queue_response.put(response)
            # Slave node sends a transaction
            elif response.startswith("M"):
                print("[M] Se encontró el nonce...")
                # Send the message to all the slaves with mode V
                send_all_slaves(response.replace("M", "V", 1), list_slaves)
            # Slave node sends the validation
            elif response.startswith("V"):
                print("[V] Un nodo slave valido el hash ...")
                print(f"El slave envio: {response.split('-')[1]}")
                if response.split('-')[1] == 'True':
                    master_connection.send(message_saved)
                    message_saved = ''
            # Slave node sends the hash
            elif response.startswith("F"):
                print("[F] Se recibió un error...")
            # Unknown message
            else:
                print(f"Respuesta desconocida: {response}")

# ------------------------------------
# Function to handle the queues
# ------------------------------------
# Function receives two queues, a master connection and a list of connections
# and handles the queues to send messages to the master and slave nodes
def handle_queues(master_connection, slaves_list):
    # Loop to check if the queues are not empty
    while True:
        # Verify if the queue of reads is not empty
        if not queue_reads.empty():
            # Get message from the queue of reads
            message_from_client = queue_reads.get()
            # Choose a random slave node
            random_index = get_random_index(len(slaves_list))
            # Send message to the random slave node
            slaves_list[random_index].send(message_from_client)
        # Verify if the queue of transactions is not empty
        if not queue_transactions.empty():
            # Get message from the queue of transactions
            message_from_client = queue_transactions.get()
            # Send message to the master node
            master_connection.send(message_from_client)
        if not queue_response.empty():
            # Get message from the queue of responses
            message_to_client = queue_response.get()
            print(f"Usuario: {message_to_client.split('-')[2]} - Balance: {message_to_client.split('-')[3]}")
            # Get the ip and port of the client
            user_id = message_to_client.split('-')[1]
            user_ip, user_port = user_id.split(':')
            # Find the client socket corresponding to the user's IP and port
            client_socket = find_client_socket( clients, user_ip, int(user_port))
            # If the client socket was found
            if client_socket:
                # Send message to the client
                client_socket.send(message_to_client.encode('utf-8'))
                print(f"Mensaje enviado al cliente: {message_to_client}")
            else:
                print(f"No se encontró el cliente correspondiente a {user_id}")

        # Wait 100 ms to check again
        time.sleep(0.1)

# ------------------------------------
# Function main
# ------------------------------------
def main():
    # ---------- Master node connection ----------

    # Create a socket to connect to the master node
    master_connection = ClientConnection(master_node['address'], master_node['port'])

    # Connect to the master node
    master_connection.connect()
    print(f"Conectado al nodo Master {str(master_node)}")

    # ---------- Slave nodes connection ----------

    # Get the size of the list of slaves
    size = len(slave_nodes)

    # Create a list of connections to the slave nodes
    slaves_list = []

    # Connect to all the slave nodes
    for i in range(size):
        # Create a socket to connect to the slave node
        slave_connection = ClientConnection(slave_nodes[i]['address'], slave_nodes[i]['port'])

        # Connect to the slave node
        slave_connection.connect()
        print(f"Conectado al nodo Slave: {str(slave_nodes[i])}")
        
        # Add the connection to the list of connections
        slaves_list.append(slave_connection)

    # ---------- Send accounts to all slaves ----------

    # Execute mode C to get the accounts
    master_connection.send("C")

    # ---------- Threads creation ----------

    # Create a thread to handle the communication with the master node
    response_thread_master = threading.Thread(target=handle_master_response, args=(master_connection,slaves_list))
    response_thread_master.start()

    # Create a thread to handle the communication with the slave nodes
    for i in range(size):
        response_thread_slave = threading.Thread(target=handle_slave_response, args=(slaves_list[i],master_connection,slaves_list))
        response_thread_slave.start()

    # Create a thread to handle the queues
    queue_thread = threading.Thread(target=handle_queues, args=(master_connection, slaves_list))
    queue_thread.start()

    # ---------- Server Forwarder creation ----------

    # Create a server to listen for incoming connections
    server_forwarder = ServerCreation(forwarder_node['address'], forwarder_node['port'])
    server_forwarder.listen(forwarder_node['queue_size'])
    print("Esperando conexiones entrantes...")

    # List of sockets for select.select()
    sockets_list = [server_forwarder]

    # ---------- Client nodes connection ----------

    while True:
        # Wait for an activity to happen
        read_sockets, _, _ = select.select(sockets_list, [], [])

        for sock in read_sockets:
            # New connection
            if sock == server_forwarder:
                # Accept the connection from the client
                client_socket, address_client = server_forwarder.accept()
                # Add the connection to the list of sockets
                sockets_list.append(client_socket)
                # Add the client to the list of clients
                clients[client_socket] = address_client
                print(f"Conexión establecida desde {address_client}")

            # Message from a client
            else:
                try:
                    # Receive message from the client
                    message = sock.recv(1024).decode("utf-8")
                    if message:
                        if message.startswith("L"):
                            print(f"Procesar modo L: {message}")
                            # Add message to the queue of reads
                            queue_reads.put(message)
                        elif message.startswith("A"):
                            print(f"Procesar modo A: {message}")
                            # Add message to the queue of transactions
                            queue_transactions.put(message)
                        else:
                            # Message unknown
                            print(f"Respuesta desconocida: {message}")
                except Exception as e:
                    print(f"Cliente {clients[sock]} desconectado...")
                    sock.close()
                    sockets_list.remove(sock)
                    del clients[sock]
                    continue

if __name__ == "__main__":
    main()

# python3 main.py