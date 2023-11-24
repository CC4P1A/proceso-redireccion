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
import time
import queue
import asyncio
import websockets
import json
import uuid

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
        self.connection.sendall(message.encode('utf-8'))

    # Receive a message from the node
    def receive(self):
        return self.connection.recv(1024).decode('utf-8')

    # Close the connection with the node
    def close(self):
        self.connection.close()


# ------------------------------------
# Configuration of the nodes
# ------------------------------------
master_node = {'address':'localhost', 'port':40000}
slave_nodes = [
    {'address':'localhost', 'port':50000}
]
forwarder_node = {'address':'localhost', 'port':60000}


# ------------------------------------
# Queues to handle the messages
# ------------------------------------
queue_reads = queue.Queue()
queue_transactions = queue.Queue()
queue_response = queue.Queue()
queries_dict = []
validation = []

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
def find_client_socket(query_id):
    for index in range(len(queries_dict)):
        if queries_dict[index]['id'] == int(query_id):
            return queries_dict[index]['client']
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
                response = "-".join([aux[0]] + aux[2:])
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
                print(f"Respuesta desconocida: {response} desde el nodo Master: {str(master_connection)}")


# ------------------------------------
# Function to handle the communication with the slave node
# ------------------------------------
# Function receives a slave connection, a master connection and a list of connections
# and handles the communication with the slave node
def handle_slave_response(slave_connection, master_connection, list_slaves):
    nonce_found = ''
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
                # V-nonce-tiempo-nroCeros-hashAnt-hashRaiz-HashTotal
                nonce_found = response.replace("M", "V", 1)
                arr = nonce_found.split('-')
                new_message = "-".join(arr[:2] + arr[3:])
                send_all_slaves(new_message, list_slaves)
            # Slave node sends the validation
            elif response.startswith("V"):
                print("[V] Un nodo slave valido el hash ...")
                validation_response = response.split('-')[1]
                validation.append(validation_response)
                if len(validation) == len(list_slaves):
                    print("Se valido el nonce...")
                    #percentage = validation.count('true')/len(validation)
                    # V-nonce-tiempo-nroCeros-hashAnt-hashRaiz-HashTotal\r\n-porcentaje
                    nonce_found = nonce_found.replace("\r\n", f"-{1}\r\n")
                    master_connection.send(nonce_found)
                    validation.clear()
                    nonce_found=''
            # Slave node sends the hash
            elif response.startswith("F"):
                queue_response.put(response)
                print("[F] Se recibió un error...")
            # Unknown message
            else:
                print(f"Respuesta desconocida: {response} desde el nodo Slave: {str(slave_connection)}")


def get_unique_id():
    return uuid.uuid4().int & (1<<16)-1


# ------------------------------------
# Function handle client (WebSocket)
# ------------------------------------
async def handle_client(websocket):
    # Add your WebSocket handling logic here
    print(f"Cliente conectado desde {websocket.remote_address}")
    # Loop to receive messages from the client
    async for message in websocket:
        try:
            # Process the received message as needed
            data = json.loads(message)
            if data['type'] == 'L':
                # Extract the necessary information
                mode = data['type']
                origin_account = data['origin_account']
                query_id = get_unique_id()
                # Create the query read
                read = f"{mode}-{query_id}-{origin_account}\n"
                print(f"Recibe id_solicitud: {query_id} de lectura para la cuenta {origin_account}")
                # Add the query to the list of queries
                queries_dict.append({'id': query_id, 'client': websocket})
                # Add message to the queue of reads
                queue_reads.put(read)
            elif data['type'] == 'A':
                # Extract the necessary information
                mode = data['type']
                origin_account = data['origin_account']
                destination_account = data['destination_account']
                amount = data['amount']
                query_id = get_unique_id()
                # Create the query transaction
                transaction = f"{mode}-{query_id}-{origin_account}-{destination_account}-{amount}\n"
                print(f"Recibe transaccion id: {query_id} monto: {amount} acc_from:{origin_account} hacia acc_to:{destination_account}")
                # Add the query to the list of queries
                queries_dict.append({'id': query_id, 'client': websocket})
                # Add message to the queue of transactions
                queue_transactions.put(transaction)
            else:
                # Message unknown
                print(f"Respuesta desconocida: {data} desde el cliente: {websocket.remote_address}")
        except Exception as e:
            print(f"Error al procesar el mensaje: {e}")


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
            print(f"Se envio la consulta al eslave[{random_index}]")
        # Verify if the queue of transactions is not empty
        if not queue_transactions.empty():
            # Get message from the queue of transactions
            message_from_client = queue_transactions.get()
            # Send message to the master node
            master_connection.send(message_from_client)
            print(f"Se envio la consulta de actualizacion al master")
        # Wait 100 ms to check again
        time.sleep(0.1)

async def handle_response_client():
    # Loop to check if the queues are not empty
    while True:
        if not queue_response.empty():
            # Get message from the queue of responses
            message_to_client = queue_response.get()
            # Get the ip and port of the client
            query_id = message_to_client.split('-')[1]
            print(f"Se encontro una respuesta para id_solicitud: {query_id}...")
            # Find the client socket corresponding to the user's IP and port
            client_websocket = find_client_socket(query_id)
            # If the client socket was found
            if client_websocket:
                # Send message to the client
                mode = message_to_client.split('-')[0]
                json_response = ''
                if mode == 'L':
                    id_client = message_to_client.split('-')[1]
                    origin_account = message_to_client.split('-')[2]
                    origin_amount = message_to_client.split('-')[3].replace('\r\n', '')
                    json_response = json.dumps({'type': mode, 'id': id_client, 'origin_account': origin_account, 'origin_amount': origin_amount})
                elif mode == 'A':
                    id_client = message_to_client.split('-')[1]
                    origin_account = message_to_client.split('-')[2]
                    origin_amount = message_to_client.split('-')[3]
                    destination_account = message_to_client.split('-')[4]
                    destination_amount = message_to_client.split('-')[5].replace('\r\n', '')
                    json_response = json.dumps({'type': mode, 'id': id_client, 'origin_account': origin_account, 'origin_amount': origin_amount, 'destination_account': destination_account, 'destination_amount': destination_amount})
                elif mode == 'F':
                    error = message_to_client.split('-')[1].replace('\r\n', '')
                    json_response = json.dumps({'type': mode, 'error': error})
                await client_websocket.send(json_response)
                print("Mensaje enviado al cliente")
            else:
                print(f"No se encontró el cliente asignado para la solicitud {query_id}")
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

    # ---------- Threads creation ----------
    # Iniciar un bucle asyncio
    loop = asyncio.get_event_loop()

    # Crear y ejecutar un hilo para la función asyncio
    asyncio_thread = threading.Thread(target=run_asyncio_loop, args=(loop,))
    asyncio_thread.start()

    response_thread_master = threading.Thread(target=handle_master_response, args=(master_connection,slaves_list))
    response_thread_master.start()

    for i in range(len(slaves_list)):
        response_thread_slave = threading.Thread(target=handle_slave_response, args=(slaves_list[i], master_connection, slaves_list))
        response_thread_slave.start()

    queue_thread = threading.Thread(target=handle_queues, args=(master_connection, slaves_list))
    queue_thread.start()
    


def run_asyncio_loop(loop):
    loop.run_until_complete(main_server_forwarder())


async def main_server_forwarder():

    async with websockets.serve(handle_client, forwarder_node['address'], forwarder_node['port']):
        print(f"WebSocket Server Forwarder escuchando: {forwarder_node}")
        await asyncio.Future()

if __name__ == "__main__":
    main()
    asyncio.run(handle_response_client())

# python main.py