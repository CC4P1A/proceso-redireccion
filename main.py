# ------------------------------------
# Master node logic
# ------------------------------------
# Accounts (Obtener cuentas)
# sendToMaster(mode=C): MODE
# receiveFromMaster(mode=C): MODE-(ACCOUNTS-AMOUNTS)
#
# Transactions (Actualizar cuenta)
# sendToMater(mode=A): MODE-ID_CLIENT-ORIGIN_ACCOUNT-DESTINY_ACCOUNT-AMOUNT
# receiveFromMaster(mode=A): MODE-ORIGIN_ACCOUNT-ORIGIN_AMOUNT-DESTINY_ACCOUNT-DESTINY_AMOUNT
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
# receiveFromSlave(mode=L): MODE-ORIGIN_ACCOUNT-ORIGIN_AMOUNT
#
# Transactions (Actualizar cuenta)
# sendToSlave(mode=A): MODE-ORIGIN_ACCOUNT-ORIGIN_AMOUNT-DESTINY_ACCOUNT-DESTINY_AMOUNT
#
# Miner (Encontrar el nonce)
# sendToSlave(mode=M): MODE-N_ZEROS-HASH_BLOCK_PREV-HASH_ROOT
# receiveFromSlave(mode=M): MODE-NONCE-TIME-N_ZEROS-//HASH_BLOCK_PREV-HASH_ROOT//-HASH_TOTAL <- se necesita los hashes anteriores para validar el hash
#
# Validation (Validar hash con el nonce encontrado)
# sendToSlave(mode=V): MODE-NONCE-TIME-N_ZEROS-HASH_BLOCK_PREV-HASH_ROOT-HASH_TOTAL
# receiveFromSlave(mode=V): MODE-BOOLEAN
#
# Blockchain (Recibir bloque)
# sendToSlave(mode=B): MODE-ORIGIN_ACCOUNT-DESTINY_ACCOUNT-AMOUNT-HASH_BLOCK
#
# Error (Recibir error)
# receiveFromSlave(mode=F): MODE-ERROR


# ------------------------------------
# Imports
# ------------------------------------
import socket
import random
import threading
import time


# ------------------------------------
# Class Client Connection
# ------------------------------------
class ClientConnection:

    # Constructor
    def __init__(self, host, port):
        self.host = host
        self.port = port

    # Metodo connect para conectarse al nodo
    def connect(self):
        self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connection.connect((self.host, self.port))

    # Metodo send para enviar un mensaje al nodo
    def send(self, message):
        self.connection.send(message.encode('utf-8'))

    # Metodo receive para recibir un mensaje del nodo
    def receive(self):
        return self.connection.recv(1024).decode('utf-8')

    # Metodo close para cerrar la conexión con el nodo
    def close(self):
        self.connection.close()


# ------------------------------------
# Class Server Creation
# ------------------------------------
class ServerCreation:

    # Constructor
    def __init__(self, host, port):
        self.host = host
        self.port = port

    # Metodo listen para esperar conexiones entrantes
    def listen(self, queue_size):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind((self.host, self.port))
        self.server.listen(queue_size)

    # Metodo accept para aceptar una conexión entrante
    def accept(self):
        self.connection, self.address = self.server.accept()
        return self.connection, self.address

    # Metodo send para enviar un mensaje al cliente
    def send(self, message):
        self.connection.send(message.encode('utf-8'))

    # Metodo receive para recibir un mensaje del cliente
    def receive(self):
        return self.connection.recv(1024).decode('utf-8')

    # Metodo close para cerrar la conexión con el cliente
    def close(self):
        self.connection.close()


# ------------------------------------
# Variables globales
# ------------------------------------
master_node = {'address':'localhost', 'port':40000}
slave_nodes = [
    {'address':'localhost', 'port':50000}
]


# ------------------------------------
# Funcion para obtener un indice aleatorio
# ------------------------------------
def get_random_index(size):
    return random.randint(0, size - 1)


# ------------------------------------
# Function send all slaves message
# ------------------------------------

def send_all_slaves(message, list_slaves_connections):
    size = len(list_slaves_connections)
    for i in range(size):
        list_slaves_connections[i].send(message)
        print(f"Mensaje enviado al nodo Slave: {str(slave_nodes[i])}")


# ------------------------------------
# Funcion para manejar la comunicación del nodo Master
# ------------------------------------
def handle_master_response(master_connection, list_slaves_connections):
    # Bucle infinito para recibir mensajes del nodo Master
    while True:
        # Recibir mensaje del nodo Master
        response = master_connection.receive()
        if response:
            if response[0] == 'C':
                print("[C] Se recibió la lista de cuentas...")
                send_all_slaves(response, list_slaves_connections)
            elif response[0] == 'A':
                print("[A] Se recibió una transacción...")
                send_all_slaves(response, list_slaves_connections)
            elif response[0] == 'M':
                print("[M] Se recibió hash anterior y hash de raiz...")
                send_all_slaves(response, list_slaves_connections)
            elif response[0] == 'B':
                print("[B] Se recibió un bloque...")
                send_all_slaves(response, list_slaves_connections)
            elif response[0] == 'F':
                print("[F] Se recibió un error...")
            else:
                print(f"Respuesta desconocida: {response}")


# ------------------------------------
# Funcion para manejar la comunicación del nodo Slave
# ------------------------------------
def handle_slave_response(slave_connection, master_connection, list_slaves_connections):
    message_saved = ''
    # Bucle infinito para recibir mensajes del nodo Slave
    while True:
        # Recibir mensaje del nodo Slave
        response = slave_connection.receive()
        if response:
            if response[0] == 'L':
                print("[L] Se encontro un usuario...")
                print(f"Usuario: {response.split('-')[1]} - Balance: {response.split('-')[2]}")
            elif response[0] == 'M':
                print("[M] Se encontró el nonce...")
                message_saved = 'V' + response[1:]
                send_all_slaves(message_saved, list_slaves_connections)
            elif response[0] == 'V':
                print("[V] Un nodo slave valido el hash ...")
                print(f"El slave envio: {response.split('-')[1]}")
                if response.split('-')[1] == 'True':
                    master_connection.send(message_saved)
                    message_saved = ''
            elif response[0] == 'F':
                print(f"Procesar modo F: {response}")
                print("[F] Se recibió un error...")
            else:
                print(f"Respuesta desconocida: {response}")


# ------------------------------------
# Funcion para manejar la comunicación con el nodo Client
# ------------------------------------
def handle_client(client_socket, address_client, master_connection, list_slaves_connections):
    print(f"Conexión aceptada desde {address_client}")

    while True:
        message_from_client = client_socket.recv(1024).decode('utf-8')
        if message_from_client:
            # Cliente solicita obtener monto de una cuenta
            if message_from_client[0] == 'L':
                print(f"Procesar modo L: {message_from_client}")
                # Elegir un nodo Slave aleatorio
                random_index = get_random_index(len(list_slaves_connections))
                # Enviar mensaje al nodo Slave aleatorio
                list_slaves_connections[random_index].send(message_from_client)
            elif message_from_client[0] == 'A':
                print(f"Procesar modo A: {message_from_client}")
                # Enviar mensaje al nodo master
                master_connection.send(message_from_client)
            else:
                # Mensaje desconocido
                print(f"Respuesta desconocida: {message_from_client}")
            break
    # Cerrar la conexión con el cliente
    client_socket.close()
    print(f"Conexión cerrada con {address_client}")


# ------------------------------------
# Main function
# ------------------------------------

def main():
    # ---------- Master node connection ----------

    # Crear un socket para conectarse al nodo Master
    master_connection = ClientConnection(master_node['address'], master_node['port'])

    # Conectar al nodo Master
    master_connection.connect()
    print(f"Conectado al nodo Master {str(master_node)}")

    # ---------- Slave nodes connection ----------

    # Obtener el tamaño de la lista de nodos slave
    size = len(slave_nodes)

    # Crear una lista de conexiones
    list_slaves_connections = []

    # Conectar a los nodos slave
    for i in range(size):
        # Crear un socket para conectarse al nodo Slave
        slave_connection = ClientConnection(slave_nodes[i]['address'], slave_nodes[i]['port'])

        # Conectar al nodo Slave
        slave_connection.connect()
        print(f"Conectado al nodo Slave: {str(slave_nodes[i])}")
        
        # Agregar conexión a la lista de conexiones
        list_slaves_connections.append(slave_connection)

    # ---------- Send accounts to all slaves ----------

    # Ejecutar modo C (Obtener cuentas)
    master_connection.send("C")

    # ---------- Nodes thread listen  ----------

    # Crear un hilo para manejar las respuestas del nodo Master en segundo plano
    response_thread_master = threading.Thread(target=handle_master_response, args=(master_connection,list_slaves_connections))
    response_thread_master.start()

    # Crear n hilos para manejar las respuestas de los nodos Slaves en segundo plano
    for i in range(size):
        response_thread_slave = threading.Thread(target=handle_slave_response, args=(list_slaves_connections[i],master_connection,list_slaves_connections))
        response_thread_slave.start()

    # ---------- Server Forwarder creation ----------

    server_forwarder = ServerCreation('localhost', 60000)
    server_forwarder.listen(1)

    # ---------- Client nodes connection ----------

    while True:
        # Aceptar la conexión entrante desde el cliente
        client_socket, address_client = server_forwarder.accept()

        # Crear un hilo para manejar la conexión con el cliente
        client_handler_thread = threading.Thread(target=handle_client, args=(client_socket, address_client, master_connection, list_slaves_connections))
        client_handler_thread.start()


if __name__ == "__main__":
    main()


