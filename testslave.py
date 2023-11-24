import socket

def main():
    # Dirección y puerto del servidor
    server_address = ('localhost', 50000)

    # Crear un socket del cliente
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        # Conectar al servidor
        client_socket.connect(server_address)
        
        # Enviar el mensaje "Hola, mundo"
        mensaje = f"L-1-10\n"
        client_socket.send(mensaje.encode('utf-8'))
        
        print(f"Mensaje enviado: {mensaje}")

        # Esperar la respuesta del servidor
        response = client_socket.recv(1024).decode('utf-8')
        print(f"Respuesta del servidor: {response}")

    finally:
        # Cerrar el socket del cliente
        client_socket.close()

if __name__ == "__main__":
    main()