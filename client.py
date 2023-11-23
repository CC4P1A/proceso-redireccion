import socket

def main():
    # Dirección y puerto del servidor
    server_address = ('localhost', 60000)

    # Crear un socket del cliente
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        # Conectar al servidor
        client_socket.connect(server_address)
        
        local_address = client_socket.getsockname()
        client_info = f"{local_address[0]}:{local_address[1]}"
        # Enviar el mensaje "Hola, mundo"
        mensaje = f"L-{client_info}-542"
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