import socket

def main():
    # Crear un socket para el nodo A
    servidor_a = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor_a.bind(('localhost', 40000))
    servidor_a.listen(1)

    print("Nodo A esperando conexiones...")

    # Aceptar la conexión entrante desde el nodo de redirección
    conexion_redireccion, direccion_redireccion = servidor_a.accept()
    print(f"Conexión establecida desde el nodo de redirección: {direccion_redireccion}")

    # Recibir mensaje del nodo de redirección
    mensaje_redireccion = conexion_redireccion.recv(1024)
    print(f"Nodo A recibió el mensaje del nodo de redirección: {mensaje_redireccion.decode('utf-8')}")

    # Enviar respuesta al nodo de redirección
    response = "C-1-120.4;2-152.2;3-455.5"
    conexion_redireccion.send(response.encode('utf-8'))

    # Cerrar conexión
    conexion_redireccion.close()
    servidor_a.close()

if __name__ == "__main__":
    main()