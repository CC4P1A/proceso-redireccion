import socket

def main():
    # Crear un socket para el nodo
    servidor_b = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor_b.bind(('localhost', 50000))
    servidor_b.listen(1)

    print("Nodo B esperando conexiones en el puerto 50000...")

    # Aceptar la conexión entrante desde el nodo de redirección
    conexion_redireccion, direccion_redireccion = servidor_b.accept()
    print(f"Conexión establecida desde el nodo de redirección: {direccion_redireccion}")

    # Esperar hasta recibir un mensaje que comience con "L"
    while True:
        # Recibir mensaje del nodo de redirección
        mensaje_redireccion = conexion_redireccion.recv(1024).decode('utf-8')
        
        if mensaje_redireccion.startswith("C"):
            print(f"Nodo B recibió el mensaje del nodo de redirección: {mensaje_redireccion}")
        # Verificar si el mensaje comienza con "L"
        if mensaje_redireccion.startswith("L"):
            print(f"Nodo B recibió el mensaje del nodo de redirección: {mensaje_redireccion}")
            
            # Enviar respuesta al nodo de redirección
            response = f"{mensaje_redireccion}-455.5"
            conexion_redireccion.send(response.encode('utf-8'))
            
            # Romper el bucle después de procesar el mensaje "L"
            break

    # Cerrar conexión
    conexion_redireccion.close()
    servidor_b.close()

if __name__ == "__main__":
    main()