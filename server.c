#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <time.h>
#include <asm-generic/socket.h>

#define PORT 8080
#define MAX_MATRIX_SIZE 30000
#define CHUNK_SIZE 1000  // Number of rows to send at a time

int **create_random_matrix(int rows, int cols) {
    int **matrix = (int **)malloc(rows * sizeof(int *));
    if (!matrix) {
        perror("Matrix allocation failed");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < rows; i++) {
        matrix[i] = (int *)malloc(cols * sizeof(int));
        if (!matrix[i]) {
            perror("Matrix row allocation failed");
            exit(EXIT_FAILURE);
        }
        for (int j = 0; j < cols; j++) {
            matrix[i][j] = rand() % 100;  // Random numbers between 0-99
        }
    }
    return matrix;
}

void free_matrix(int **matrix, int rows) {
    for (int i = 0; i < rows; i++) {
        free(matrix[i]);
    }
    free(matrix);
}

void send_matrix(int sock, int **matrix, int rows, int cols) {
    // First send matrix dimensions
    int dimensions[2] = {rows, cols};
    if (send(sock, dimensions, sizeof(dimensions), 0) < 0) {
        perror("Send dimensions failed");
        exit(EXIT_FAILURE);
    }

    // Then send matrix data in chunks
    for (int chunk_start = 0; chunk_start < rows; chunk_start += CHUNK_SIZE) {
        int chunk_end = (chunk_start + CHUNK_SIZE < rows) ? chunk_start + CHUNK_SIZE : rows;
        int chunk_rows = chunk_end - chunk_start;

        // Send chunk rows count
        if (send(sock, &chunk_rows, sizeof(int), 0) < 0) {
            perror("Send chunk rows failed");
            exit(EXIT_FAILURE);
        }

        // Send each row in the chunk
        for (int i = chunk_start; i < chunk_end; i++) {
            if (send(sock, matrix[i], cols * sizeof(int), 0) < 0) {
                perror("Send row failed");
                exit(EXIT_FAILURE);
            }
        }
    }
}

int **receive_matrix(int sock, int *rows, int *cols) {
    // First receive matrix dimensions
    int dimensions[2];
    if (recv(sock, dimensions, sizeof(dimensions), MSG_WAITALL) != sizeof(dimensions)) {
        perror("Receive dimensions failed");
        exit(EXIT_FAILURE);
    }
    *rows = dimensions[0];
    *cols = dimensions[1];

    // Allocate matrix
    int **matrix = (int **)malloc(*rows * sizeof(int *));
    if (!matrix) {
        perror("Matrix allocation failed");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < *rows; i++) {
        matrix[i] = (int *)malloc(*cols * sizeof(int));
        if (!matrix[i]) {
            perror("Matrix row allocation failed");
            exit(EXIT_FAILURE);
        }
    }

    // Receive matrix data in chunks
    int received_rows = 0;
    while (received_rows < *rows) {
        int chunk_rows;
        if (recv(sock, &chunk_rows, sizeof(int), MSG_WAITALL) != sizeof(int)) {
            perror("Receive chunk rows failed");
            exit(EXIT_FAILURE);
        }

        for (int i = 0; i < chunk_rows; i++) {
            if (recv(sock, matrix[received_rows + i], *cols * sizeof(int), MSG_WAITALL) != *cols * sizeof(int)) {
                perror("Receive row failed");
                exit(EXIT_FAILURE);
            }
        }
        received_rows += chunk_rows;
    }

    return matrix;
}

int main() {
    srand(time(NULL));
    int server_fd, new_socket;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);

    // Create socket
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("Socket failed");
        exit(EXIT_FAILURE);
    }

    // Set socket options
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("Setsockopt failed");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    // Bind socket
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("Bind failed");
        exit(EXIT_FAILURE);
    }

    // Listen for connections
    if (listen(server_fd, 3) < 0) {
        perror("Listen failed");
        exit(EXIT_FAILURE);
    }

    printf("Server listening on port %d...\n", PORT);

    // Accept connection
    if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t *)&addrlen)) < 0) {
        perror("Accept failed");
        exit(EXIT_FAILURE);
    }

    printf("Client connected\n");

    // Create and send matrix
    int rows = 100, cols = 100;  // Default size - adjust as needed
    printf("Creating %dx%d matrix...\n", rows, cols);
    int **matrix = create_random_matrix(rows, cols);
    
    printf("Sending matrix to client...\n");
    send_matrix(new_socket, matrix, rows, cols);
    printf("Matrix sent to client\n");

    // Free the original matrix as we'll receive it back
    free_matrix(matrix, rows);

    // Receive matrix back from client
    printf("Waiting to receive matrix back from client...\n");
    int **received_matrix = receive_matrix(new_socket, &rows, &cols);
    printf("Received %dx%d matrix back from client\n", rows, cols);

    // Clean up
    free_matrix(received_matrix, rows);
    close(new_socket);
    close(server_fd);

    return 0;
}