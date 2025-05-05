#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define SERVER_IP "127.0.0.1"
#define PORT 8080
#define CHUNK_SIZE 1000

int **allocate_matrix(int rows, int cols) {
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
    }
    return matrix;
}

void free_matrix(int **matrix, int rows) {
    for (int i = 0; i < rows; i++) {
        free(matrix[i]);
    }
    free(matrix);
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
    int **matrix = allocate_matrix(*rows, *cols);

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

int main() {
    int sock = 0;
    struct sockaddr_in serv_addr;

    // Create socket
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);

    // Convert IPv4 address from text to binary form
    if (inet_pton(AF_INET, SERVER_IP, &serv_addr.sin_addr) <= 0) {
        perror("Invalid address/Address not supported");
        exit(EXIT_FAILURE);
    }

    // Connect to server
    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("Connection failed");
        exit(EXIT_FAILURE);
    }

    printf("Connected to server\n");

    // Receive matrix from server
    int rows, cols;
    printf("Waiting to receive matrix from server...\n");
    int **matrix = receive_matrix(sock, &rows, &cols);
    printf("Received %dx%d matrix from server\n", rows, cols);

    // Send matrix back to server
    printf("Sending matrix back to server...\n");
    send_matrix(sock, matrix, rows, cols);
    printf("Matrix sent back to server\n");

    // Clean up
    free_matrix(matrix, rows);
    close(sock);

    return 0;
}