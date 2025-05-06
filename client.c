#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <float.h>
#include <limits.h>
#include <asm-generic/socket.h>

#define SERVER_IP "10.0.4.174"
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

float **allocate_float_matrix(int rows, int cols) {
    float **matrix = (float **)malloc(rows * sizeof(float *));
    if (!matrix) {
        perror("Float matrix allocation failed");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < rows; i++) {
        matrix[i] = (float *)malloc(cols * sizeof(float));
        if (!matrix[i]) {
            perror("Float matrix row allocation failed");
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

void free_float_matrix(float **matrix, int rows) {
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

float **min_max_transform(int **matrix, int rows, int cols) {
    // Create normalized float matrix
    float **normalized = allocate_float_matrix(rows, cols);
    
    // Process each row separately
    for (int i = 0; i < rows; i++) {
        // Find min and max values in this row
        int min_val = INT_MAX;
        int max_val = INT_MIN;
        
        for (int j = 0; j < cols; j++) {
            if (matrix[i][j] < min_val) min_val = matrix[i][j];
            if (matrix[i][j] > max_val) max_val = matrix[i][j];
        }
        
        // Apply min-max normalization to this row
        float range = (float)(max_val - min_val);
        
        for (int j = 0; j < cols; j++) {
            if (range > 0) {
                normalized[i][j] = (float)(matrix[i][j] - min_val) / range;
            } else {
                // Handle case where all values in row are the same
                normalized[i][j] = 0.0f;
            }
        }
        
        printf("Row %d: Min value: %d, Max value: %d\n", i, min_val, max_val);
    }
    
    return normalized;
}

void send_float_matrix(int sock, float **matrix, int rows, int cols) {
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
            if (send(sock, matrix[i], cols * sizeof(float), 0) < 0) {
                perror("Send row failed");
                exit(EXIT_FAILURE);
            }
        }
    }
}

int main(int argc, char *argv[]) {
    int server_fd, client_sock;
    struct sockaddr_in address, client_addr;
    int opt = 1;
    int addrlen = sizeof(client_addr);
    int client_port = PORT;
    
    // If port is provided as command line argument
    if (argc > 1) {
        client_port = atoi(argv[1]);
    }
    
    printf("Starting client on port %d\n", client_port);

    // Creating socket file descriptor
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }
    
    // Set socket options
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("Setsockopt failed");
        exit(EXIT_FAILURE);
    }
    
    // Configure address
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(client_port);
    
    // Bind socket to port
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("Bind failed");
        exit(EXIT_FAILURE);
    }
    
    // Listen for connections
    if (listen(server_fd, 1) < 0) {
        perror("Listen failed");
        exit(EXIT_FAILURE);
    }
    
    printf("Client listening on port %d for server connection...\n", client_port);
    
    // Accept connection from server
    if ((client_sock = accept(server_fd, (struct sockaddr *)&client_addr, (socklen_t*)&addrlen)) < 0) {
        perror("peinr failed");
        exit(EXIT_FAILURE);
    }
    
    printf("Server connected\n");
    
    // Receive submatrix from server
    int rows, cols;
    printf("Waiting to receive matrix from server...\n");
    int **matrix = receive_matrix(client_sock, &rows, &cols);
    printf("Received %dx%d submatrix from server\n", rows, cols);
    
    // // Print the received matrix
    // printf("\nReceived matrix:\n");
    // for (int i = 0; i < rows; i++) {
    //     for (int j = 0; j < cols; j++) {
    //         printf("%3d ", matrix[i][j]);
    //     }
    //     printf("\n");
    // }
    // printf("\n");

    // Apply min-max transformation
    printf("Applying min-max normalization...\n");
    float **normalized_matrix = min_max_transform(matrix, rows, cols);
    
    // Print a sample of the normalized matrix
    printf("Sample of normalized matrix (up to 5x5):\n");
    for (int i = 0; i < (rows < 5 ? rows : 5); i++) {
        for (int j = 0; j < (cols < 5 ? cols : 5); j++) {
            printf("%.4f ", normalized_matrix[i][j]);
        }
        printf("\n");
    }
    
    // Wait for request from server before sending the result
    char request_code;
    printf("Waiting for server to request normalized matrix...\n");
    if (recv(client_sock, &request_code, sizeof(char), MSG_WAITALL) != sizeof(char)) {
        perror("Failed to receive request from server");
        exit(EXIT_FAILURE);
    }
    
    if (request_code != 1) {
        printf("Received unexpected request code: %d\n", request_code);
    } else {
        // Send normalized matrix back to server
        printf("Sending normalized matrix back to server...\n");
        send_float_matrix(client_sock, normalized_matrix, rows, cols);
        printf("Normalized matrix sent back to server\n");
    }
    
    // Clean up
    free_matrix(matrix, rows);
    free_float_matrix(normalized_matrix, rows);
    close(client_sock);
    close(server_fd);
    
    return 0;
}