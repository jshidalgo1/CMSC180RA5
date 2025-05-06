#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <time.h>
#include <pthread.h>
#include <sys/time.h>
#include <float.h>
#include <limits.h>
#include <asm-generic/socket.h>

// Common defines
#define MAX_MATRIX_SIZE 30000
#define CHUNK_SIZE 1000
#define MAX_CLIENTS 100
#define MAX_IP_LEN 16
#define CONFIG_FILE "config.txt"

// Function to get time in seconds with microsecond precision
double get_time_s() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + (tv.tv_usec / 1000000.0);
}

// Memory allocation functions
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

// Matrix creation function
int **create_random_matrix(int rows, int cols) {
    int **matrix = allocate_matrix(rows, cols);
    
    for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
            matrix[i][j] = rand() % 100;  // Random numbers between 0-99
        }
    }
    return matrix;
}

// Client-server communication functions
void send_submatrix(int sock, int **matrix, int start_row, int end_row, int cols) {
    int rows = end_row - start_row;
    
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
            if (send(sock, matrix[start_row + i], cols * sizeof(int), 0) < 0) {
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

void send_float_matrix(int sock, float **matrix, int rows, int cols) {
    // First send matrix dimensions
    int dimensions[2] = {rows, cols};
    if (send(sock, dimensions, sizeof(dimensions), 0) < 0) {
        perror("Send dimensions failed");
        exit(EXIT_FAILURE);
    }

    // Send in smaller chunks with delays
    int chunk_size = 100;  // Use a smaller chunk size
    
    // Then send matrix data in chunks
    for (int chunk_start = 0; chunk_start < rows; chunk_start += chunk_size) {
        int chunk_end = (chunk_start + chunk_size < rows) ? chunk_start + chunk_size : rows;
        int chunk_rows = chunk_end - chunk_start;

        // Send chunk rows count
        if (send(sock, &chunk_rows, sizeof(int), 0) < 0) {
            perror("Send chunk rows failed");
            exit(EXIT_FAILURE);
        }

        // Send each row in the chunk
        for (int i = chunk_start; i < chunk_end; i++) {
            // Add buffer for potential large data
            ssize_t total_sent = 0;
            size_t to_send = cols * sizeof(float);
            
            while (total_sent < to_send) {
                ssize_t sent = send(sock, ((char*)matrix[i]) + total_sent, to_send - total_sent, 0);
                if (sent < 0) {
                    perror("Send row failed");
                    exit(EXIT_FAILURE);
                }
                total_sent += sent;
            }
            
            // Add a small delay between sending rows to prevent overwhelming the socket buffer
            usleep(100); // 100 microseconds
        }
        
        // Add a slightly longer delay between chunks
        usleep(10000); // 10 milliseconds
    }
}

float **receive_float_matrix(int sock, int *rows, int *cols) {
    // First receive matrix dimensions
    int dimensions[2];
    if (recv(sock, dimensions, sizeof(dimensions), MSG_WAITALL) != sizeof(dimensions)) {
        perror("Receive dimensions failed");
        return NULL;
    }
    *rows = dimensions[0];
    *cols = dimensions[1];
    
    printf("Receiving matrix of size %dx%d\n", *rows, *cols);

    // Allocate matrix
    float **matrix = allocate_float_matrix(*rows, *cols);

    // Receive matrix data in chunks
    int received_rows = 0;
    while (received_rows < *rows) {
        int chunk_rows;
        if (recv(sock, &chunk_rows, sizeof(int), MSG_WAITALL) != sizeof(int)) {
            perror("Receive chunk rows failed");
            free_float_matrix(matrix, *rows);
            return NULL;
        }
        
        printf("Receiving chunk of %d rows\n", chunk_rows);

        for (int i = 0; i < chunk_rows; i++) {
            ssize_t total_received = 0;
            size_t to_receive = *cols * sizeof(float);
            
            while (total_received < to_receive) {
                ssize_t received = recv(sock, ((char*)matrix[received_rows + i]) + total_received, 
                                      to_receive - total_received, MSG_WAITALL);
                if (received <= 0) {
                    perror("Receive row failed");
                    free_float_matrix(matrix, *rows);
                    return NULL;
                }
                total_received += received;
            }
        }
        received_rows += chunk_rows;
        printf("Received %d/%d rows\n", received_rows, *rows);
    }

    return matrix;
}

// Client-specific functions
float **min_max_transform(int **matrix, int rows, int cols) {
    // Start timing the normalization process
    double start_time = get_time_s();
    
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
        
        // Only print stats for some rows when the matrix is large
        if (rows <= 20 || i % (rows/10) == 0) {
            printf("Row %d: Min value: %d, Max value: %d\n", i, min_val, max_val);
        }
    }
    
    // End timing and calculate elapsed time
    double end_time = get_time_s();
    double elapsed_time = end_time - start_time;
    
    printf("\nMin-max transformation completed in %.2f s\n", elapsed_time);
    printf("Average time per element: %.9f s\n\n", elapsed_time / (rows * cols));
    
    return normalized;
}

// Server-specific data structures and functions
typedef struct {
    char ip[MAX_IP_LEN];
    int port;
    int socket;
    int start_row;
    int end_row;
    float **partial_result;
    int rows;
    int cols;
} ClientInfo;

// Global variables for matrix distribution (server mode only)
int **global_matrix;
int global_rows, global_cols;
ClientInfo *clients;
int client_count = 0;

int read_client_config() {
    FILE *config = fopen(CONFIG_FILE, "r");
    if (config == NULL) {
        perror("Failed to open config file");
        exit(EXIT_FAILURE);
    }

    char ip[MAX_IP_LEN];
    int port;
    int count = 0;

    // First count the number of clients
    while (fscanf(config, "%s %d", ip, &port) == 2) {
        count++;
    }

    if (count == 0) {
        printf("No clients found in config file\n");
        fclose(config);
        exit(EXIT_FAILURE);
    }

    // Allocate memory for clients
    clients = (ClientInfo *)malloc(count * sizeof(ClientInfo));
    if (!clients) {
        perror("Failed to allocate memory for clients");
        fclose(config);
        exit(EXIT_FAILURE);
    }

    // Reset file pointer and read client info
    rewind(config);
    client_count = 0;
    while (fscanf(config, "%s %d", ip, &port) == 2 && client_count < count) {
        strncpy(clients[client_count].ip, ip, MAX_IP_LEN - 1);
        clients[client_count].port = port;
        clients[client_count].socket = -1;  // Initialize to invalid socket
        client_count++;
    }

    fclose(config);
    printf("Read %d clients from config file\n", client_count);
    return client_count;
}

void connect_to_clients() {
    struct sockaddr_in address;
    address.sin_family = AF_INET;

    for (int i = 0; i < client_count; i++) {
        // Create socket
        if ((clients[i].socket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
            perror("Socket creation failed");
            exit(EXIT_FAILURE);
        }

        // Set port and IP
        address.sin_port = htons(clients[i].port);
        
        // Convert IPv4 address from text to binary
        if (inet_pton(AF_INET, clients[i].ip, &address.sin_addr) <= 0) {
            printf("Invalid address for client %d: %s\n", i, clients[i].ip);
            continue;
        }

        // Connect to client
        printf("Connecting to client %d at %s:%d...\n", i, clients[i].ip, clients[i].port);
        if (connect(clients[i].socket, (struct sockaddr *)&address, sizeof(address)) < 0) {
            printf("Failed to connect to client %d at %s:%d\n", i, clients[i].ip, clients[i].port);
            close(clients[i].socket);
            clients[i].socket = -1;
            continue;
        }
        
        printf("Connected to client %d at %s:%d\n", i, clients[i].ip, clients[i].port);
    }
}

void distribute_matrix_work() {
    int active_clients = 0;
    
    // Count active clients
    for (int i = 0; i < client_count; i++) {
        if (clients[i].socket != -1) {
            active_clients++;
        }
    }
    
    if (active_clients == 0) {
        printf("No active clients to distribute work to\n");
        exit(EXIT_FAILURE);
    }
    
    // Distribute rows as evenly as possible
    int rows_per_client = global_rows / active_clients;
    int extra_rows = global_rows % active_clients;
    
    int current_row = 0;
    for (int i = 0; i < client_count; i++) {
        if (clients[i].socket != -1) {
            clients[i].start_row = current_row;
            
            // Distribute extra rows one by one
            int client_rows = rows_per_client;
            if (extra_rows > 0) {
                client_rows++;
                extra_rows--;
            }
            
            clients[i].end_row = current_row + client_rows;
            current_row += client_rows;
            
            clients[i].rows = client_rows;
            clients[i].cols = global_cols;
            
            printf("Client %d assigned rows %d to %d\n", 
                   i, clients[i].start_row, clients[i].end_row - 1);
        }
    }
}

void *handle_client(void *arg) {
    ClientInfo *client = (ClientInfo *)arg;
    
    printf("Sending submatrix to client at %s:%d (rows %d-%d)\n", 
           client->ip, client->port, client->start_row, client->end_row - 1);
    
    // Send submatrix to client
    send_submatrix(client->socket, global_matrix, client->start_row, client->end_row, client->cols);
    
    // Wait a bit to ensure client has time to process
    sleep(1);
    
    // Send a request for the normalized matrix
    char request_code = 1;  // 1 = request for normalized matrix
    if (send(client->socket, &request_code, sizeof(char), 0) < 0) {
        perror("Failed to send request for normalized matrix");
        return NULL;
    }
    
    // Receive normalized matrix back from client
    printf("Waiting to receive normalized matrix from client at %s:%d...\n", client->ip, client->port);
    int rows, cols;
    client->partial_result = receive_float_matrix(client->socket, &rows, &cols);
    
    if (client->partial_result == NULL) {
        printf("Failed to receive matrix from client at %s:%d\n", client->ip, client->port);
        return NULL;
    }
    
    if (rows != client->rows || cols != client->cols) {
        printf("Warning: Client at %s:%d returned matrix of unexpected size: %dx%d (expected %dx%d)\n",
               client->ip, client->port, rows, cols, client->rows, client->cols);
    }
    
    printf("Received normalized matrix from client at %s:%d\n", client->ip, client->port);
    
    return NULL;
}

float **combine_results() {
    // Create combined result matrix
    float **combined = allocate_float_matrix(global_rows, global_cols);
    
    // Copy partial results to combined matrix
    int missing_results = 0;
    for (int c = 0; c < client_count; c++) {
        if (clients[c].socket != -1) {
            if (clients[c].partial_result != NULL) {
                for (int i = 0; i < clients[c].rows; i++) {
                    memcpy(combined[clients[c].start_row + i], 
                           clients[c].partial_result[i], 
                           global_cols * sizeof(float));
                }
            } else {
                printf("Warning: Missing results from client %d (rows %d-%d)\n", 
                       c, clients[c].start_row, clients[c].end_row - 1);
                missing_results++;
            }
        }
    }
    
    if (missing_results > 0) {
        printf("Warning: %d clients failed to return results\n", missing_results);
    }
    
    return combined;
}

void run_server(int matrix_size) {
    srand(time(NULL));
    
    // Read client configuration
    printf("Reading client configuration from %s...\n", CONFIG_FILE);
    read_client_config();
    
    // Create matrix - adjust size as needed
    global_rows = matrix_size;
    global_cols = matrix_size;
    printf("Creating %dx%d matrix...\n", global_rows, global_cols);
    global_matrix = create_random_matrix(global_rows, global_cols);
    
    // Start timing before distribution
    double start_time = get_time_s();
    printf("Starting matrix distribution at %.2f s\n", start_time);
    
    // Connect to clients
    printf("Connecting to clients...\n");
    connect_to_clients();
    
    // Distribute matrix work
    printf("Distributing matrix work...\n");
    distribute_matrix_work();
    
    // Create threads to handle clients
    pthread_t *threads = (pthread_t *)malloc(client_count * sizeof(pthread_t));
    if (!threads) {
        perror("Failed to allocate memory for threads");
        exit(EXIT_FAILURE);
    }
    
    printf("Starting client threads...\n");
    for (int i = 0; i < client_count; i++) {
        if (clients[i].socket != -1) {
            if (pthread_create(&threads[i], NULL, handle_client, (void *)&clients[i]) != 0) {
                perror("Failed to create thread");
                exit(EXIT_FAILURE);
            }
        }
    }
    
    // Wait for all threads to complete
    for (int i = 0; i < client_count; i++) {
        if (clients[i].socket != -1) {
            pthread_join(threads[i], NULL);
        }
    }
    
    // Combine results
    printf("Combining results from all clients...\n");
    float **combined_matrix = combine_results();
    
    // End timing after rebuilding the matrix
    double end_time = get_time_s();
    double elapsed_time = end_time - start_time;
    printf("\nTotal processing time: %.2f s\n", elapsed_time);
    printf("Matrix size: %dx%d, Number of clients: %d\n", global_rows, global_cols, client_count);
    printf("Average time per element: %.9f s\n", elapsed_time / (global_rows * global_cols));
    
    // Print a sample of the normalized matrix (first 5x5 elements)
    printf("Sample of combined normalized matrix (up to 5x5):\n");
    for (int i = 0; i < (global_rows < 5 ? global_rows : 5); i++) {
        for (int j = 0; j < (global_cols < 5 ? global_cols : 5); j++) {
            printf("%.4f ", combined_matrix[i][j]);
        }
        printf("\n");
    }
    
    // Clean up
    free_matrix(global_matrix, global_rows);
    free_float_matrix(combined_matrix, global_rows);
    
    // Free client resources
    for (int i = 0; i < client_count; i++) {
        if (clients[i].socket != -1) {
            if (clients[i].partial_result != NULL) {
                free_float_matrix(clients[i].partial_result, clients[i].rows);
            }
            close(clients[i].socket);
        }
    }
    
    free(clients);
    free(threads);
    
    printf("Process completed successfully\n");
}

void run_client(int port) {
    int server_fd, client_sock;
    struct sockaddr_in address, client_addr;
    int opt = 1;
    int addrlen = sizeof(client_addr);
    int client_port = port;
    
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
        perror("Accept failed");
        exit(EXIT_FAILURE);
    }
    
    printf("Server connected\n");
    
    // Receive submatrix from server
    int rows, cols;
    printf("Waiting to receive matrix from server...\n");
    int **matrix = receive_matrix(client_sock, &rows, &cols);
    printf("Received %dx%d submatrix from server\n", rows, cols);
    
    // Print the received matrix if it's small enough
    if (rows <= 10 && cols <= 10) {
        printf("\nReceived matrix:\n");
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                printf("%3d ", matrix[i][j]);
            }
            printf("\n");
        }
        printf("\n");
    }
    
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
    
    printf("Client process completed successfully\n");
}

int main(int argc, char *argv[]) {
    if (argc != 4) {
        printf("Usage: %s <matrix_size> <port> <0=server|1=client>\n", argv[0]);
        return 1;
    }
    
    int matrix_size = atoi(argv[1]);
    int port = atoi(argv[2]);
    int is_client = atoi(argv[3]);
    
    if (matrix_size <= 0) {
        printf("Matrix size must be positive\n");
        return 1;
    }
    
    if (port <= 0) {
        printf("Port must be positive\n");
        return 1;
    }
    
    if (is_client != 0 && is_client != 1) {
        printf("Mode must be 0 (server) or 1 (client)\n");
        return 1;
    }
    
    printf("Starting %s mode with matrix size %d on port %d\n", 
           is_client ? "CLIENT" : "SERVER", matrix_size, port);
           
    if (is_client) {
        run_client(port);
    } else {
        run_server(matrix_size);
    }
    
    return 0;
}