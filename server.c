#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <time.h>
#include <asm-generic/socket.h>
#include <pthread.h>
#define PORT 8080
#define MAX_MATRIX_SIZE 30000
#define CHUNK_SIZE 1000  // Number of rows to send at a time
#define MAX_CLIENTS 100
#define MAX_IP_LEN 16    // xxx.xxx.xxx.xxx\0
#define CONFIG_FILE "config.txt"

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

// Global variables for matrix distribution
int **global_matrix;
int global_rows, global_cols;
ClientInfo *clients;
int client_count = 0;

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

void free_float_matrix(float **matrix, int rows) {
    for (int i = 0; i < rows; i++) {
        free(matrix[i]);
    }
    free(matrix);
}

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
    float **matrix = (float **)malloc(*rows * sizeof(float *));
    if (!matrix) {
        perror("Matrix allocation failed");
        return NULL;
    }

    for (int i = 0; i < *rows; i++) {
        matrix[i] = (float *)malloc(*cols * sizeof(float));
        if (!matrix[i]) {
            perror("Matrix row allocation failed");
            // Free previously allocated memory
            for (int j = 0; j < i; j++) {
                free(matrix[j]);
            }
            free(matrix);
            return NULL;
        }
    }

    // Receive matrix data in chunks
    int received_rows = 0;
    while (received_rows < *rows) {
        int chunk_rows;
        if (recv(sock, &chunk_rows, sizeof(int), MSG_WAITALL) != sizeof(int)) {
            perror("Receive chunk rows failed");
            // Free allocated memory
            for (int i = 0; i < *rows; i++) {
                free(matrix[i]);
            }
            free(matrix);
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
                    // Free allocated memory
                    for (int j = 0; j < *rows; j++) {
                        free(matrix[j]);
                    }
                    free(matrix);
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
    
    if (rows != client->rows || cols != client->cols) {
        printf("Warning: Client at %s:%d returned matrix of unexpected size: %dx%d (expected %dx%d)\n",
               client->ip, client->port, rows, cols, client->rows, client->cols);
    }
    
    printf("Received normalized matrix from client at %s:%d\n", client->ip, client->port);
    
    return NULL;
}

float **combine_results() {
    // Create combined result matrix
    float **combined = (float **)malloc(global_rows * sizeof(float *));
    if (!combined) {
        perror("Failed to allocate memory for combined result");
        exit(EXIT_FAILURE);
    }
    
    for (int i = 0; i < global_rows; i++) {
        combined[i] = (float *)malloc(global_cols * sizeof(float));
        if (!combined[i]) {
            perror("Failed to allocate memory for combined result row");
            exit(EXIT_FAILURE);
        }
    }
    
    // Copy partial results to combined matrix
    for (int c = 0; c < client_count; c++) {
        if (clients[c].socket != -1 && clients[c].partial_result != NULL) {
            for (int i = 0; i < clients[c].rows; i++) {
                memcpy(combined[clients[c].start_row + i], 
                       clients[c].partial_result[i], 
                       global_cols * sizeof(float));
            }
        }
    }
    
    return combined;
}

int main() {
    srand(time(NULL));
    
    // Read client configuration
    printf("Reading client configuration from %s...\n", CONFIG_FILE);
    read_client_config();
    
    // Create matrix - adjust size as needed
    global_rows = 20000;  // Adjust matrix size as needed
    global_cols = 20000;
    printf("Creating %dx%d matrix...\n", global_rows, global_cols);
    global_matrix = create_random_matrix(global_rows, global_cols);
    
    // printf("\nOriginal matrix:\n");
    // for (int i = 0; i < global_rows; i++) {
    //     for (int j = 0; j < global_cols; j++) {
    //         printf("%3d ", global_matrix[i][j]);
    //     }
    //     printf("\n");
    // }
    // printf("\n");

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
    
    // // Print a sample of the normalized matrix
    // printf("Sample of combined normalized matrix (up to 5x5):\n");
    // for (int i = 0; i < global_rows; i++) {
    //     for (int j = 0; j < global_cols; j++) {
    //         printf("%.4f ", combined_matrix[i][j]);
    //     }
    //     printf("\n");
    // }
    
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
    return 0;
}