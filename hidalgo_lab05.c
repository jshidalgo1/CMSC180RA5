#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdint.h> // Include for uint8_t
#include <sched.h> // For sched_setaffinity

#define MAX_SLAVES 16
#define BUFFER_SIZE (15* 1024 * 1024)  // 4MB buffer
#define CONFIG_FILE "config.txt"
#define CHUNK_SIZE 64              // Rows per chunk
#define CHUNK_DELAY_US 1000

typedef struct {
    char ip[16];
    int port;
} SlaveInfo;

typedef struct {
    int **matrix;          // Normalized matrix
    int **original_matrix; // Original matrix
    int n;                 // Matrix size
    int p;                 // Port number
    int s;                 // Status (0=master, 1=slave)
    int t;                 // Number of slaves
    SlaveInfo slaves[MAX_SLAVES];
} ProgramState;

typedef struct {
    int start_row;
    int end_row;
    int **submatrix;
    double **normalized_matrix;
    int cols;
    int core_id; // Core to bind the thread
} MMTArgs;

typedef struct {
    ProgramState *state;
    int slave_index;
    int start_row;
    int rows_for_this_slave;
    int sock; 
} ThreadArgs;

int get_usable_cores() {
    int total_cores = sysconf(_SC_NPROCESSORS_ONLN);
    return total_cores > 1 ? total_cores - 1 : 1; // Use n-1 cores, but at least 1
}

void read_config(ProgramState *state, int required_slaves) {
    FILE *file = fopen(CONFIG_FILE, "r");
    if (!file) {
        perror("Failed to open config file");
        exit(EXIT_FAILURE);
    }

    state->t = 0;
    char line[100];
    while (state->t < required_slaves && fgets(line, sizeof(line), file)) {
        sscanf(line, "%s %d", state->slaves[state->t].ip, &state->slaves[state->t].port);
        state->t++;
    }
    fclose(file);
}

void allocate_matrix(ProgramState *state) {
    // Try to allocate with error handling and recovery
    printf("Allocating matrices of size %d x %d...\n", state->n, state->n);
    
    // Allocate memory for the original matrix
    state->original_matrix = (int **)malloc(state->n * sizeof(int *));
    if (!state->original_matrix) {
        perror("Original matrix allocation failed");
        exit(EXIT_FAILURE);
    }

    // Allocate memory for the normalized matrix
    state->matrix = (int **)malloc(state->n * sizeof(int *));
    if (!state->matrix) {
        perror("Normalized matrix allocation failed");
        free(state->original_matrix);
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < state->n; i++) {
        state->original_matrix[i] = (int *)malloc(state->n * sizeof(int));
        if (!state->original_matrix[i]) {
            perror("Original matrix row allocation failed");
            // Clean up previously allocated rows
            for (int j = 0; j < i; j++) {
                free(state->original_matrix[j]);
            }
            free(state->original_matrix);
            free(state->matrix);
            exit(EXIT_FAILURE);
        }

        state->matrix[i] = (int *)malloc(state->n * sizeof(int));
        if (!state->matrix[i]) {
            perror("Normalized matrix row allocation failed");
            // Clean up previously allocated rows
            for (int j = 0; j <= i; j++) {
                free(state->original_matrix[j]);
            }
            free(state->original_matrix);
            for (int j = 0; j < i; j++) {
                free(state->matrix[j]);
            }
            free(state->matrix);
            exit(EXIT_FAILURE);
        }
    }
    
    printf("Matrix allocation successful\n");
}

void free_matrix(ProgramState *state) {
    if (state->original_matrix) {
        for (int i = 0; i < state->n; i++) {
            free(state->original_matrix[i]);
        }
        free(state->original_matrix);
    }

    if (state->matrix) {
        for (int i = 0; i < state->n; i++) {
            free(state->matrix[i]);
        }
        free(state->matrix);
    }
}

void create_matrix(ProgramState *state) {
    srand(time(NULL));
    for (int i = 0; i < state->n; i++) {
        for (int j = 0; j < state->n; j++) {
            state->original_matrix[i][j] = rand() % 100 + 1;
            state->matrix[i][j] = state->original_matrix[i][j]; // Copy to normalized matrix
        }
    }
}

void print_matrix(int **matrix, int rows, int cols) {
    printf("Received matrix:\n");
    for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
            printf("%d ", matrix[i][j]);
        }
        printf("\n");
    }
}

void print_double_matrix(double **matrix, int rows, int cols) {
    for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
            printf("%.2f ", matrix[i][j]);
        }
        printf("\n");
    }
}

void *threaded_mmt(void *arg) {
    MMTArgs *args = (MMTArgs *)arg;

    // Set core affinity
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(args->core_id, &cpuset);
    pthread_t thread = pthread_self();
    if (pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset) != 0) {
        perror("Failed to set thread affinity");
        pthread_exit(NULL);
    }

    // Perform Min-Max Transformation
    for (int i = args->start_row; i < args->end_row; i++) {
        int min_val = args->submatrix[i][0];
        int max_val = args->submatrix[i][0];
        for (int j = 1; j < args->cols; j++) {
            if (args->submatrix[i][j] < min_val) min_val = args->submatrix[i][j];
            if (args->submatrix[i][j] > max_val) max_val = args->submatrix[i][j];
        }

        for (int j = 0; j < args->cols; j++) {
            if (max_val == min_val) {
                args->normalized_matrix[i][j] = 0.0; // Avoid division by zero
            } else {
                args->normalized_matrix[i][j] = (double)(args->submatrix[i][j] - min_val) / (max_val - min_val);
            }
        }
    }

    pthread_exit(NULL);
}

void *send_to_slave(void *arg) {
    ThreadArgs *args = (ThreadArgs *)arg;
    ProgramState *state = args->state;
    int slave = args->slave_index;
    int start_row = args->start_row;
    int rows_for_this_slave = args->rows_for_this_slave;
    int max_retries = 3; // Number of connection retries

    printf("Sending data to slave %d at IP %s, Port %d\n", 
           slave, state->slaves[slave].ip, state->slaves[slave].port);

    int retry_count = 0;
    int connected = 0;
    int sock = -1;
    
    while (retry_count < max_retries && !connected) {
        // Create socket
        sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            perror("Socket creation failed");
            sleep(2); // Wait before retry
            retry_count++;
            continue;
        }
        
        // Set socket options
        int flag = 1;
        setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(flag));
        setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));
        
        // Increase buffer sizes significantly
        int send_buf_size = BUFFER_SIZE * 4;
        int recv_buf_size = BUFFER_SIZE * 4;
        setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &send_buf_size, sizeof(send_buf_size));
        setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &recv_buf_size, sizeof(recv_buf_size));
        
        // Set timeouts
        struct timeval timeout;
        timeout.tv_sec = 60;  // 60 second timeout (increased)
        timeout.tv_usec = 0;
        setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        
        // Connect to slave
        struct sockaddr_in slave_addr;
        memset(&slave_addr, 0, sizeof(slave_addr));
        slave_addr.sin_family = AF_INET;
        slave_addr.sin_port = htons(state->slaves[slave].port);
        inet_pton(AF_INET, state->slaves[slave].ip, &slave_addr.sin_addr);
        
        if (connect(sock, (struct sockaddr *)&slave_addr, sizeof(slave_addr)) < 0) {
            perror("Connection failed");
            close(sock);
            printf("Retrying connection to slave %d (%d/%d)...\n", 
                   slave, retry_count+1, max_retries);
            sleep(5); // Wait 5 seconds before retry
            retry_count++;
            continue;
        }
        
        connected = 1;
    }
    
    if (!connected) {
        printf("Failed to connect to slave %d after %d attempts\n", slave, max_retries);
        pthread_exit(NULL);
    }
    
    // Store the socket in args
    args->sock = sock;

    // Send submatrix size info with a small message first to test connection
    printf("Testing connection to slave %d...\n", slave);
    char test_msg[64];
    sprintf(test_msg, "TEST_CONNECTION");
    if (send(sock, test_msg, strlen(test_msg)+1, 0) <= 0) {
        perror("Connection test failed");
        close(sock);
        pthread_exit(NULL);
    }
    
    printf("Sent test message to slave %d, waiting for acknowledgment...\n", slave);
    
    // Set a short timeout for handshake
    struct timeval short_timeout;
    short_timeout.tv_sec = 5;  // 5 second timeout for handshake
    short_timeout.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &short_timeout, sizeof(short_timeout));
    
    // Wait for ack from slave
    char ack[64];
    memset(ack, 0, sizeof(ack));
    int recv_result = recv(sock, ack, sizeof(ack), 0);
    if (recv_result <= 0) {
        if (recv_result == 0)
            fprintf(stderr, "Connection closed by slave %d during handshake\n", slave);
        else
            perror("Failed to receive test acknowledgment");
        close(sock);
        pthread_exit(NULL);
    }
    
    printf("Received acknowledgment from slave %d: %s\n", slave, ack);
    
    // Reset timeout to original value
    struct timeval timeout;
    timeout.tv_sec = 60;
    timeout.tv_usec = 0;
    timeout.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    
    // Now send the actual matrix info
    int info[2] = {rows_for_this_slave, state->n};
    if (send(sock, info, sizeof(info), 0) != sizeof(info)) {
        perror("Failed to send matrix info");
        close(sock);
        pthread_exit(NULL);
    }

    // Rest of the function continues as before...
    printf("Connection to slave %d established successfully.\n", slave);

    // Start timing
    struct timeval time_before, time_after;
    gettimeofday(&time_before, NULL);

    // Send data in chunks
    printf("Sending rows %d to %d to slave %d\n", 
           start_row, start_row + rows_for_this_slave - 1, slave);

    size_t total_bytes_sent = 0; // Track total bytes sent
    int total_chunks = (rows_for_this_slave + CHUNK_SIZE - 1) / CHUNK_SIZE;
    
    for (int i = 0, chunk_num = 0; i < rows_for_this_slave; i += CHUNK_SIZE, chunk_num++) {
        // Show progress every 10th chunk or at beginning/end
        if (chunk_num == 0 || chunk_num == total_chunks-1 || chunk_num % 10 == 0) {
            printf("Slave %d: Sending chunk %d/%d (%.1f%%)\n", 
                slave, chunk_num+1, total_chunks, 
                (chunk_num+1) * 100.0 / total_chunks);
        }
        
        int rows_to_send = (i + CHUNK_SIZE > rows_for_this_slave) ? 
                          (rows_for_this_slave - i) : CHUNK_SIZE;
        int total_bytes = rows_to_send * state->n * sizeof(int);
        total_bytes_sent += total_bytes;

        // Allocate temporary buffer
        int *buffer = malloc(total_bytes);
        for (int j = 0; j < rows_to_send; j++) {
            memcpy(buffer + j * state->n, state->matrix[start_row + i + j], 
                   state->n * sizeof(int));
        }
        
        // Send the chunk
        int bytes_sent = 0;
        while (bytes_sent < total_bytes) {
            int sent = send(sock, (char*)buffer + bytes_sent, total_bytes - bytes_sent, 0);
            if (sent < 0) {
                perror("Failed to send matrix chunk");
                free(buffer);
                exit(EXIT_FAILURE);
            }
            bytes_sent += sent;
        }
        free(buffer);
        // Add delay after sending chunk
        usleep(CHUNK_DELAY_US);
    }

    // End timing
    gettimeofday(&time_after, NULL);

    // Calculate elapsed time
    double elapsed = (time_after.tv_sec - time_before.tv_sec) + 
                     (time_after.tv_usec - time_before.tv_usec) / 1000000.0;

    // Calculate Mbps
    double mbps = (total_bytes_sent * 8) / (elapsed * 1000000.0); // Convert bytes to bits, then to Mbps
    printf("Slave %d: Sent %zu bytes in %.6f seconds (%.2f Mbps)\n", 
           slave, total_bytes_sent, elapsed, mbps);

    // Return success value (non-NULL) to indicate thread completed successfully
    pthread_exit((void*)1);  // Use any non-NULL value
}

// Replace distribute_submatrices with this non-threaded version
void distribute_submatrices_sequential(ProgramState *state) {
    int slave_count = state->t;
    int base_rows_per_slave = state->n / slave_count;
    int extra_rows = state->n % slave_count;
    int start_row = 0;

    printf("\n*** USING SEQUENTIAL (NON-THREADED) DISTRIBUTION ***\n");

    // Allocate memory for the normalized matrix
    double **normalized_matrix = (double **)malloc(state->n * sizeof(double *));
    if (!normalized_matrix) {
        perror("Normalized matrix allocation failed");
        exit(EXIT_FAILURE);
    }
    for (int i = 0; i < state->n; i++) {
        normalized_matrix[i] = (double *)malloc(state->n * sizeof(double));
        if (!normalized_matrix[i]) {
            perror("Normalized matrix row allocation failed");
            exit(EXIT_FAILURE);
        }
    }

    // Track successful slaves
    int slave_success[MAX_SLAVES] = {0};
    int sockets[MAX_SLAVES] = {-1}; // Store socket for each slave
    
    // Process each slave sequentially
    for (int slave = 0; slave < slave_count; slave++) {
        int rows_for_this_slave = base_rows_per_slave + (slave < extra_rows ? 1 : 0);
        
        printf("\n--- Processing Slave %d ---\n", slave);
        printf("Sending data to slave %d at IP %s, Port %d\n", 
               slave, state->slaves[slave].ip, state->slaves[slave].port);
        printf("Rows %d to %d assigned to slave %d\n", 
               start_row, start_row + rows_for_this_slave - 1, slave);
        
        // CONNECT TO SLAVE
        int max_retries = 3;
        int retry_count = 0;
        int sock = -1;
        int connected = 0;
        
        while (retry_count < max_retries && !connected) {
            sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock < 0) {
                perror("Socket creation failed");
                sleep(2);
                retry_count++;
                continue;
            }
            
            // Set socket options
            int flag = 1;
            setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(flag));
            setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));
            
            // Increase buffer sizes
            int send_buf_size = BUFFER_SIZE * 4;
            int recv_buf_size = BUFFER_SIZE * 4;
            setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &send_buf_size, sizeof(send_buf_size));
            setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &recv_buf_size, sizeof(recv_buf_size));
            
            // Set timeouts
            struct timeval timeout;
            timeout.tv_sec = 60;
            timeout.tv_usec = 0;
            setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
            setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
            
            // Connect to slave
            struct sockaddr_in slave_addr;
            memset(&slave_addr, 0, sizeof(slave_addr));
            slave_addr.sin_family = AF_INET;
            slave_addr.sin_port = htons(state->slaves[slave].port);
            inet_pton(AF_INET, state->slaves[slave].ip, &slave_addr.sin_addr);
            
            if (connect(sock, (struct sockaddr *)&slave_addr, sizeof(slave_addr)) < 0) {
                perror("Connection failed");
                close(sock);
                printf("Retrying connection to slave %d (%d/%d)...\n", 
                       slave, retry_count+1, max_retries);
                sleep(5);
                retry_count++;
                continue;
            }
            
            connected = 1;
        }
        
        if (!connected) {
            printf("Failed to connect to slave %d after %d attempts, skipping\n", slave, max_retries);
            // Update start row for next slave and continue
            start_row += rows_for_this_slave;
            continue;
        }
        
        sockets[slave] = sock; // Store socket for later use
        
        // TEST CONNECTION
        printf("Testing connection to slave %d...\n", slave);
        char test_msg[64];
        sprintf(test_msg, "TEST_CONNECTION");
        if (send(sock, test_msg, strlen(test_msg)+1, 0) <= 0) {
            perror("Connection test failed");
            close(sock);
            start_row += rows_for_this_slave;
            continue;
        }
        
        // Set shorter timeout for handshake
        struct timeval short_timeout;
        short_timeout.tv_sec = 5;
        short_timeout.tv_usec = 0;
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &short_timeout, sizeof(short_timeout));
        
        // Wait for ack
        char ack[64];
        memset(ack, 0, sizeof(ack));
        int recv_result = recv(sock, ack, sizeof(ack), 0);
        if (recv_result <= 0) {
            if (recv_result == 0)
                fprintf(stderr, "Connection closed by slave %d during handshake\n", slave);
            else
                perror("Failed to receive test acknowledgment");
            close(sock);
            start_row += rows_for_this_slave;
            continue;
        }
        
        printf("Received acknowledgment from slave %d: %s\n", slave, ack);
        
        // Reset timeout
        timeout.tv_sec = 60;
        timeout.tv_usec = 0;
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        
        // SEND MATRIX INFO
        int info[2] = {rows_for_this_slave, state->n};
        if (send(sock, info, sizeof(info), 0) != sizeof(info)) {
            perror("Failed to send matrix info");
            close(sock);
            start_row += rows_for_this_slave;
            continue;
        }
        
        printf("Connection to slave %d established and info sent successfully\n", slave);
        
        // SEND DATA CHUNKS
        struct timeval time_before, time_after;
        gettimeofday(&time_before, NULL);
        
        size_t total_bytes_sent = 0;
        int total_chunks = (rows_for_this_slave + CHUNK_SIZE - 1) / CHUNK_SIZE;
        
        for (int i = 0, chunk_num = 0; i < rows_for_this_slave; i += CHUNK_SIZE, chunk_num++) {
            // Show progress
            if (chunk_num == 0 || chunk_num == total_chunks-1 || chunk_num % 10 == 0) {
                printf("Slave %d: Sending chunk %d/%d (%.1f%%)\n", 
                    slave, chunk_num+1, total_chunks, 
                    (chunk_num+1) * 100.0 / total_chunks);
            }
            
            int rows_to_send = (i + CHUNK_SIZE > rows_for_this_slave) ? 
                              (rows_for_this_slave - i) : CHUNK_SIZE;
            int total_bytes = rows_to_send * state->n * sizeof(int);
            total_bytes_sent += total_bytes;
            
            // Allocate buffer
            int *buffer = malloc(total_bytes);
            for (int j = 0; j < rows_to_send; j++) {
                memcpy(buffer + j * state->n, state->matrix[start_row + i + j], 
                       state->n * sizeof(int));
            }
            
            // Send chunk
            int bytes_sent = 0;
            while (bytes_sent < total_bytes) {
                int sent = send(sock, (char*)buffer + bytes_sent, total_bytes - bytes_sent, 0);
                if (sent < 0) {
                    perror("Failed to send matrix chunk");
                    free(buffer);
                    close(sock);
                    goto next_slave; // Skip to next slave
                }
                bytes_sent += sent;
            }
            
            free(buffer);
            usleep(CHUNK_DELAY_US);
        }
        
        gettimeofday(&time_after, NULL);
        double elapsed = (time_after.tv_sec - time_before.tv_sec) + 
                         (time_after.tv_usec - time_before.tv_usec) / 1000000.0;
        double mbps = (total_bytes_sent * 8) / (elapsed * 1000000.0);
        printf("Slave %d: Sent %zu bytes in %.6f seconds (%.2f Mbps)\n", 
               slave, total_bytes_sent, elapsed, mbps);
               
        slave_success[slave] = 1;  // Mark this slave as successful
        
        next_slave:
        start_row += rows_for_this_slave;
    }
    
    // RECEIVE RESULTS FROM EACH SUCCESSFUL SLAVE
    start_row = 0;
    for (int slave = 0; slave < slave_count; slave++) {
        int rows_for_this_slave = base_rows_per_slave + (slave < extra_rows ? 1 : 0);
        
        if (!slave_success[slave]) {
            printf("Skipping slave %d as its processing failed\n", slave);
            start_row += rows_for_this_slave;
            continue;
        }
        
        int sock = sockets[slave];
        printf("\nReceiving normalized data from slave %d\n", slave);
        
        // Receive the normalized submatrix in chunks
        int chunk_size = CHUNK_SIZE * state->n * sizeof(double);
        double *buffer = (double *)malloc(chunk_size);
        if (!buffer) {
            perror("Buffer allocation failed");
            close(sock);
            start_row += rows_for_this_slave;
            continue;
        }
        
        for (int i = 0; i < rows_for_this_slave; i += CHUNK_SIZE) {
            // Send request for the next chunk
            char request[16];
            snprintf(request, sizeof(request), "SEND %d", i / CHUNK_SIZE);
            if (send(sock, request, strlen(request) + 1, 0) <= 0) {
                perror("Request send failed");
                free(buffer);
                close(sock);
                break;
            }
            
            // Calculate chunk size
            int rows_to_receive = (i + CHUNK_SIZE > rows_for_this_slave) ? 
                                  (rows_for_this_slave - i) : CHUNK_SIZE;
            int total_bytes = rows_to_receive * state->n * sizeof(double);
            
            int bytes_received = 0;
            while (bytes_received < total_bytes) {
                int received = recv(sock, (char*)buffer + bytes_received, total_bytes - bytes_received, 0);
                if (received <= 0) {
                    perror("Failed to receive normalized matrix chunk");
                    free(buffer);
                    close(sock);
                    goto finish_slave;
                }
                bytes_received += received;
            }
            
            // Copy rows into normalized matrix
            for (int j = 0; j < rows_to_receive; j++) {
                memcpy(normalized_matrix[start_row + i + j], buffer + j * state->n, state->n * sizeof(double));
            }
            
            // Show progress
            if (i % (CHUNK_SIZE * 5) == 0 || i + CHUNK_SIZE >= rows_for_this_slave) {
                printf("Received chunk containing rows %d-%d from slave %d\n",
                       start_row + i, start_row + i + rows_to_receive - 1, slave);
            }
        }
        
        free(buffer);
        
        // Receive final ack
        char ack[4];
        if (recv(sock, ack, sizeof(ack), 0) != sizeof(ack)) {
            perror("Ack receive failed");
        } else {
            printf("Received final ack from slave %d\n", slave);
        }
        
        finish_slave:
        close(sock);
        start_row += rows_for_this_slave;
    }
    
    printf("\nNormalized matrix processing complete\n");
    
    // Free memory
    for (int i = 0; i < state->n; i++) {
        free(normalized_matrix[i]);
    }
    free(normalized_matrix);
}

void slave_listen(ProgramState *state) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Set socket options
    int flag = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(flag));
    
    // Increase buffer size significantly
    int buf_size = BUFFER_SIZE * 4;
    setsockopt(server_fd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));
    setsockopt(server_fd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));
    
    // Set keep-alive to detect connection failures
    setsockopt(server_fd, SOL_SOCKET, SO_KEEPALIVE, &flag, sizeof(int));

    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(state->p);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("Bind failed");
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 3) < 0) {
        perror("Listen failed");
        exit(EXIT_FAILURE);
    }

    printf("Slave listening on port %d...\n", state->p);

    int addrlen = sizeof(address);
    int master_sock = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen);
    if (master_sock < 0) {
        perror("Accept failed");
        exit(EXIT_FAILURE);
    }

    printf("Master connection accepted\n");
    
    // Handle connection test
    char test_msg[64];
    memset(test_msg, 0, sizeof(test_msg));
    
    int test_received = recv(master_sock, test_msg, sizeof(test_msg), 0);
    if (test_received <= 0) {
        perror("Failed to receive test message");
        close(master_sock);
        close(server_fd);
        exit(EXIT_FAILURE);
    }
    
    printf("Received test message: %s\n", test_msg);
    
    // Send acknowledgment back
    char ack[] = "TEST_ACK";
    if (send(master_sock, ack, strlen(ack) + 1, 0) <= 0) {
        perror("Failed to send test acknowledgment");
        close(master_sock);
        close(server_fd);
        exit(EXIT_FAILURE);
    }
    
    printf("Test acknowledgment sent\n");
    
    // Now receive the actual matrix info
    int info[2];
    if (recv(master_sock, info, sizeof(info), 0) != sizeof(info)) {
        perror("Failed to receive matrix info");
        exit(EXIT_FAILURE);
    }
    int rows = info[0];
    int cols = info[1];

    printf("Slave received matrix size: %d rows x %d cols\n", rows, cols);

    // Allocate memory for submatrix
    int **submatrix = (int **)malloc(rows * sizeof(int *));
    if (!submatrix) {
        perror("Submatrix allocation failed");
        exit(EXIT_FAILURE);
    }
    for (int i = 0; i < rows; i++) {
        submatrix[i] = (int *)malloc(cols * sizeof(int));
        if (!submatrix[i]) {
            perror("Submatrix row allocation failed");
            exit(EXIT_FAILURE);
        }
    }

    // Receive the submatrix data in chunks
    printf("Slave beginning to receive data in chunks...\n");
    int *row_buffer = (int *)malloc(cols * sizeof(int));
    if (!row_buffer) {
        perror("Buffer allocation failed");
        exit(EXIT_FAILURE);
    }
    
    for (int i = 0; i < rows; i++) {
        int bytes_received = 0;
        int bytes_to_receive = cols * sizeof(int);
        
        while (bytes_received < bytes_to_receive) {
            int received = recv(master_sock, 
                               (char*)row_buffer + bytes_received, 
                               bytes_to_receive - bytes_received, 
                               0);
                               
            if (received <= 0) {
                perror("Failed to receive matrix row");
                exit(EXIT_FAILURE);
            }
            bytes_received += received;
        }
        
        // Copy the received data to the submatrix
        memcpy(submatrix[i], row_buffer, bytes_to_receive);
        
        // Print progress occasionally
        if (i % 100 == 0 || i == rows-1) {
            printf("Received %d/%d rows (%.1f%%)\n", 
                  i+1, rows, (i+1)*100.0/rows);
        }
    }
    
    free(row_buffer);

    printf("Slave finished receiving data from master.\n");

    // Start timing for Min-Max Transformation
    struct timeval mmt_start, mmt_end;
    gettimeofday(&mmt_start, NULL);

    // Allocate memory for normalized matrix
    double **normalized_matrix = (double **)malloc(rows * sizeof(double *));
    if (!normalized_matrix) {
        perror("Normalized matrix allocation failed");
        exit(EXIT_FAILURE);
    }
    for (int i = 0; i < rows; i++) {
        normalized_matrix[i] = (double *)malloc(cols * sizeof(double));
        if (!normalized_matrix[i]) {
            perror("Normalized matrix row allocation failed");
            exit(EXIT_FAILURE);
        }
    }

    // Perform Min-Max Transformation (MMT computation)
    for (int i = 0; i < rows; i++) {
        int min_val = submatrix[i][0];
        int max_val = submatrix[i][0];
        for (int j = 1; j < cols; j++) {
            if (submatrix[i][j] < min_val) min_val = submatrix[i][j];
            if (submatrix[i][j] > max_val) max_val = submatrix[i][j];
        }

        for (int j = 0; j < cols; j++) {
            if (max_val == min_val) {
                normalized_matrix[i][j] = 0.0; // Avoid division by zero
            } else {
                normalized_matrix[i][j] = (double)(submatrix[i][j] - min_val) / (max_val - min_val);
            }
        }
    }

    // End timing for Min-Max Transformation
    gettimeofday(&mmt_end, NULL);
    double mmt_elapsed = (mmt_end.tv_sec - mmt_start.tv_sec) + 
                         (mmt_end.tv_usec - mmt_start.tv_usec) / 1000000.0;
    
    printf("Min-Max Transformation completed in %.6f seconds for %d×%d matrix\n", 
           mmt_elapsed, rows, cols);

    printf("Slave normalized matrix:\n");

    // Send the normalized submatrix back to the master in chunks
    int chunk_size = CHUNK_SIZE * cols * sizeof(double); // Chunk size in bytes
    double *buffer = (double *)malloc(chunk_size);
    if (!buffer) {
        perror("Buffer allocation failed");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < rows; i += CHUNK_SIZE) {
        // Wait for master's request
        char request[16];
        if (recv(master_sock, request, sizeof(request), 0) <= 0) {
            perror("Request receive failed");
            free(buffer);
            exit(EXIT_FAILURE);
        }
    
        // Parse the request (optional, for debugging)
        printf("Slave received request: %s\n", request);
    
        // Prepare the chunk to send
        int rows_to_send = (i + CHUNK_SIZE > rows) ? (rows - i) : CHUNK_SIZE;
        int total_bytes = rows_to_send * cols * sizeof(double);
    
        for (int j = 0; j < rows_to_send; j++) {
            memcpy(buffer + (j * cols), normalized_matrix[i + j], cols * sizeof(double));
        }
    
        // Send the chunk
        int bytes_sent = 0;
        while (bytes_sent < total_bytes) {
            int sent = send(master_sock, (char*)buffer + bytes_sent, total_bytes - bytes_sent, 0);
            if (sent <= 0) {
                perror("Failed to send normalized matrix chunk");
                free(buffer);
                exit(EXIT_FAILURE);
            }
            bytes_sent += sent;
        }
    }

    printf("Slave finished sending normalized data to master.\n");

    // Send acknowledgment
    if (send(master_sock, "ack", 4, 0) != 4) {
        perror("Failed to send acknowledgment");
        exit(EXIT_FAILURE);
        }

    free(buffer);

    // Free allocated memory
    for (int i = 0; i < rows; i++) {
        free(submatrix[i]);
        free(normalized_matrix[i]);
    }
    free(submatrix);
    free(normalized_matrix);

    close(master_sock);
    close(server_fd);
}

// Add this function before main() around line 532
int calculate_optimal_chunk_size(int matrix_size) {
    if (matrix_size <= 1000) return 32;
    else if (matrix_size <= 5000) return 64;
    else if (matrix_size <= 10000) return 128;
    else return 256;
}

// Add before main():

void check_network_connectivity(ProgramState *state) {
    printf("\nChecking network connectivity to slaves...\n");
    
    for (int i = 0; i < state->t; i++) {
        printf("Checking slave %d at %s:%d... ", i, state->slaves[i].ip, state->slaves[i].port);
        fflush(stdout);
        
        // Create socket for basic test
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            perror("Socket creation failed");
            continue;
        }
        
        // Set short timeout
        struct timeval timeout;
        timeout.tv_sec = 2;
        timeout.tv_usec = 0;
        setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
        
        // Try to connect
        struct sockaddr_in slave_addr;
        memset(&slave_addr, 0, sizeof(slave_addr));
        slave_addr.sin_family = AF_INET;
        slave_addr.sin_port = htons(state->slaves[i].port);
        inet_pton(AF_INET, state->slaves[i].ip, &slave_addr.sin_addr);
        
        if (connect(sock, (struct sockaddr *)&slave_addr, sizeof(slave_addr)) < 0) {
            perror("Failed");
        } else {
            printf("Success\n");
        }
        
        close(sock);
    }
    printf("\n");
}

int main(int argc, char *argv[]) {
    if (argc != 4 && argc != 5) {
        printf("Usage: %s <matrix_size> <port> <status (0=master, 1=slave)> [slave_count]\n", argv[0]);
        return EXIT_FAILURE;
    }

    ProgramState state;
    state.n = atoi(argv[1]);
    state.p = atoi(argv[2]);
    state.s = atoi(argv[3]);
    state.matrix = NULL;
    state.t = 0;

    if (state.n <= 0) {
        printf("Invalid matrix size. Must be positive\n");
        return EXIT_FAILURE;
    }

    if (state.s == 0) {
        if (argc == 5) {
            state.t = atoi(argv[4]);
        } else {
            printf("Error: Master requires slave count parameter\n");
            return EXIT_FAILURE;
        }
        
        printf("Running as master with %d slaves\n", state.t);
        
        read_config(&state, state.t);

        // Call this in main() after reading config but before distributing work:
        check_network_connectivity(&state);

        allocate_matrix(&state);
        create_matrix(&state);

        // Print the original matrix
        //printf("Master created original matrix:\n");
        //print_matrix(state.original_matrix, state.n, state.n);

        // Add before distribute_submatrices() call in main() (around line 575)
        // Calculate optimal chunk size based on matrix dimensions
        int optimal_chunk = calculate_optimal_chunk_size(state.n);
        printf("Using optimal chunk size of %d for matrix size %d\n", optimal_chunk, state.n);
        
        // Start timing the entire process
        struct timeval total_time_before, total_time_after;
        gettimeofday(&total_time_before, NULL);

        distribute_submatrices_sequential(&state);

        // End timing the entire process
        gettimeofday(&total_time_after, NULL);
        double total_elapsed = (total_time_after.tv_sec - total_time_before.tv_sec) + 
                               (total_time_after.tv_usec - total_time_before.tv_usec) / 1000000.0;

        printf("Total time from sending to rebuilding normalized matrix: %.6f seconds\n", total_elapsed);

        free_matrix(&state);
    } else {
        slave_listen(&state);
    }

    return EXIT_SUCCESS;
}


