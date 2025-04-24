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
#define BUFFER_SIZE (1 * 1024 * 1024)  // 1MB buffer
#define CONFIG_FILE "config.txt"
#define CHUNK_SIZE 10              // Rows per chunk

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
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < state->n; i++) {
        state->original_matrix[i] = (int *)malloc(state->n * sizeof(int));
        if (!state->original_matrix[i]) {
            perror("Original matrix row allocation failed");
            exit(EXIT_FAILURE);
        }

        state->matrix[i] = (int *)malloc(state->n * sizeof(int));
        if (!state->matrix[i]) {
            perror("Normalized matrix row allocation failed");
            exit(EXIT_FAILURE);
        }
    }
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

    printf("Sending data to slave %d at IP %s, Port %d\n", 
           slave, state->slaves[slave].ip, state->slaves[slave].port);

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }
    // Store the socket in args
    args->sock = sock;

    // Set TCP_NODELAY and buffer sizes
    int flag = 1;
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(int));
    int buf_size = BUFFER_SIZE;
    setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));

    struct sockaddr_in slave_addr;
    memset(&slave_addr, 0, sizeof(slave_addr));
    slave_addr.sin_family = AF_INET;
    slave_addr.sin_port = htons(state->slaves[slave].port);
    inet_pton(AF_INET, state->slaves[slave].ip, &slave_addr.sin_addr);

    if (connect(sock, (struct sockaddr *)&slave_addr, sizeof(slave_addr)) < 0) {
        perror("Connection failed");
        exit(EXIT_FAILURE);
    }

    // Send submatrix size info
    int info[2] = {rows_for_this_slave, state->n};
    if (send(sock, info, sizeof(info), 0) != sizeof(info)) {
        perror("Failed to send matrix info");
        exit(EXIT_FAILURE);
    }

    // Start timing
    struct timeval time_before, time_after;
    gettimeofday(&time_before, NULL);

    // Send data in chunks
    printf("Sending rows %d to %d to slave %d\n", 
           start_row, start_row + rows_for_this_slave - 1, slave);

    size_t total_bytes_sent = 0; // Track total bytes sent

    for (int i = 0; i < rows_for_this_slave; i += CHUNK_SIZE) {
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
        if (send(sock, buffer, total_bytes, 0) != total_bytes) {
            perror("Failed to send matrix chunk");
            free(buffer);
            exit(EXIT_FAILURE);
        }
        free(buffer);

        
        // double delay_in_seconds = (double)total_bytes / 6062500; // 1,875,000 bytes per second
        // usleep((useconds_t)(delay_in_seconds * 1e6)); // Convert seconds to microseconds
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

    // Wait for acknowledgment
    char ack[4];
    if (recv(sock, ack, sizeof(ack), 0) != sizeof(ack)) {
        perror("Failed to receive acknowledgment");
        exit(EXIT_FAILURE);
    }

    if (strcmp(ack, "ack") != 0) {
        fprintf(stderr, "Did not receive proper acknowledgment from slave %d\n", slave);
        exit(EXIT_FAILURE);
    }

    return NULL;
}

void distribute_submatrices(ProgramState *state) {
    int slave_count = state->t;
    int base_rows_per_slave = state->n / slave_count;
    int extra_rows = state->n % slave_count;
    int start_row = 0;

    pthread_t threads[MAX_SLAVES];
    ThreadArgs args[MAX_SLAVES];

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

    for (int slave = 0; slave < slave_count; slave++) {
        args[slave].state = state;
        args[slave].slave_index = slave;
        args[slave].start_row = start_row;
        args[slave].rows_for_this_slave = base_rows_per_slave + (slave < extra_rows ? 1 : 0);
        
        pthread_create(&threads[slave], NULL, send_to_slave, &args[slave]);
        
        start_row += args[slave].rows_for_this_slave;
    }

    // Wait for all threads to complete
    for (int slave = 0; slave < slave_count; slave++) {
        pthread_join(threads[slave], NULL);
    }

    // Receive normalized submatrices from slaves using the stored sockets
    start_row = 0;
    for (int slave = 0; slave < slave_count; slave++) {
        int rows_for_this_slave = base_rows_per_slave + (slave < extra_rows ? 1 : 0);
        int sock = args[slave].sock;  // Use the stored socket

        // Receive the normalized submatrix in chunks
        int chunk_size = CHUNK_SIZE * state->n * sizeof(double);
        double *buffer = (double *)malloc(chunk_size);
        if (!buffer) {
            perror("Buffer allocation failed");
            exit(EXIT_FAILURE);
        }

        for (int i = 0; i < rows_for_this_slave; i += CHUNK_SIZE) {
            int rows_to_receive = (i + CHUNK_SIZE > rows_for_this_slave) ? 
                                (rows_for_this_slave - i) : CHUNK_SIZE;
            int total_bytes = rows_to_receive * state->n * sizeof(double);

            // Receive the chunk
            if (recv(sock, buffer, total_bytes, 0) != total_bytes) {
                perror("Failed to receive normalized matrix chunk");
                free(buffer);
                exit(EXIT_FAILURE);
            }

            // Copy rows from the buffer into the normalized matrix
            for (int j = 0; j < rows_to_receive; j++) {
                memcpy(normalized_matrix[start_row + i + j], 
                      buffer + j * state->n, 
                      state->n * sizeof(double));
            }
        }

        free(buffer);
        close(sock);  // Now we can close the socket
        start_row += rows_for_this_slave;
    }

    // Print the final normalized matrix
    printf("Normalized matrix:\n");
    for (int i = 0; i < state->n; i++) {
        for (int j = 0; j < state->n; j++) {
            printf("%.2f ", normalized_matrix[i][j]);
        }
        printf("\n");
    }

    // Free the normalized matrix
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
    int buf_size = BUFFER_SIZE;
    setsockopt(server_fd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));

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

    // Receive submatrix size info
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
    for (int i = 0; i < rows; i++) {
        if (recv(master_sock, submatrix[i], cols * sizeof(int), 0) != cols * sizeof(int)) {
            perror("Failed to receive matrix row");
            exit(EXIT_FAILURE);
        }
    }

    printf("Slave finished receiving data from master.\n");

    // Send acknowledgment
    if (send(master_sock, "ack", 4, 0) != 4) {
    perror("Failed to send acknowledgment");
    exit(EXIT_FAILURE);
    }

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

    printf("Slave normalized matrix:\n");
    for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
            printf("%.2f ", normalized_matrix[i][j]);
        }
        printf("\n");
    }

    // Send the normalized submatrix back to the master in chunks
    int chunk_size = CHUNK_SIZE * cols * sizeof(double); // Chunk size in bytes
    double *buffer = (double *)malloc(chunk_size);
    if (!buffer) {
        perror("Buffer allocation failed");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < rows; i += CHUNK_SIZE) {
        int rows_to_send = (i + CHUNK_SIZE > rows) ? (rows - i) : CHUNK_SIZE;
        int total_bytes = rows_to_send * cols * sizeof(double);

        // Copy rows into the buffer
        for (int j = 0; j < rows_to_send; j++) {
            memcpy(buffer + j * cols, normalized_matrix[i + j], cols * sizeof(double));
        }

        // Send the chunk
        if (send(master_sock, buffer, total_bytes, 0) != total_bytes) {
            perror("Failed to send normalized matrix chunk");
            free(buffer);
            exit(EXIT_FAILURE);
        }
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
        allocate_matrix(&state);
        create_matrix(&state);

        // Print the original matrix
        printf("Master created original matrix:\n");
        print_matrix(state.original_matrix, state.n, state.n);

        // Start timing the entire process
        struct timeval total_time_before, total_time_after;
        gettimeofday(&total_time_before, NULL);

        distribute_submatrices(&state);

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


