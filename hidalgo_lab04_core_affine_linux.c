#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <sched.h> // For sched_setaffinity

#define MAX_SLAVES 16
#define BUFFER_SIZE 1024
#define CONFIG_FILE "config.txt"

typedef struct {
    char ip[16];
    int port;
} SlaveInfo;

typedef struct {
    int n;       // Matrix size
    int p;       // Port number
    int s;       // Status (0=master, 1=slave)
    int t;       // Number of slaves
    SlaveInfo slaves[MAX_SLAVES];
    int **matrix;
} ProgramState;

// Function to read slave configuration from config.txt
void read_config(ProgramState *state, int required_slaves) {
    FILE *file = fopen(CONFIG_FILE, "r");
    if (!file) {
        perror("Failed to open config file");
        exit(EXIT_FAILURE);
    }

    state->t = 0;
    char line[100];
    while (fgets(line, sizeof(line), file) && state->t < required_slaves) {
        sscanf(line, "%s %d", state->slaves[state->t].ip, &state->slaves[state->t].port);
        state->t++;
    }
    fclose(file);
}

// Allocate memory for the matrix
void allocate_matrix(ProgramState *state) {
    state->matrix = (int **)malloc(state->n * sizeof(int *));
    if (!state->matrix) {
        perror("Matrix allocation failed");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < state->n; i++) {
        state->matrix[i] = (int *)malloc(state->n * sizeof(int));
        if (!state->matrix[i]) {
            perror("Matrix row allocation failed");
            exit(EXIT_FAILURE);
        }
    }
}

// Free the allocated memory for the matrix
void free_matrix(ProgramState *state) {
    if (state->matrix) {
        for (int i = 0; i < state->n; i++) {
            free(state->matrix[i]);
        }
        free(state->matrix);
    }
}

// Initialize the matrix with random values
void create_matrix(ProgramState *state) {
    srand(time(NULL));
    for (int i = 0; i < state->n; i++) {
        for (int j = 0; j < state->n; j++) {
            state->matrix[i][j] = rand() % 100 + 1; // Random number between 1 and 100
        }
    }
}

// Distribute submatrices to slaves
void distribute_submatrices(ProgramState *state) {
    struct timeval time_before, time_after;
    gettimeofday(&time_before, NULL);

    int slave_count = state->t;  // Get slave count from state
    int base_rows_per_slave = state->n / slave_count;
    int extra_rows = state->n % slave_count;  // Remaining rows to distribute

    int start_row = 0;

    for (int slave = 0; slave < slave_count; slave++) {
        int rows_for_this_slave = base_rows_per_slave + (slave < extra_rows ? 1 : 0);  // Add 1 row for the first 'extra_rows' slaves
        printf("Sending data to slave %d at IP %s, Port %d\n", slave, state->slaves[slave].ip, state->slaves[slave].port);

        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            perror("Socket creation failed");
            exit(EXIT_FAILURE);
        }

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

        // Send the submatrix data in chunks
        printf("Sending rows %d to %d to slave %d\n", start_row, start_row + rows_for_this_slave - 1, slave);
        for (int i = 0; i < rows_for_this_slave; i++) {
            int *row = state->matrix[start_row + i];

            ssize_t bytes_sent = 0;
            ssize_t total_bytes = state->n * sizeof(int);
            while (bytes_sent < total_bytes) {
                ssize_t result = send(sock, (char *)row + bytes_sent, total_bytes - bytes_sent, 0);
                if (result < 0) {
                    perror("Failed to send matrix row");
                    exit(EXIT_FAILURE);
                }
                bytes_sent += result;
            }
        }

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

        close(sock);

        // Update start_row for the next slave
        start_row += rows_for_this_slave;
    }

    gettimeofday(&time_after, NULL);
    double elapsed = (time_after.tv_sec - time_before.tv_sec) + 
                    (time_after.tv_usec - time_before.tv_usec) / 1000000.0;
    printf("Master elapsed time: %.6f seconds\n", elapsed);
}

// Set CPU affinity for the current process
void set_core_affinity(int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    if (sched_setaffinity(0, sizeof(cpu_set_t), &cpuset) != 0) {
        perror("Failed to set CPU affinity");
    } else {
        printf("Process bound to core %d\n", core_id);
    }
}

void slave_listen(ProgramState *state) {
    printf("Slave on port %d starting...\n", state->p);

    // Set CPU affinity for this slave process
    int core_id = state->p % sysconf(_SC_NPROCESSORS_ONLN); // Map port to core
    set_core_affinity(core_id);

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

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

    // Receive the submatrix data
    for (int i = 0; i < rows; i++) {
        ssize_t bytes_received = 0;
        ssize_t total_bytes = cols * sizeof(int);
        while (bytes_received < total_bytes) {
            ssize_t result = recv(master_sock, (char *)submatrix[i] + bytes_received, 
                                 total_bytes - bytes_received, 0);
            if (result < 0) {
                perror("Failed to receive matrix row");
                exit(EXIT_FAILURE);
            }
            bytes_received += result;
        }
    }

    // Send acknowledgment
    if (send(master_sock, "ack", 4, 0) != 4) {
        perror("Failed to send acknowledgment");
        exit(EXIT_FAILURE);
    }

    printf("Slave processed %d rows\n", rows);

    // Clean up
    for (int i = 0; i < rows; i++) {
        free(submatrix[i]);
    }
    free(submatrix);
    close(master_sock);
    close(server_fd);
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        printf("Usage: %s <matrix_size> <port> <status (0=master, 1=slave)> [slave_count]\n", argv[0]);
        return EXIT_FAILURE;
    }

    ProgramState state;
    state.n = atoi(argv[1]);
    state.p = atoi(argv[2]);
    state.s = atoi(argv[3]);
    state.matrix = NULL;

    if (state.s == 0) {
        // Master logic
        if (argc == 5) {
            state.t = atoi(argv[4]);
        } else {
            printf("Error: Master requires slave count parameter\n");
            return EXIT_FAILURE;
        }

        printf("Running as master with %d slaves\n", state.t);

        // Read slave configuration from config.txt
        read_config(&state, state.t);

        // Master logic for matrix allocation and distribution
        allocate_matrix(&state);
        create_matrix(&state);
        distribute_submatrices(&state);
        free_matrix(&state);
    } else {
        // Slave logic
        slave_listen(&state);
    }

    return EXIT_SUCCESS;
}