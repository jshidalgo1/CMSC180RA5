# CMSC180RA5 - Distributed Min-Max Matrix Normalization

This program demonstrates distributed matrix normalization using a client-server architecture.

## Overview

The program implements a distributed system where:
- The server distributes portions of a matrix to multiple clients
- Clients perform min-max normalization on their assigned portions
- Normalized results are sent back to the server and combined

## Requirements

- GCC compiler
- POSIX-compliant system (Linux, macOS)
- Multiple machines connected on a network (or can be run on a single machine with different ports)
- Sufficient RAM (see hardware considerations below)

## Hardware Considerations

**Important:** This application requires significant memory resources:

- Running the code on drone swarms will not work since they typically have only 2GB RAM
- For testing with matrix sizes of n=1000 or larger, local testing will require high RAM (64GB or higher)
- Best results are achieved using PC Lab computers or equivalent systems with sufficient memory
- For large matrices, distribute the work across multiple machines to avoid memory overload errors

## Compilation

To compile the program:

```bash
gcc -o matrix_normalizer hidalgo_lab5.c -pthread
```

## Configuration

Before running the server, create a `config.txt` file in the same directory with the IP addresses and ports of all client machines:

```
192.168.1.101 5001
192.168.1.102 5002
192.168.1.103 5003
```

For local testing, you can use:

```
127.0.0.1 5001
127.0.0.1 5002
127.0.0.1 5003
```

## Running the Program

### Client Mode

Run the client on each machine (or in separate terminals for local testing):

```bash
./matrix_normalizer <matrix_size> <port> 1
```

Example:
```bash
./matrix_normalizer 5000 5001 1
```

This starts a client that will listen on port 5001 and wait for the server to connect.

### Server Mode

After starting all clients, run the server:

```bash
./matrix_normalizer <matrix_size> <port> 0
```

Example:
```bash
./matrix_normalizer 5000 5000 0
```

The server will:
1. Read the client configuration from `config.txt`
2. Create a random matrix of the specified size
3. Connect to all available clients
4. Distribute portions of the matrix to each client
5. Collect normalized results and combine them
6. Display timing information and a sample of the normalized matrix

## Notes

- The third argument specifies the mode (0 = server, 1 = client)
- The matrix size argument specifies the dimensions of the square matrix (N×N)
- For the server, the port argument is not used but is required as a placeholder
- For clients, the port argument specifies which port to listen on

## Example Workflow (Local Testing)

1. Open 3 terminal windows
2. Create `config.txt` with local addresses:
   ```
   127.0.0.1 5001
   127.0.0.1 5002
   127.0.0.1 5003
   ```
3. Start 3 clients:
   ```bash
   # Terminal 1
   ./matrix_normalizer 5000 5001 1
   
   # Terminal 2
   ./matrix_normalizer 5000 5002 1
   
   # Terminal 3
   ./matrix_normalizer 5000 5003 1
   ```
4. Start the server in a 4th terminal:
   ```bash
   ./matrix_normalizer 5000 5000 0
   ```