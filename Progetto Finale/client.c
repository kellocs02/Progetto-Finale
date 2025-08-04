#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>      // close(), read(), write()
#include <arpa/inet.h>   // inet_ntoa, htons, sockaddr_in
#include <sys/socket.h>  // socket(), bind(), listen(), accept()
#include <netinet/in.h>  // struct sockaddr_in
#include <pthread.h>
#include "MapReduce.h" //file di libreria del progetto

#define SERVER_IP "127.0.0.1"   // IP del server
#define SERVER_PORT 8080       // Porta del server


int main() {
    int sockfd;
    struct sockaddr_in server_addr;
    char buffer[DIM_CHUNK];
    char messaggio[] = "Ciao, server!\n";

    // 1. Creazione socket
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("Errore nella creazione della socket");
        exit(EXIT_FAILURE);
    }

    // 2. Configurazione indirizzo del server
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);

    if (inet_pton(AF_INET, SERVER_IP, &server_addr.sin_addr) <= 0) {
        perror("Indirizzo IP non valido");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    // 3. Connessione al server
    if (connect(sockfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("Errore nella connessione");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    printf("Connesso al server %s:%d\n", SERVER_IP, SERVER_PORT);

    // 4. Invio del messaggio al server
    send(sockfd, messaggio, strlen(messaggio), 0);

    // 5. Ricezione della risposta
    //possiamo ricevere più chunk
    //dobbiamo gestire ogni chunk ricevuto
    //dobbiamo modulare i tempi di invio e di ricezione dei chunk
    int n = recv(sockfd, buffer, DIM_CHUNK - 1, 0);
    if (n > 0) {
        buffer[n] = '\0';  // Aggiungi terminatore di stringa
        printf("Risposta dal server: %s\n", buffer);
        Map(buffer);
    } else if (n == 0) {
        printf("Connessione chiusa dal server.\n");
    } else {
        perror("Errore nella ricezione");
    }

    // 6. Chiusura della connessione
    close(sockfd);

    return 0;
}