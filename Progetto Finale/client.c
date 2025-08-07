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


    // 5. Ricezione della risposta
    int n = recv(sockfd, buffer, DIM_CHUNK - 1, 0);
    if (n <= 0) {
        if (n == 0) 
            fprintf(stderr, "Connessione chiusa dal server prima di ricevere dati.\n");
        else 
            perror("recv fallita");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    buffer[n] = '\0';  // Aggiungi terminatore di stringa

    Blocco_Parole blocco = Map(buffer);

    printf("ATTENTO QUA contatore: %d, parola: %s\n", blocco.lunghezza_contatore, blocco.struttura_parole[0].parola);
    printf("Contatore: %d\n", blocco.lunghezza_contatore);

    printf("siamo prima del ciclo di invio \n");

    for (int i = 0; i < blocco.lunghezza_contatore; i++) {
        WordCount *w = &blocco.struttura_parole[i];

        int len = strlen(w->parola) + 1; // include terminatore \0
        int len_net = htonl(len);
        int cont_net = htonl(w->contatore);

        

        // 1. Invio lunghezza parola
        ssize_t bytes_inviati = 0;
        while (bytes_inviati < (ssize_t)sizeof(len_net)) {
            ssize_t r = send(sockfd, ((char*)&len_net) + bytes_inviati, sizeof(len_net) - bytes_inviati, 0);
            if (r <= 0) {
                perror("send fallita lunghezza parola");
                goto fine;
            }
            bytes_inviati += r;
        }

        if (w->parola == NULL) {
            fprintf(stderr, "ERRORE: parola NULL a indice %d\n", i);
            continue;
        }

        

        // 2. Invio parola (stringa)
        bytes_inviati = 0;
        while (bytes_inviati < (ssize_t)len) {
            ssize_t r = send(sockfd, w->parola + bytes_inviati, len - bytes_inviati, 0);
            if (r <= 0) {
                perror("send fallita parola");
                goto fine;
            }
            bytes_inviati += r;
        }

        // 3. Invio contatore
        bytes_inviati = 0;
        while (bytes_inviati < (ssize_t)sizeof(cont_net)) {
            ssize_t r = send(sockfd, ((char*)&cont_net) + bytes_inviati, sizeof(cont_net) - bytes_inviati, 0);
            if (r <= 0) {
                perror("send fallita contatore");
                goto fine;
            }
            bytes_inviati += r;
        }
    }

fine:
    close(sockfd);
    return 0;
}
