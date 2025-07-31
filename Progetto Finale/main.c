#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>      // close(), read(), write()
#include <arpa/inet.h>   // inet_ntoa, htons, sockaddr_in
#include <sys/socket.h>  // socket(), bind(), listen(), accept()
#include <netinet/in.h>  // struct sockaddr_in
#include "MapReduce.h" //file di libreria del progetto

#define PORTA 8080
#define MAX_CLIENT 10

//il primo passo è quello di prendere il file e suddividerlo in chunk da 512 byte
//Essendo che eseguendo una divisione netta potremmo tagliare una parola a metà
//tagliamo fino all'ultimo spazio prima del 512 byte


int main(){
    char** Collezione_chunk=malloc(DIM_CHUNK+1); //Alloco lo spazio per contenere almeno un chunk
    chuck(&Collezione_chunk);

    //creiamo il server TCP
    int server_fd, client_fd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_len=sizeof(client_addr);

    //AF_INET-> IPv4
    //SOCK_STREAM → TCP
    //0 -> Protocollo automatico (TCP)
    server_fd= socket(AF_INET, SOCK_STEAM, 0);
    if(server_fd < 0){
        perror("Socket fallita");
        exit(EXIT_FAILURE);
    }

    memset(&server_addr,0,sizeof(server_addr)); //azzera tutti i byte della struttura server_addr
    server_addr.sin_family=AF_INET; //usiamo indirizzi IPv4
    server_addr.sin_addr.s_addr=INADDR_ANY; //ascolta su tutte le interfacce di rete disponibili
    server_addr.sin_port= htons(PORTA); //imposta la porta del server,8080.La htons() converte da host byte order a network byte order, cioè da little endian a big endian

    //colleghiamo la socket all'indirizzo e alla porta
    if(bind(server_fd,(struct sockaddr*)&server_addr,sizeof(server_addr))<0){
        perror("Errore nel bind");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    //mettiamo la socket in ascolto
    if(listen(server_fd,MAX_CLIENT)<0){
        perror("Errore nella listen");
        close(server_fd);
        exit(EXIT_FAILURE); 
    }
    printf("Server in ascolto sulla porta %d...\n", PORTA);
    //la accept scrive le info del client dentro la struct sockaddr
    //client_len contiene la dimensione della struct sockaddr che è client_addr
    //la accept restituisce un file descriptor , che identifica la socket di comunicazione con quel client
    //accept blocca l'esecuzione inchè un client non si connette
    //quando un client si connette la socket di ascolto continua ad ascoltare per altre connessioni
    //quindi per ogni client che si connette dobbiamo creare un thread
    //se gestissimo i client connessi nel thread principale si bloccherebbe tutto
    while(1){
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
            perror("Errore nella accept");
            close(server_fd);
            exit(EXIT_FAILURE);
        }

        printf("Connessione accettata da %s:%d\n",inet_ntoa(client_addr.sin_addr),ntohs(client_addr.sin_port));
    }
}