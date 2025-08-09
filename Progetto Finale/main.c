#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>      // close(), read(), write()
#include <arpa/inet.h>   // inet_ntoa, htons, sockaddr_in
#include <sys/socket.h>  // socket(), bind(), listen(), accept()
#include <netinet/in.h>  // struct sockaddr_in
#include <pthread.h>
#include "MapReduce.h" //file di libreria del progetto

#define PORTA 8080


//il primo passo è quello di prendere il file e suddividerlo in chunk da 512 byte
//Essendo che eseguendo una divisione netta potremmo tagliare una parola a metà
//tagliamo fino all'ultimo spazio prima del 512 byte

int indice_assegnazione_chunk=0; //rappresenta il client

int main(){
    WordCount* risultati[MAX_CLIENT]; //mettiamo i valori di ritorno dei thread
    printf("ciao\n");
    printf("Inizio Programma\n");
    pthread_t thread[MAX_CLIENT]; //creiamo il pool di thread
    int contatore_thread=0;
    int numero_chunk=0;
    printf("numero chunk: %d\n",numero_chunk);
    char** Collezione_chunk=NULL;//Alloco lo spazio per contenere almeno un chunk
    chunk(&Collezione_chunk,&numero_chunk);
    printf("Prima di StampaChunk numero chunk: %d\n",numero_chunk);
    StampaChunk(Collezione_chunk,numero_chunk);
    //Dopo questa istruzione abbiamo il numero di chunk
    for(int i=0; i<numero_chunk;i++){
        printf("Chunk %d: %s\n",i,Collezione_chunk[i]);
    }

    //decidiamo qua come redistribuire i chunk tra i client.
    //impostiamo che il servizio per funzionare deve far si che si colleghino tutti i client, quindi MAX_CLIENT
    //redistribuiamo questi chunk su questi client
    int indice_Di_Redistribuzione=0; //questa variabile viene utilizzata prima della fase di creazione del thread per gestire la distribuzione dei chunk sui client.Rappresenta il chunk attuale da assegnare
    int Chunk_Per_Client=0;
    if(numero_chunk>MAX_CLIENT){
        if(numero_chunk%MAX_CLIENT==0){
            Chunk_Per_Client=numero_chunk/MAX_CLIENT;
        }else{
            Chunk_Per_Client=-1; //gestiamo in altro modo questa situazione
        }
    }

    //creiamo il server TCP
    int server_fd, client_fd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_len=sizeof(client_addr);

    //AF_INET-> IPv4
    //SOCK_STREAM → TCP
    //0 -> Protocollo automatico (TCP)
    server_fd= socket(AF_INET, SOCK_STREAM, 0);
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
    //Imponiamo (per ora) il massimo di client connessi al numero di chunk
    //ma non è la soluzione ottimale
    //se numero di chunk troppo elevato grandi rischi di gestione per il server
    //dobbiamo trovare un'altra soluzione
    //edit rimettiamo MAXCLIENT
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
    Struttura_Chunk *S_Chunk= malloc(MAX_CLIENT*(sizeof(Struttura_Chunk))); //puntatore che punta all'area di memoria dove saranno allocate le strutture chunk
    while(indice_assegnazione_chunk < MAX_CLIENT){
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
            perror("Errore nella accept");
            close(server_fd);
            exit(EXIT_FAILURE);
        }

        printf("Connessione accettata da %s:%d\n",inet_ntoa(client_addr.sin_addr),ntohs(client_addr.sin_port));
        //dobbiamo decidere come gestire le connessioni, se mettere un limite in base ai chunk. 
        //Cosa succede se abbiamo più chunk rispetto ai client connessi?
        //Cosa succede se abbiamo meno chunk rispetto ai client connessi?
        printf("Abbiamo creato la struttura Chunk\n");
        if(Chunk_Per_Client!=-1){
            printf("Siamo in Chunk_Per_Client diverso da 1\n");
            //se numero chunk è diverso da meno 1 vuol dire che avremo una redistribuzione dei chunk in modo proporzionale tra i client
             //dobbiamo creare un array di S_chunk perchè nel momento in cui passiamo il successivo chunk al seguente thread, se non creo l'array di chunk vi sarà una sovvrascrizione dell'area di memoria
            S_Chunk[indice_assegnazione_chunk].numero_chunk=Chunk_Per_Client;
            S_Chunk[indice_assegnazione_chunk].Array_Di_Chunk=malloc(numero_chunk*(sizeof(char*)));//alloco lo spazio per contenere i chunk
            S_Chunk[indice_assegnazione_chunk].fd=client_fd; //fd per la comunicazione col client
            for(int i=0;i<numero_chunk;i++){
                S_Chunk[indice_assegnazione_chunk].Array_Di_Chunk[i] = malloc(strlen(Collezione_chunk[indice_Di_Redistribuzione]) + 1); // +1 per '\0'
                strcpy(S_Chunk[indice_assegnazione_chunk].Array_Di_Chunk[i],Collezione_chunk[indice_Di_Redistribuzione]);//copiamo i chunk nella struttura da chunk, così potremo passarla al pthread create
            }
            pthread_create(&thread[contatore_thread],NULL,FunzioneThread,&S_Chunk[indice_assegnazione_chunk]);//Dobbiamo passare al thread sia l'FD della socket sia la struttura dati che contiene i chunk
            indice_assegnazione_chunk++; //dovrebbe arrivara a (MAX_CLIENT-1) .... Controllare
        }else{
            //Entriamo qua se numero_chunk è -1, questo significa che vi sarà una distribuzione non proporzionale dei chunk tra i client connessi
            if(numero_chunk<MAX_CLIENT){
                    S_Chunk[indice_assegnazione_chunk].numero_chunk=1; //un solo chunk per quasi tutti i client, vi saranno client con 0 chunk
                    S_Chunk[indice_assegnazione_chunk].Array_Di_Chunk=malloc(1*(sizeof(char*))); //alloco lo spazio per un unico chunk
                    S_Chunk[indice_assegnazione_chunk].fd=client_fd; //fd per la comunicazione col client
                    strcpy(S_Chunk[indice_assegnazione_chunk].Array_Di_Chunk[0],Collezione_chunk[indice_Di_Redistribuzione]);
                    indice_assegnazione_chunk++;
                    //pthread_create
            }else{
                printf("Siamo in Chunk_Per_Client uguale a -1\n");
                //numero chunk maggiore di MAX_ClIENT
                int numero_di_chunk_da_assegnare_al_client=numero_chunk/MAX_CLIENT; //Esempio (11 chunk , 3 client)-> 11/3 = 3
                int resto=numero_chunk%MAX_CLIENT; //il resto lo assegnamo al primo chunk
                if(indice_Di_Redistribuzione==0){//stiamo assegnando i chunk al primo client
                    S_Chunk[indice_assegnazione_chunk].numero_chunk=numero_di_chunk_da_assegnare_al_client+resto;
                    S_Chunk[indice_assegnazione_chunk].fd=client_fd;
                    S_Chunk[indice_assegnazione_chunk].Array_Di_Chunk=malloc(S_Chunk[indice_assegnazione_chunk].numero_chunk*(sizeof(char *)));
                    for(int i=0;i<S_Chunk[indice_assegnazione_chunk].numero_chunk;i++){
                        printf("siamo prima di strcpy\n");
                        S_Chunk[indice_assegnazione_chunk].Array_Di_Chunk[i] = malloc(strlen(Collezione_chunk[indice_Di_Redistribuzione]) + 1); // +1 per '\0'
                        strcpy(S_Chunk[indice_assegnazione_chunk].Array_Di_Chunk[i],Collezione_chunk[indice_Di_Redistribuzione]);
                        indice_Di_Redistribuzione++;
                    }
                 }else{
                    S_Chunk[indice_assegnazione_chunk].numero_chunk=numero_di_chunk_da_assegnare_al_client;
                    S_Chunk[indice_assegnazione_chunk].fd=client_fd;
                    S_Chunk[indice_assegnazione_chunk].Array_Di_Chunk=malloc(S_Chunk[indice_assegnazione_chunk].numero_chunk*(sizeof(char *)));
                    for(int i=0;i<S_Chunk[indice_assegnazione_chunk].numero_chunk;i++){
                        S_Chunk[indice_assegnazione_chunk].Array_Di_Chunk[i] = malloc(strlen(Collezione_chunk[indice_Di_Redistribuzione]) + 1); 
                        strcpy(S_Chunk[indice_assegnazione_chunk].Array_Di_Chunk[i],Collezione_chunk[indice_Di_Redistribuzione]);
                        indice_Di_Redistribuzione++;
                 }                                                                   

                
            }
            printf("siamo prima della pthread_create\n");
            pthread_create(&thread[contatore_thread],NULL,FunzioneThread,&S_Chunk[indice_assegnazione_chunk]);
            indice_assegnazione_chunk++;
            contatore_thread++;
            }
        }
        
    }
    printf("siamo usciti dal ciclo\n");
    for (int i = 0; i < contatore_thread; i++) {
        printf("siamo nell'ultimo for quello dei pthread_join\n");
        void *ptr;
        pthread_join(thread[i], &ptr); //il valore di ritorno del thread finisce in un puntatore
        risultati[i] = (WordCount*)ptr; //risultati è un array di struct di tipo wordcount
    }
    //printf("Risultati : %s ",risultati[0]->parola);
    WordCount* risultato_finale=Reduce(risultati); //passiamo un puntatore a WordCount che contiene tutte le strutture Wordcount
    printf("Fase di inizio Stampa dei risultati tra 5 secodni:\n");
    sleep(5);
    for(int i=0; risultato_finale[i].parola!=NULL;i++){
        printf("Parola: %s, Contatore: %d\n",risultato_finale[i].parola,risultato_finale[i].contatore);
    }
    printf("Fine\n");

}
