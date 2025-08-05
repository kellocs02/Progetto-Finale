#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include "MapReduce.h" 

//funzione eseguita da ogni thread
//Attraverso essa dobbiamo inviare i chunk al client
//il client dovrà occupparsi delle operazioni di elaborazione di quest'ultimi

pthread_mutex_t mutex=PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
int variabile_condivisa=0;

WordCount* Gestisci_Ricezione(Struttura_Chunk* mio_chunk){
    WordCount* w=malloc(4*sizeof(WordCount)); //alloco inzialmente 4 spazi 
    int indice=0; //l'indice tiene conto della posizone in cui salveremo la prossima stuct worldcount
    int capienza=4; //la capienza tiene conto dello spazio allocato 
    while(1){
        int len_net;
        int cont_net;
        int n;

        n=recv(mio_chunk->fd,&len_net, sizeof(len_net), 0); //riceviamo la lunghezza della parola
        if (n == 0) break; // connessione chiusa
        if (n <= 0) {
            perror("recv len");
            break;
        }

        int len = ntohl(len_net);
        if (len <= 0 || len > MAX_PAROLA) {
            fprintf(stderr, "Lunghezza parola non valida: %d\n", len);
            break;
        }

        char *parola = malloc(len);
        if (!parola) {
            perror("malloc parola");
            break;
        }

        int ricevuti = 0;
        while (ricevuti < len) {
            //len - ricevuti è quanti byte mancano ancora da leggere.
            int r = recv(mio_chunk->fd, parola + ricevuti, len - ricevuti, 0); //parola + ricevuti è un puntatore che punta al primo byte libero del buffer dove stai salvando la parola. Ad esempio: se hai già ricevuto 3 byte, scrivi a partire dal 4° byte del buffer.
            if (r <= 0) {
                perror("recv parola");
                free(parola);
                break;
            }
            ricevuti += r;
        }
        
        //ricevo il contatore della singola parola
        n = recv(mio_chunk->fd, &cont_net, sizeof(cont_net), 0);
        if (n <= 0) {
            perror("recv contatore");
            free(parola);
            break;
        }
        
        //converto nel formato della macchina
        int contatore = ntohl(cont_net);
        if(indice<capienza){
            w[indice].parola=parola;
            w[indice].contatore=contatore;
            indice++;
        }else{
            capienza+=10; //aumento la capienza di 10
            w=realloc(w,capienza*sizeof(WordCount)); //allochiamo altro spazio per w
            if (!w) {
                perror("realloc");
                free(parola);
                break;
            }
            w[indice].parola=parola;
            w[indice].contatore=contatore;
            indice++;
        }
        
    return w;
    }
}

void *FunzioneThread(void *arg) {
    WordCount ricevuto;
    Struttura_Chunk *mio_chunk = (Struttura_Chunk *)arg;
    //meccanismo di barriera
    pthread_mutex_lock(&mutex); //prendiamo possesso del lock
    variabile_condivisa++; 
    if(variabile_condivisa<MAX_CLIENT){
        pthread_cond_wait(&cond,&mutex);//il thread si blocca e rilascia il mutex
    }else{
        pthread_cond_broadcast(&cond);//sveglia tutti i thread in attesa
    }
    pthread_mutex_unlock(&mutex);  
    for (int i = 0; i < mio_chunk->numero_chunk; i++) {
        size_t len = strlen(mio_chunk->Array_Di_Chunk[i]);
        ssize_t sent = send(mio_chunk->fd, mio_chunk->Array_Di_Chunk[i], len, 0);
        if (sent < 0) {
            perror("Errore durante send");
            break;
        } else {
            printf("Inviato chunk %d: %zd byte\n", i, sent);
        }
    }
    WordCount* w=Gestisci_Ricezione(mio_chunk);
    close(mio_chunk->fd);
    pthread_exit(NULL);
}



void StampaChunk(char ** Collezione_chunk, int numero_chunk){
        printf("numero chunk: %d\n",numero_chunk);
        for(int i=0;i<numero_chunk;i++){
            //printf("Chunk numero %d: %s\n",i,Collezione_chunk[i]);
            printf("ciao\n");
        }
        printf("numero chunk:%d\n",numero_chunk);
        return;
}

//alla fine del ciclo di letture del file, averemo un array di puntatori dinamico popolato dai vari chunk
void salva_chunk(char*** collezione_chunck, char* chunk, int *numero_chunk){
    printf("siamo in salva_chunk\n");
    char* copia= malloc(strlen(chunk)+1); // strlen restituisce il numero di caratteri visibili escludendo il terminatore di riga quindi poniamo +1
                                          // Non usiamo sizeof perchè ci restituirebbe la lunghezza del tipo, in questo caso il puntatore in un'architettura a 64 bit è 64
    if (!copia) {
        perror("malloc fallita");
        exit(EXIT_FAILURE);
    }
    strcpy(copia, chunk); //copimo il contenuto del chunck nel buffer copia, strcpy aggiunge in automatico il terminatore
    char **tmp = realloc(*collezione_chunck, (*numero_chunk + 1) * sizeof(char *)); //qui stiamo allocando lo spazio per un nuovo puntatore a char, infatti usiamo sizeof(char*)
    if(!tmp){
        perror("errore in tmp");
        return;
    }
    *collezione_chunck=tmp; //aggiorno il puntatore originale
    (*collezione_chunck)[*numero_chunk] = copia; //inseriamo il chunk nel nuovo slot
    (*numero_chunk)++; //incrementiamo il numero dei chunk
    }



void chunk(char*** collezione_chunk,int *numero_chunk) {
    printf("siamo entrati in chunk\n");
    FILE *f = fopen("lotr.txt", "r");
    if (!f) {
        perror("errore apertura file");
        return;
    }
    printf("abbiamo aperto il file di LOTR\n");
    sleep(1);
    char *buffer = malloc(DIM_CHUNK + 1); //allochiamo 512 kb di spazio per il buffer +1 per il carattere terminatore di stringa
    if (!buffer) {
        perror("errore allocazione buffer");
        fclose(f);
        return;
    }

    while (1) {
        size_t n = fread(buffer, 1, DIM_CHUNK, f); //leggiamo un blocco, RESTITUISCE 0 SE NON HA LETTO NULLA
        printf("Valore di n: %zu\n", n);
        if (n == 0) {
            if (feof(f)) break;
            if (ferror(f)) {
                perror("Errore in fread");
                break;
            }
        }

        buffer[n] = '\0'; //metto il terminatore in fondo al buffer, così posso usare funzioni tipo strrchr senza problemi

        //ora i dati letti si trovano nel buffer
        //dobbiamo far sì che non abbiamo salvato parole spezzate
        //quindi poniamo il puntatore all'ultimo ' ' che abbiamo nel buffer

        char *ultimo_spazio = strrchr(buffer, ' '); //ci restituisce un puntatore a carattere che punta all'ultimo spazio nel buffer
                                                    //se non ci sono spazi, cosa altamente improbabile, entriamo nell'if

        if (!ultimo_spazio || ultimo_spazio==buffer) {
            ultimo_spazio = buffer + n; //buffer punta al primo elemento dello spazio di memoria che abbiamo allocato, 
                                        //n sono i caratteri successivi che abbiamo immesso nel buffer,
                                        //ultimo_spazio ora punta alla posizione successiva a tutto questo blocco
        }

        size_t lunghezza_chunk = ultimo_spazio - buffer;
        printf("ultimo_spazio - buffer = %ld\n", ultimo_spazio - buffer);

        char *chunk = malloc(lunghezza_chunk + 1); //copiamo il chunk buono, senza parole spezzate 
        memcpy(chunk, buffer, lunghezza_chunk);    //copiamo effettivamente i dati
        chunk[lunghezza_chunk] = '\0';             //pongo il terminatore
        //printf("chunk1: %s\n",chunk);
        sleep(2);
        //printf("Contenuto letto: '%.*s'\n", (int)n, buffer);
        salva_chunk(collezione_chunk, chunk, numero_chunk);
        printf("siamo dopo salva_chunk\n");
        free(chunk); //libero lo spazio in memoria

        //indietro contiene il numero di byte letti in più rispetto a quelli che vogliamo usare
        //n rappresenta il numero di byte letti
        //lunghezza_chunk è la grandezza del chunk in byte
        //portiamo il cursore del file indietro in modo che dalla prossima lettura ripartiamo dall'inizio di quella parola spezzata che non abbiamo salvato.
        long indietro = n - lunghezza_chunk;
        if (indietro > 0) {
            fseek(f, -indietro, SEEK_CUR);
        }
    }

    free(buffer);
    fclose(f);
    return;
}