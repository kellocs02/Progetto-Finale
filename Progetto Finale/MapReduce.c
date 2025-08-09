#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include "MapReduce.h" 

//funzione eseguita da ogni thread
//Attraverso essa dobbiamo inviare i chunk al client
//il client dovrà occupparsi delle operazioni di elaborazione di quest'ultimi

pthread_mutex_t mutex=PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
int variabile_condivisa=0;

WordCount* Reduce(WordCount** risultati) {
    printf("Siamo in reduce\n");

    int capienza = 10; 
    int size = 0; // elementi usati
    WordCount* totale = malloc(capienza * sizeof(WordCount));

    for (int i = 0; i < MAX_CLIENT; i++) {
        for (int j = 0; risultati[i][j].parola != NULL; j++) {
            char* parola = risultati[i][j].parola;
            int cont = risultati[i][j].contatore;

            //cerchiamo se la parola si trova già in totale
            int trovata = 0;
            for (int k = 0; k < size; k++) {
                if (strcmp(totale[k].parola, parola) == 0) {
                    totale[k].contatore += cont;
                    trovata = 1;
                    break;
                }
            }

            //se la parola non è stata trovata la aggiungiamo
            if (!trovata) {
                // Se serve, aumentiamo capacità
                if (size >= capienza) {
                    capienza += 10;
                    totale = realloc(totale, capienza * sizeof(WordCount));
                }
                totale[size].parola = strdup(parola);
                totale[size].contatore = cont;
                size++;
            }
        }
    }
    totale = realloc(totale, (size + 1) * sizeof(WordCount));
    totale[size].parola = NULL;
    totale[size].contatore = 0;

    return totale;
}




WordCount* Gestisci_Ricezione(Struttura_Chunk* mio_chunk){
    printf("siamo in gestisci Ricezione\n");
    WordCount* w = malloc(4 * sizeof(WordCount)); // spazio iniziale
    if (!w) {
        perror("malloc iniziale fallita");
        return NULL;
    }
    int indice = 0;
    int capienza = 4;

    while(1){
        int len_net;
        int cont_net;

        // Ricevi esattamente sizeof(len_net) byte per la lunghezza parola
        size_t ricevuti = 0;
        while (ricevuti < sizeof(len_net)) { //ricevuti deve essere minore di 4 byte, poichè len_net è un intero
            int r = recv(mio_chunk->fd, ((char*)&len_net) + ricevuti, sizeof(len_net) - ricevuti, 0);
            if (r <= 0) { //succede se il client chiude la connessione con closefd
                if (r == 0) printf("Connessione chiusa dal peer\n");
                else perror("recv len_net");
                goto fine;
            }
            ricevuti += r; //per ogni byte ricevuto incremento ricevuti
        }

        size_t len = (size_t) ntohl(len_net);
        printf("lunghezza della parola: %zu\n", len);
        if (len <= 0 || len > MAX_PAROLA) {
            fprintf(stderr, "Lunghezza parola non valida: %zu\n", len);
            goto fine;
        }

        char *parola = malloc(len);
        if (!parola) {
            perror("malloc parola");
            goto fine;
        }

        // Ricevi esattamente len byte per la parola
        ricevuti = 0;
        while (ricevuti < len) {
            int r = recv(mio_chunk->fd, parola + ricevuti, len - ricevuti, 0);
            if (r <= 0) {
                perror("recv parola");
                free(parola);
                goto fine;
            }
            ricevuti += r;
        }

        // Ricevi esattamente sizeof(cont_net) byte per il contatore
        ricevuti = 0;
        while (ricevuti < sizeof(cont_net)) {
            int r = recv(mio_chunk->fd, ((char*)&cont_net) + ricevuti, sizeof(cont_net) - ricevuti, 0);
            if (r <= 0) {
                perror("recv cont_net");
                free(parola);
                goto fine;
            }
            ricevuti += r;
        }

        int contatore = ntohl(cont_net);

        if (indice >= capienza) {
            capienza += 10;
            WordCount* tmp = realloc(w, capienza * sizeof(WordCount));
            if (!tmp) {
                perror("realloc");
                free(parola);
                goto fine;
            }
            w = tmp;
        }

        w[indice].parola = parola;
        w[indice].contatore = contatore;
        indice++;
    }

fine:
    w[indice].parola = NULL;   //segnala la fine . L'ultimo elemento lo ponimao a NULL.
    w[indice].contatore = 0;   
    return w;
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
    printf("SIAMo DOPO IL FOR IN FUNZIONE THREAD, STIAMO PER INVOCARE GESTISCI RICEZIONE\n");
    WordCount* w=Gestisci_Ricezione(mio_chunk);
    printf("FD:%d,stringa: %s\n",mio_chunk->fd,w->parola);
    int chiusura=close(mio_chunk->fd);
    if(chiusura==-1){
        perror("Errore nella chiusura di fd");
    }else{
        printf("Chiusura della connessione col client avvenuta col successore, FD->%d\n",mio_chunk->fd);
    }
    printf("Connessione chiusa col client\n");
    pthread_exit(w);
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
        sleep(1);
        //printf("Contenuto letto: '%.*s'\n", (int)n, buffer);
        //aggiungiamo il chunk alla collezione
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
    //appena il file viene finito di leggere
    char *penultimo = (*collezione_chunk)[(*numero_chunk) - 2];
    char *ultimo = (*collezione_chunk)[(*numero_chunk) - 1];
    
    size_t nuova_lunghezza = strlen(penultimo) + 1 + strlen(ultimo) + 1;
    
    char *nuovo_penultimo = realloc(penultimo, nuova_lunghezza);
    if (!nuovo_penultimo) {
        perror("realloc fallita");
        exit(EXIT_FAILURE);
    }
    
    strcat(nuovo_penultimo, " ");
    strcat(nuovo_penultimo, ultimo);
    
    (*collezione_chunk)[(*numero_chunk) - 2] = nuovo_penultimo;
    
    free((*collezione_chunk)[(*numero_chunk) - 1]);

    (*numero_chunk)--;

    sleep(5);
    printf("CIAOOOOO\n");
    sleep(10);
    printf("UltimoOOOOOOOOOOOOOOOOOOOOOOOOOOO %s\n", (*collezione_chunk)[(*numero_chunk) - 1] );
    sleep(5);
    //(*collezione_chunk)[*numero_chunk-1]=realloc((*collezione_chunk)[*numero_chunk-1],sizeof((*collezione_chunk)[*numero_chunk]))
    free(buffer);
    fclose(f);
    return;
} 