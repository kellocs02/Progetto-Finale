#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include "MapReduce.h" 

//funzione eseguita da ogni thread
//Attraverso essa dobbiamo inviare i chunk al client
//il client dovrà occupparsi delle operazioni di elaborazione di quest'ultimi



void* FunzioneThread(void* args){
    printf("ciao\n");

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