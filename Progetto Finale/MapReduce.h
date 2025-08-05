#ifndef MAPREDUCE_H
#define MAPREDUCE_H

#define DIM_CHUNK  262200 //circa 256 kb
#define MAX_CLIENT 3

typedef struct{
    int lunghezza_contatore;
    WordCount* struttura_parole;
}Blocco_Parole;


typedef struct{
    int fd;
    int numero_chunk;
    char ** Array_Di_Chunk;
}Struttura_Chunk;

typedef struct {
    char *parola;
    int contatore;
} WordCount;

void chunk(char*** collezione_chunk,int *numero_chunk);

void salva_chunk(char*** collezione_chunck, char* chunk, int *numero_chunk);

void StampaChunk(char ** Collezione_chunk, int numero_chunk);

void* FunzioneThread(void* args);

WordCount *Map(char *buffer);

#endif