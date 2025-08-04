#ifndef MAPREDUCE_H
#define MAPREDUCE_H

#define DIM_CHUNK  262200 //circa 256 kb

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

char Map(char **array);

#endif