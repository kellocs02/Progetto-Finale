#ifndef MAPREDUCE_H
#define MAPREDUCE_H

#define DIM_CHUNK  262144 //16kb

void chunk(char*** collezione_chunk,int *numero_chunk);

void salva_chunk(char*** collezione_chunck, char* chunk, int *numero_chunk);

void StampaChunk(char ** Collezione_chunk, int numero_chunk);

#endif