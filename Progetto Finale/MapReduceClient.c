#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <ctype.h>
#include <arpa/inet.h>
#include "MapReduce.h" 

#define INIZIALE 4

int Controllo(char *buffer,WordCount* contatore_parole, int lunghezza_contatore){
    for(int i=0; i<lunghezza_contatore;i++){
        //restituisce 0 se le stringhe sono uguali
        //altro se non sono uguali 
        if(strcmp(buffer,contatore_parole[i].parola)==0){
            //le parole sono uguali
            //la lunghezza del contatore non aumenta
            contatore_parole[i].contatore++; //aumenta però il contatore relativo alla singola parola
            return 1; //restituiamo 1 se nella struttura era presente la parola
        } 
    }
    return 0; //restituiamo 0 se nella struttura non è presente la parola
}

Blocco_Parole Map(char* array) {
    int capacità = INIZIALE;
    WordCount *contatore_parole = malloc(capacità * sizeof(WordCount));
    if (!contatore_parole) {
        perror("malloc fallita");
        exit(EXIT_FAILURE);
    }

    int lunghezza_contatore = 0;
    char buffer[100];

    for (int i = 0; array[i] != '\0'; ) {
        int j = 0;

        // Salta tutti i caratteri non validi (spazi, punteggiatura)
        while (array[i] != '\0' && !isalnum(array[i])) {
            i++;
        }

        // Costruisci la parola con soli caratteri alfanumerici
        while (array[i] != '\0' && isalnum(array[i])) {
            buffer[j++] = tolower(array[i]);
            i++;
        }

        buffer[j] = '\0';

        // Evita parole vuote
        if (j == 0) {
            continue;
        }

        // Se la parola non è già presente
        if (Controllo(buffer, contatore_parole, lunghezza_contatore) == 0) {
            if (lunghezza_contatore == capacità) {
                capacità *= 2;
                WordCount *tmp = realloc(contatore_parole, capacità * sizeof(WordCount));
                if (!tmp) {
                    perror("realloc fallita");
                    exit(EXIT_FAILURE);
                }
                contatore_parole = tmp;
            }

            char *copia = strdup(buffer);
            if (!copia) {
                perror("strdup fallita");
                exit(EXIT_FAILURE);
            }

            contatore_parole[lunghezza_contatore].parola = copia;
            contatore_parole[lunghezza_contatore].contatore = 1;
            lunghezza_contatore++;
        }
    }

    Blocco_Parole blocco = { lunghezza_contatore, contatore_parole };
    return blocco;
}

