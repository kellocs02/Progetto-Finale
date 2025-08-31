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
    int capacità = INIZIALE; //spazio iniziale contatore parole
    WordCount *contatore_parole = malloc(capacità * sizeof(WordCount)); //array che conterrà le parole e i contatori
    if (!contatore_parole) {
        perror("malloc fallita");
        exit(EXIT_FAILURE);
    }
    int lunghezza_contatore = 0; //tiene il conteggio delle parole uniche trovate
    char buffer[100]; //buffer per costruire la parola
    for (int i = 0; array[i] != '\0'; ) { //scorre la stringa fino al terminatore
        int j = 0; //indice per riempire il buffer
        while (array[i] != '\0' && !isalnum(array[i])) { //salta tutti i caratteri non alfanumerici, (spazi e punteggiatura)
            i++;
        }
        while (array[i] != '\0' && isalnum(array[i])) {//la parola trovata viene convertita in minuscolo
            buffer[j++] = tolower(array[i]);
            i++;
        }

        buffer[j] = '\0'; //mettiamo il terminatore nella stringa del buffer

        //se non abbiamo trovato una parola si va avanti(ciao,,,bello), j è il numero di caratteri copiati nel buffer
        if (j == 0) {
            continue; //salta il resto del ciclo e ricomincia dall'inizio del for
        }

        //se la parola non è presente nella nostra lista:
        if (Controllo(buffer, contatore_parole, lunghezza_contatore) == 0) {
            if (lunghezza_contatore == capacità) { //controllo sullo spazio disponibile per l'immagazzinamento dei dati nell'array
                capacità *= 2; //aumentiamo la capacità
                WordCount *tmp = realloc(contatore_parole, capacità * sizeof(WordCount));//riallochiamo lo spazio
                if (!tmp) {
                    perror("realloc fallita");
                    exit(EXIT_FAILURE);
                }
                contatore_parole = tmp;//facciamo puntare contatore_parole all'area di memoria di tmp
            }

            char *copia = strdup(buffer);//alloca nell'heap la memoria per contenere la stringa contenuta in buffer, inoltre copia la parola in questa area di memoria
            if (!copia) {
                perror("strdup fallita");
                exit(EXIT_FAILURE);
            }

            contatore_parole[lunghezza_contatore].parola = copia; //immagazziniamo la parola nell'array
            contatore_parole[lunghezza_contatore].contatore = 1; //impostaimo il contatore ad 1
            lunghezza_contatore++; //aumentiamo il valore della variabile che tiene il conto delle parole trovate
        }
    }

    Blocco_Parole blocco = { lunghezza_contatore, contatore_parole };
    return blocco;
}

