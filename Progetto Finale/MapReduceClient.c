#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <ctype.h>
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

Blocco_Parole Map(char* array){
    int capacità = INIZIALE; //rappresenta lo spazio allocato
    WordCount *contatore_parole = malloc(capacità * sizeof(WordCount));
    int lunghezza_contatore=0;
    char buffer[100];                               //buffer per contenere le parole temporanee per la fase di elaborazione 
    for(int i=0;array[i]!='\0';i++){
        int j=0; //scorriamo l'array fino alla fine
        while(array[i]!=' ' && array[i]!='\0'){ //scorriamo le parole dell'array fino a trovare uno spazio e nel frattempo riempiamo il buffer temporale
            if(isupper(array[i])){
                buffer[j]=tolower(array[i]); //se la lettera è maiuscola la salviamo in formato minuscolo
            }else{
                buffer[j]=array[i]; //riempiamo il buffer
            }
            i++;
            j++; 
            }
        
        buffer[j] = '\0';
        if(Controllo(buffer,contatore_parole,lunghezza_contatore)==0){//verifichiamo se la parola è presente in contatore parole
            lunghezza_contatore++;
            //parola non presente, la dobbiamo aggiungere
            if (lunghezza_contatore == capacità) {
                capacità *= 2; // Raddoppi lo spazio
                contatore_parole = realloc(contatore_parole, capacità * sizeof(WordCount));
            }
            //abbiamo aggiunto un blocco alla struttura
            contatore_parole[lunghezza_contatore-1].parola = strdup(buffer); //copiamo la nuova parola nell'ulitma cella della struttura
            contatore_parole[lunghezza_contatore-1].contatore=1;  
        }

    }
    Blocco_Parole blocco={lunghezza_contatore,contatore_parole};
    return blocco;
}