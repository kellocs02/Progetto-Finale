#include <stdio.h>
#include <string.h>
#include <stdlib.h>

int main(){
    char array[]="ciao";
    printf("array: %s, lunghezza, %d\n",array,strlen(array));

    char *p= "ciao";

    char **stringhe=malloc(2*sizeof(char*));
    (*stringhe)[0]="bella ";
    (*stringhe)[1]="ciao ";

    for(int i=0; i<2;i++){
        printf("stringa %d: %s",i,(*stringhe)[i]);
    }
}