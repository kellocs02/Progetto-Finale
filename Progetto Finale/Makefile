# Compilatore e flag
CC = gcc
CFLAGS = -Wall -Wextra -pthread

# File sorgenti e oggetti
SRC = main.c MapReduce.c
OBJ = $(SRC:.c=.o)
TARGET = programma

# Regola principale
$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) -o $@ $^

# Regola generica per compilare .o da .c
%.o: %.c %.h
	$(CC) $(CFLAGS) -c $< -o $@

# Clean: rimuove oggetti ed eseguibile
clean:
	rm -f $(OBJ) $(TARGET)
