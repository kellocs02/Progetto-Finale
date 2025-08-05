# Compilatore e flag
CC = gcc
CFLAGS = -Wall -Wextra -pthread

# File sorgenti
SRCS_MAIN = main.c MapReduce.c
SRCS_CLIENT = client.c MapReduceClient.c

# File oggetto
OBJS_MAIN = $(SRCS_MAIN:.c=.o)
OBJS_CLIENT = $(SRCS_CLIENT:.c=.o)

# Target eseguibili
TARGET_MAIN = programma
TARGET_CLIENT = client

# Regola di default
all: $(TARGET_MAIN) $(TARGET_CLIENT)

# Regola per programma principale
$(TARGET_MAIN): $(OBJS_MAIN)
	$(CC) $(CFLAGS) -o $@ $^

# Regola per client
$(TARGET_CLIENT): $(OBJS_CLIENT)
	$(CC) $(CFLAGS) -o $@ $^

# Regola generica per compilare .o da .c
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# Pulizia
clean:
	rm -f *.o $(TARGET_MAIN) $(TARGET_CLIENT)



