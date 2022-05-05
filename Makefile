all: serverprogram clientprogram

serverprogram: server.c
	gcc -Wall -o server server.c -pthread -lm

clientprogram: client.c
	gcc -Wall -o client client.c -pthread -lm

clean: 
	  $(RM) server
	  $(RM) client
