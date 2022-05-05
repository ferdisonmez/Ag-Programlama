#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h> 
#include <sys/time.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define SIZE 1024
#define BLKSIZE 1024
#define buffersize 12048
void xerror(const char* fun){
    fprintf(stderr, "Error in %s. ERRNO: %d. Exiting...", fun, errno);
    exit(EXIT_FAILURE);
}
//Move to cursor in file
int xlseek(int fd, __off_t off, int flag){
    int offset = 0;
    if ((offset = lseek(fd, off, flag)) == -1){
        fprintf(stderr, "\nERROR: %s: lseek error. Err no: %d\n", __func__, errno);
        exit(EXIT_FAILURE);
    }
    return offset;
}

int xread(int fd, void* buf, size_t size){
    int read_bytes = read(fd, buf, size);
    if(read_bytes == -1){
        fprintf(stderr, "\nERROR:%s: read input file. errno: %d\n", __func__, errno);
        exit(EXIT_FAILURE);
    }
    return read_bytes;
}
//This function open the file
int xopen(const char* file, int oflag){
    int fd = 0;
    if ((fd = open(file, oflag, 0666)) < 0) {
        fprintf(stderr, "\nERROR: %s: open input file %s. err no: %d\n", __func__, file, errno);
        exit(EXIT_FAILURE);
    }

    return fd;
}
int filesize(int fd){
    const int READ_SIZE = 512; // it will read 512 bytes each.
    char buf[512];
    xlseek(fd, 0, SEEK_SET); // move to cursor to the beginning.
    int _size = 0;

    int rd = 0;
    while ((rd = xread(fd, buf, READ_SIZE)) > 0)
        _size += rd;
    return _size;
}

//This function write the file
int xwrite(int fd, void* buf, size_t size){
    int write_byte = write(fd, buf, size);
    if(write_byte == -1){
        fprintf(stderr, "\nERROR: %s: write output file. err no: %d\n", __func__, errno);
        exit(EXIT_FAILURE);
    }
    return write_byte;
}
int splitarr(char newarr[][SIZE],int readsize,char buf[]){
	    int i=0,m=0,n=0,k=0,col=0,flag=0;
    while(i!=readsize && i<=readsize){
    	m=0;
    	while(buf[k]!='\n'){
			if (buf[k]=='*')
			{
				flag=1;
			}
			if (buf[k]==',' || buf[k]==';')
			{
    		    k++;
			}
			else{
				newarr[n][m]=buf[k];
				m++;
				k++;
			}
    	}
    	newarr[n][m]='\0';
 		i=++k;
 		n++;
 		col++;
    }
	return flag;
}
int countrow(char* buf ,int filesize){  ///Count Row
	int i=0,k=0,count=0;

	while(i!=filesize){
		while(buf[k]!='\n'){
			k++;
		}
		i=++k;
		count++;
	}
	return count;

}
int countcol(char* buf ,int filesize){   ///Count Colunm
    int k=0,count=0;
		while(buf[k]!='\0'){
            k++;
            count++;   
		}
		
	return count;

}
void sigint_handler(int signo) { //Handler for SIGINT
 	printf("Exiting Client...\n");
	exit(0);
}

int main(int argc, char *argv[])
{
    int opt;
	char* ip;
	int PORT,id;
    char* pathToQueryFile;
	int queryflag;

	// Check argument amount
	if(argc != 9)
	{
		printf("This usage is wrong. \n");
		printf("./client –i id -a 127.0.0.1 -p PORT -o pathToQueryFile\n");
		exit(EXIT_FAILURE);
	}

	// I used the getopt() library method for parsing commandline arguments.
	while((opt = getopt(argc, argv, "apio")) != -1)
	{
		switch(opt)
		{
			case 'a':
				ip = argv[optind];
				break;
			case 'p':
				PORT = atoi(argv[optind]);
				break;
			case 'i':
				id = atoi(argv[optind]);
				break;
			case 'o':
				pathToQueryFile = argv[optind];
				break;
			default:
			// If the command line arguments are missing/invalid your program must print usage information and exit.
				printf("This usage is wrong. \n");
				printf("./client –i id -a 127.0.0.1 -p PORT -o pathToQueryFile\n");
				exit(EXIT_FAILURE);
		}
	}
	if (signal(SIGINT, sigint_handler) == SIG_ERR)
  	      fprintf(stderr,"\ncan't catch SIGINT %s \n",strerror(errno));
	// Check values
	if(PORT < 0)
	{
		printf("Portnumber must be positive.\n");
		exit(EXIT_FAILURE);
	}

	if(id<0)
	{
		printf("ID must not negative.\n");
		exit(EXIT_FAILURE);
	}
    int socket10 = 0, size = 0; 
	struct sockaddr_in serverAddr; 
	char bufread[SIZE] = {' '};
	char bufwrite[SIZE] = {0};
	struct timeval start, end;
	printf("Client-%d connecting to %s:%d\n",id, ip, PORT);
	int fdS = open(pathToQueryFile,O_RDONLY);   
    if(fdS == -1)
	{
		fprintf (stderr, "\nerrno = %d: %s (open pathToQueryFile)\n\n", errno, strerror (errno));
		exit(1);
	}
    int readsizeS=filesize(fdS); //Learn the file size
    xlseek(fdS, 0, SEEK_SET);
    char buf[SIZE]={0};    
    xread(fdS,buf,readsizeS);
    int rowS=countrow(buf,readsizeS);//Count row size
    char newarrS[rowS][SIZE];    ///Create Array[][]
    queryflag=splitarr(newarrS,readsizeS,buf);
	// Create socket file descriptor 
	if((socket10 = socket(AF_INET, SOCK_STREAM, 0)) < 0) 
	{
		fprintf(stderr, "\nerrno = %d: %s Socket creation error\n\n", errno, strerror(errno));
		exit(EXIT_FAILURE);
	}

	serverAddr.sin_family = AF_INET; 
	serverAddr.sin_port = htons(PORT);

	// Convert IPv4 and IPv6 addresses from text to binary form
	if(inet_pton(AF_INET, ip, &serverAddr.sin_addr)<=0)  
	{
		fprintf(stderr, "\nerrno = %d: %s Invalid address/ Address not supported\n\n", errno, strerror(errno));
		exit(EXIT_FAILURE);
	}
	if(connect(socket10, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) < 0) 
	{
		fprintf(stderr, "\nerrno = %d: %s Connection Failed (connect) \n\n", errno, strerror(errno));
		exit(EXIT_FAILURE);
	}
	
	// Send nodes to server
	int k=0;
	while (k<=rowS)
	{
		if (id==(newarrS[k][0]-'0'))
		{
		printf("Client-%d connected and sending query '%s'\n",id,newarrS[k]);
		strcpy(bufwrite,newarrS[k]);
		send(socket10, bufwrite, strlen(bufwrite), 0);

		// Calculate response time
		gettimeofday(&start, NULL);
		// Take result from server
		size = read(socket10, bufread, SIZE);
		gettimeofday(&end, NULL);
		long seconds = (end.tv_sec - start.tv_sec);
		long micros = ((seconds * 1000000) + end.tv_usec) - (start.tv_usec);
		double diff_t = (double) micros / 1000000;

		// Server response
		if(size == 0)
			printf("Server's response to Client-%d no record and arrived in %lfseconds,close\n",id, diff_t);
		else
			printf("Server's response to Client-%d is \n%s\n record and arrived in %lfseconds.\n",id, bufread, diff_t);
		}
	++k;
	}
	close(socket10);

    return 0;
}
