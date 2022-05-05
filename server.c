#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/time.h>
#include <signal.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <pthread.h>
#include <sys/stat.h>

#define SIZE 1024
#define NUMBER 10

//tcount-->Sockete bağlanmak isteyenleri sayar
//

int arraycol;
int poolsize=0;
int fdsocket;
int sayrow=0;
///This queue structure is taken from https://www.geeksforgeeks.org/queue-set
typedef struct node{
	int item;
	int index;
	struct node *next;
}node;

typedef struct{
	node *front;
	node *rear;
}Queue;
Queue ports;
int empty(Queue *q)
{
	if(q->front==NULL)
		return 0;
	else
		return 1;
}
void enqueue(Queue *q,int item)
{
	node* tempNode = (node *) malloc(sizeof(node));
	tempNode->next = NULL;
	tempNode->item = item;

	if(empty(q)!=0)
		q->rear->next = tempNode;
	else
		q->front = tempNode;
	
	q->rear = tempNode;
}

int dequeue(Queue *q)
{										
	int client;
	node *tempNode;

	if(empty(q)!=0)
	{
		client = q->front->item;
		tempNode = q->front;
		q->front = q->front->next;
		
		free(tempNode);
		
		if(q->front==NULL)
			q->rear=NULL;
	}
	return client;
}
// mainmutex is used to synchronization for pool threads and main thread
pthread_mutex_t mainmutex;
pthread_mutex_t mutex1;
pthread_mutex_t mutex2;

pthread_cond_t cond1;
// For reader - writer paradigm (Slayt week 10)
int AR = 0, AW = 0, WR = 0, WW = 0;
pthread_cond_t okToRead = PTHREAD_COND_INITIALIZER;
pthread_cond_t okToWrite = PTHREAD_COND_INITIALIZER;
// Global counters
int pcount = 0, pcount2 = 0, pcount3 = 0, tcount = 0;
int flagint =0, socket1 =-1;
int fd, fd2;
int** Thread = NULL;
int row1=0;
int colum1=0;
char*** array=NULL;

// mutex3 is used to synchronization for main thread and dynamic coordinate thread
pthread_mutex_t mutex3 = PTHREAD_MUTEX_INITIALIZER;
// tmutex is used to forward connection to pool threads
pthread_mutex_t tmutex = PTHREAD_MUTEX_INITIALIZER;
// sendmutex is used to send message via ports for pool threads
pthread_mutex_t sendmutex = PTHREAD_MUTEX_INITIALIZER;
// printmutex is used to print message to log filer for all threads
pthread_mutex_t printmutex = PTHREAD_MUTEX_INITIALIZER;

// cempty and cfull condition variable for main thread and pool threads.
// For simple producer - consumer synchronization
pthread_cond_t cempty = PTHREAD_COND_INITIALIZER;
pthread_cond_t cfull = PTHREAD_COND_INITIALIZER;

// pempty and pfull condition variable for main thread and dynamic coordinate thread.
// For simple producer - consumer synchronization
pthread_cond_t pempty = PTHREAD_COND_INITIALIZER;
pthread_cond_t pfull = PTHREAD_COND_INITIALIZER;

// tempty and tfull condition variable for main thread and pool threads.
// For simple producer - consumer synchronization, for forward connection to pool threads
pthread_cond_t tempty = PTHREAD_COND_INITIALIZER;
pthread_cond_t tfull = PTHREAD_COND_INITIALIZER;
FILE* fp;
void CountCsv(FILE* fp,int* row,int* colum){
        char buffer[1024];
        char *line,*record;
        fseek(fp,0, SEEK_SET); //Locate to the second line, the size of each English character is 1
		int j = 0;
		 while ((line = fgets(buffer, sizeof(buffer), fp))!=NULL)//The loop continues when the end of the file is not read
		{
			record = strtok(line, ",");
			 while (record != NULL)//Read the data of each row
			{
				// printf("%s\n", record);//Print out every data read
                if((*row)==0){
                    (*colum)++;
                }
				record = strtok(NULL, ",");
				j++;
			}
			j = 0;
            
            (*row)++;
		}
}
void writefile(char* str)
{
	pthread_mutex_lock(&printmutex);  ///Başka thread yazmasını önlemek için
	time_t xtime;
	struct tm xresult;
	char sxtime[32];
	size_t n;
	char buf[SIZE*2];
	char* bp;
	int forread, forwrite;

	xtime = time(NULL);
	localtime_r(&xtime, &xresult);
	asctime_r(&xresult, sxtime);
	n = strlen(sxtime);
	if(n && sxtime[n-1] == '\n') sxtime[n-1] = '\0';
	sprintf(buf, "[%s] %s\n", sxtime, str);

	forread = strlen(buf);
	bp = buf;
	while(forread > 0)
	{
		while(((forwrite = write(fd2, bp,forread)) == -1) && (errno = EINTR));
		if(forwrite < 0)
			break;
		forread -= forwrite;
		bp += forwrite;
	}

	pthread_mutex_unlock(&printmutex);
}

void freeOther()
{
	int i;
	for(i = 0; i < poolsize; i++)
	{
	//	if(Thread != NULL && Thread[i] != NULL)
			free(Thread[i]);
	}
//	if(Thread != NULL)
		free(Thread);
		fclose(fp);
}
void freeArray(){
	int i,j;
	for(i = 0; i<row1; i++)
	{
			for ( j = 0; j < colum1; j++)
			{
				   free(array[i][j]);
			}	
			free(array[i]);
	}
	if(array!=NULL)
		free(array);
}

void sigint_handler(int signo) { //Handler for SIGINT
 	printf("Exiting...\n");
	 freeOther();
	 freeArray();
	 close(fdsocket);
	 flagint=1;
	 socket1=-2;
	 exit(0);
}
void parseforupdate(char buf[],char* parse[],int readsize){
	  int i=0,m=0,n=0,k=0,flag=0;
	  int col=0;
	  char temp[SIZE]={0};	
	  char d=39; //For --> '
    while(i<readsize){
		n=0;
		temp[0]='\0';
    	while(buf[k]!='=' && buf[k]!=d && buf[k]!='\0'){
				temp[n]=buf[k];
				k++;
				n++;	
    	}	
		if (buf[k]=='=')
		{
			col++;
		}
		if((buf[k]=='=' || buf[k]=='\0')){
			temp[n]='\0';
			parse[col]=malloc(sizeof(char*)*SIZE);
			strcpy(parse[col],temp);
		}///İf	
 		i=k++;
	}

}
void* funThread(void* args){   ///Thread fonksiyonu
    int x =((int*)args)[0];
    int cflag=0;
    int temp,rsize;
    char buffer[SIZE] = {0};
	char buffer1[SIZE]  = {0};
	char* parsebuffer[SIZE]  = {0};

	char buf[SIZE*2] = {0};///Error Buffer
    while(flagint==0){
        pthread_mutex_lock(&mutex1);
        if(flagint == 0)
		{
			sprintf(buf,"Thread #%d: waiting for connection", x);
		    writefile(buf);
			
		}
        // If any thread completes an loop // Mainden gelen Thread
		if(cflag == 1)
		{
			// Decrase counters
			pcount3 -= 1;
			pcount2 -= 1;
			pthread_cond_signal(&cempty);
		}
		else
			cflag = 1;
        // Waits to be awakened by the main thread.
		while(flagint == 0 && pcount == 0)
			pthread_cond_wait(&cfull, &mutex1);
		if(flagint != 0)
		{
			pthread_cond_broadcast(&cempty);
			pthread_mutex_unlock(&mutex1);
			break;
		}
        // Take socket from queue
		temp = dequeue(&ports);
		pcount -= 1;
		sprintf(buf,"A connection has been delegated to thread id #%d",x);
		writefile(buf);

		//strcpy(buffer, " ");
		rsize = read(temp , buffer, SIZE);
		buffer[rsize] = '\0';
		strcpy(buffer1,buffer);
		// Indicates that the connection was received by any thread.
		pthread_mutex_lock(&tmutex);
		while(flagint == 0 && tcount == 0)
			pthread_cond_wait(&tfull, &tmutex);
		tcount = 0;
		pthread_cond_broadcast(&tempty);
		pthread_mutex_unlock(&tmutex);

		pthread_cond_broadcast(&cempty);
		pthread_mutex_unlock(&mutex1);
		// Parse message
		int h=0;
		const char s[2] = " ";
		char *token;
		token = strtok(buffer, s);
		/* walk through other tokens */
		while( token != NULL ) {
			parsebuffer[h]=malloc(sizeof(char*)*SIZE);
			strcpy(parsebuffer[h],token);
			token = strtok(NULL, s);
			h++;
		}
		
        ///Readers are threads that try to search the data structure to find out if it contains a query.
		pthread_mutex_lock(&mutex2);
		while((AW + WW) > 0)
		{
			WR++;
			pthread_cond_wait(&okToRead, &mutex2);
			WR--;
		}
		AR++;
		pthread_mutex_unlock(&mutex2);
		sprintf(buf,"Thread #%d: searching database for %s",x,buffer1);
		writefile(buf);
		int l=0,o=0;
		if (strcmp("SELECT",parsebuffer[1])==0 && strcmp("DISTINCT",parsebuffer[2])!=0 && strcmp("*",parsebuffer[2])==0) ///Sadece Select
		{
			if (strcmp("*",parsebuffer[2])==0)
			{
				char* gonder=malloc(sizeof(char)*SIZE);
				for ( l = 0; l <row1; l++)
				{
					gonder[0]='\0';	
					for ( o = 0; o < colum1; o++)
					{
						strcat(gonder,array[l][o]);
						strcat(gonder,"\t");						
					}
					strcat(gonder,"\n");
					send(temp,gonder, strlen(gonder), 0);
				}
				free(gonder);
			}
			else{

			}
			
		}
		else if (strcmp("SELECT",parsebuffer[1])==0 && strcmp("DISTINCT",parsebuffer[2])==0) ///Select +Disctinct
		{
			int t=3,ix=0,iz=0,ic=0,tx=0;
			int coltut[colum1];
			char* gonder=malloc(sizeof(char)*SIZE);
			for ( ic = 0; ic < colum1; ic++)
			{
				coltut[ic]=-1;
			}
			while (parsebuffer[t]!=NULL)
			{
				
				for ( ix = 0; ix < colum1; ix++)
				{
					
					if (strcmp(parsebuffer[t],array[0][ix])==0)
					{
						coltut[tx]=ix;
						tx++;
					}
					
				}
				t++;
			}
			for ( ix = 0; ix <row1; ix++)
			{
				gonder[0]='\0';
				for ( iz = 0; iz <colum1; iz++)
				{
					if (coltut[iz]!=-1)
					{
						strcat(gonder,array[ix][coltut[iz]]);
						strcat(gonder,"\t");
		
					}
					
				}
				
				strcat(gonder,"\n");
				send(temp,gonder, strlen(gonder), 0);
				
			}	
			free(gonder);
			
		}
		
		else if (strcmp("SELECT",parsebuffer[1])==0) ///Select +Columname
		{
			int t=2,ix=0,iz=0,ic=0,tx=0;
			int coltut[colum1];
			char* gonder=malloc(sizeof(char)*SIZE);
			for ( ic = 0; ic < colum1; ic++)
			{
				coltut[ic]=-1;
			}
			while (parsebuffer[t]!=NULL)
			{
				for ( ix = 0; ix < colum1; ix++)
				{
					
					if (strcmp(parsebuffer[t],array[0][ix])==0)
					{
						coltut[tx]=ix;
						tx++;
					}
					
				}
				t++;
			}
			for ( ix = 0; ix <row1; ix++)
			{
				gonder[0]='\0';
				for ( iz = 0; iz <colum1; iz++)
				{
					if (coltut[iz]!=-1)
					{
						strcat(gonder,array[ix][iz]);
						strcat(gonder,"\t");
						send(temp,array[ix][iz], strlen(array[ix][iz]), 0);
		
					}
					
				}
				strcat(gonder,"\n");
				send(temp,gonder, strlen(gonder), 0);
				
			}	
			free(gonder);

		}
		else if (strcmp("UPDATE",parsebuffer[1])==0) ///Just
		{
			int g=4;
			char* twoarray[2]={0};
			while (parsebuffer[g]!=NULL)
			{
				printf("ThreadParseBuffer:%s\n",parsebuffer[g]);
				//parseforupdate(parsebuffer[g],twoarray,strlen(parsebuffer[g]));
				g++;
			}
			printf("Parse1:%s\n",twoarray[0]);
			printf("Parse2:%s\n",twoarray[1]);
			
		//	printf("ParseBuffer:%s\n",parsebuffer[5]);
		}
		printf("Sonda\n");
	
    }//While
    pthread_mutex_unlock(&mutex1);
   // printf("Thread\n");
   return NULL;
}

// It was made by looking at the 770 th page of the book(The Linux Programming Interface-Michael Kerrisk).
int Daemon()
{
	int maxfd, fd;

	switch(fork())
	{
		case -1:
			return -1;
		case 0:
			break;
		default:
			exit(EXIT_SUCCESS);
	}

	if(setsid() == -1)
		return -1;

	switch(fork())
	{
		case -1:
			return -1;
		case 0:
			break;
		default:
			exit(EXIT_SUCCESS);
	}

	umask(0);

	maxfd = sysconf(_SC_OPEN_MAX);
	if(maxfd == -1)
		maxfd = 8192;
	
	// Close all of its inherited open files
	for(fd = 0; fd < maxfd; fd++)
		close(fd);
	
	return 0;
}
void splitarr(int srow,int readsize,char str[]){
   int m=0;
   const char s[2] = ",";
   char *token;
   /* get the first token */
   token = strtok(str, s);
   /* walk through other tokens */
   while(token != NULL) {
      printf( " %s\n", token );
	   array[srow][m]=malloc(sizeof(char*));
	   strcpy(array[srow][m],token);
      token = strtok(NULL, s);
	  m++;
   }
}
int splitarrfun(int sayrow,int readsize,char buf[]){
	    int i=0,m=0,n=0,k=0,flag=0;
		char temp[SIZE]={0};		
    while(i<readsize && m<=colum1){
		n=0;
    	while(buf[k]!=','){
				temp[n]=buf[k];
				k++;
				n++;
				if (buf[k]=='\0')
				{
					break;
				}
    	}	
		if((buf[k]==',' || buf[k]=='\0')){
			temp[n]='\0';
			array[sayrow][m]=malloc(sizeof(char)*SIZE);
			strcpy(array[sayrow][m],temp);
			m++;
		}///İf	
 		i=k++;
	}
	return flag;
}

int main(int argc, char  **argv)
{
    
	// Double instantiation will be prevented.
	fd = open("Daemon", O_CREAT | O_EXCL);
	if(fd == -1)
	{
		fprintf(stderr,"\n (it should not be possible to start 2 instances of the server process)\n");
		exit(EXIT_FAILURE);
	}

    int PORT,poolsize,opt;
    char* pathToLogFile;
    char* datasetPath;
    int i;
    struct timeval start, end;
    pthread_mutex_init(&mutex1,NULL);
    pthread_cond_init(&cond1,NULL);

	char writebuf[SIZE*2] = {0};
      if (argc<9)
      {
      	fprintf(stderr,"This usage is wrong. \n");
		fprintf(stderr,"./server -p PORT -o pathToLogFile –l poolSize –d datasetPath\n");
        unlink("Daemon");
		close(fd);
		exit(0);
      }
    	if (signal(SIGINT, sigint_handler) == SIG_ERR)
  	      fprintf(stderr,"\ncan't catch SIGINT %s \n",strerror(errno));

	while ((opt = getopt (argc, argv, "o d p:l:")) != -1)
  	{
	    switch (opt)
	      { 
        case 'o':
	        pathToLogFile = argv[optind];
	        break;
        case 'd':
	        datasetPath= argv[optind];
	        break;
	    case 'p':
	        PORT = atoi(optarg);
	        break;
	    case 'l':
	        poolsize=atoi(optarg);
	        break;       
       
	    default:
	        fprintf(stderr,"This usage is wrong. \n");
		    fprintf(stderr,"./server -p PORT -o pathToLogFile –l poolSize –d datasetPath\n");
			exit(0);
	      }
         
	}///While

    if(PORT < 0)
	{
		unlink("Daemon");
		close(fd);
		sprintf(writebuf,"Erorr because port number is negative.\n");
		writefile(writebuf);
		exit(EXIT_FAILURE);
	}

	if(poolsize < 2)
	{
		unlink("Daemon");
		close(fd);
		sprintf(writebuf,"Number of threads can be at least 2.\n");
		writefile(writebuf);
		exit(EXIT_FAILURE);
	}
	/*if (Daemon() == -1)
	{
		unlink("Daemon");
		close(fd);
		sprintf(writebuf,"\nerrno = %d: %s Daemon failed\n\n", errno, strerror(errno));
		writefile(writebuf);
		exit(EXIT_FAILURE);
	}*/
	fd2 = open(pathToLogFile, O_RDWR | O_CREAT | O_TRUNC, 0666);
	if(fd2 == -1)
	{
		unlink("Daemon");
		close(fd);
		fprintf(stderr, "errno = %d: %s open pathToLogFile\n\n", errno, strerror(errno));
		exit(EXIT_FAILURE);
	}
    fp = fopen(datasetPath, "r");
    if (!fp){
		sprintf(writebuf,"Can't open file\n");
	    writefile(writebuf);
	}
    else {
        gettimeofday(&start, NULL);
        CountCsv(fp,&row1,&colum1);
		array=(char***)malloc(sizeof(char**)*row1);
		for (int i = 0; i < row1; i++)
		{
			array[i]=malloc(sizeof(char*)*colum1);			
		}
        arraycol=colum1;
        char buffer[SIZE];
        char *line;
        fseek(fp,0, SEEK_SET); //Locate to the second line, the size of each English character is 1
		int j = 0;
		while ((line = fgets(buffer, sizeof(buffer), fp))!=NULL)//The loop continues when the end of the file is not read
		{
			line[strlen(line)-1]='\0';
			splitarrfun(j,strlen(line),line);	
			j++;
		}
		
    gettimeofday(&end, NULL);
	long seconds = (end.tv_sec - start.tv_sec);
	long micros = ((seconds * 1000000) + end.tv_usec) - (start.tv_usec);
	double time_taken = (double) micros / 1000000;
	sprintf(writebuf,"Dataset loaded in %lf seconds with %d records.",time_taken,row1);
	writefile(writebuf); 
        fdsocket=socket(AF_INET,SOCK_STREAM,0);
        if (fdsocket<0)
        {
			sprintf(writebuf,"Socket Error\n");
		    writefile(writebuf);
            exit(0);
        }
         pthread_t th[poolsize];
         Thread = (int**) calloc(poolsize,sizeof(int*));
         pthread_mutex_lock(&mainmutex);  ///
          for (i = 0; i <poolsize; i++)
          {
              Thread[i] = malloc(sizeof(int));
		     *(Thread[i]) = i;
            if(pthread_create(&th[i],NULL,&funThread,Thread[i])!=0){
				sprintf(writebuf,"Pthread_Error %s",strerror(errno));
		        writefile(writebuf);
            }    
          }
        pthread_mutex_unlock(&mainmutex);
        struct sockaddr_in serverAddr;
        int socket2;
        int addrlen = sizeof(serverAddr);
        int opt1 = 1;
        // Create socket file descriptor 
        if((socket1 = socket(AF_INET, SOCK_STREAM, 0))==0) 
        {
            close(fd);
			sprintf(writebuf,"Errno = %d: %s socket failed\n", errno, strerror(errno));
		    writefile(writebuf);
            exit(EXIT_FAILURE); 
        } 
        // Forcefully attaching socket to the port
        if(setsockopt(socket1, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt1, sizeof(opt1))) 
        {
            sprintf(writebuf,"Errno = %d: %s setsockopt failed\n", errno, strerror(errno));
		    writefile(writebuf);
            exit(EXIT_FAILURE); 
        }
        serverAddr.sin_family = AF_INET;; 
        serverAddr.sin_addr.s_addr = INADDR_ANY; 
        serverAddr.sin_port = htons(PORT);
		if(bind(socket1, (struct sockaddr *)&serverAddr, sizeof(serverAddr))<0) 
		{
			sprintf(writebuf, "errno = %d: %s bind failed\n", errno, strerror(errno));
			writefile(writebuf);
			unlink("Deamon");
			close(fd);
			exit(EXIT_FAILURE); 
		}

        if (listen(socket1,4096) < 0) 
        {
            sprintf(writebuf,"Errno = %d: %s listen failed\n", errno, strerror(errno));
		    writefile(writebuf);
            exit(EXIT_FAILURE); 
        }

       
    // Main Thread
	// Endless loop, it just check sigint signal arrives.
	while(flagint==0)
	{
		// Wait for connection is established between client and server
		if((socket2 = accept(socket1, (struct sockaddr *)&serverAddr, (socklen_t*)&addrlen))<0)
		{
			if(socket1 != -2) ///For Ctrl+C
			{
				sprintf(writebuf,"Errno = %d: %s accept failed\n", errno, strerror(errno));
		        writefile(writebuf);
				unlink("Daemon");
				close(fd);
				exit(EXIT_FAILURE);
			} 
		}
		pthread_mutex_lock(&mutex1);
		// Connection counter increased by 1 ///Bağlanılınca counter 1 artar
		pcount2 += 1;
		pthread_cond_broadcast(&pempty);
		if(flagint != 0)
		{
			pthread_mutex_unlock(&mutex1);
			break;
		}
		if (flagint!=0)
		{
			break;
		}
		// If it reaches the maximum number of threads, the threads are expected to finish.
		if(pcount2 > poolsize)
		{
			sprintf(writebuf,"No thread is available! Waiting for one.");
		    writefile(writebuf);
		}

		while(flagint == 0 && (pcount2 > poolsize))
			pthread_cond_wait(&cempty, &mutex1);

		// Enqueue the provided socket to queue, And increase counters
		enqueue(&ports,socket2);
		pcount += 1;
		pcount3 += 1;

		pthread_cond_broadcast(&cfull);
		pthread_mutex_unlock(&mutex1);
		// It is ensured that the connection provided by the main thread is forwarded to any thread.
		pthread_mutex_lock(&tmutex);
		while(flagint == 0 && tcount == 1)
			pthread_cond_wait(&tempty, &tmutex);
		tcount = 1;
		pthread_cond_broadcast(&tfull);
		pthread_mutex_unlock(&tmutex);
	}//Asil While

	 for (i = 0; i <poolsize; i++) ///Thread Wait
            {
               if(pthread_join(th[i],NULL)!=0){
				 sprintf(writebuf,"th[%d] Pthread_Join_Error\n",i);
		         writefile(writebuf);
                }
            }

    }///else
    pthread_mutex_destroy(&mainmutex);
    pthread_mutex_destroy(&mutex1);
    pthread_cond_destroy(&cond1);
    return 0;
}//Main
