#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <time.h>
/* usage: rtime OUTFILE cmd args...  writes: exit, wall_s, maxrss_kib */
int main(int argc,char**argv){
  struct timespec a,b; clock_gettime(CLOCK_MONOTONIC,&a);
  pid_t p=fork();
  if(!p){execvp(argv[2],argv+2);perror("exec");_exit(127);}
  int st; struct rusage ru; wait4(p,&st,0,&ru); clock_gettime(CLOCK_MONOTONIC,&b);
  int ec = WIFEXITED(st)?WEXITSTATUS(st):128+WTERMSIG(st);
  FILE*f=fopen(argv[1],"w");
  fprintf(f,"exit=%d\nwall_s=%.2f\nmaxrss_kib=%ld\nsignaled=%d\n",ec,(b.tv_sec-a.tv_sec)+(b.tv_nsec-a.tv_nsec)/1e9,ru.ru_maxrss,WIFSIGNALED(st)?WTERMSIG(st):0);
  fclose(f); return ec;
}
