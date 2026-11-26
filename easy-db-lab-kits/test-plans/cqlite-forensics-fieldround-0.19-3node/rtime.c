#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <time.h>
/* usage: rtime OUTFILE cmd args...
 * Runs cmd, samples /proc/<pid>/status every ~20 ms (VmHWM, RssAnon, RssFile) and writes: exit, wall_s, maxrss_kib (ru_maxrss when
 * available, else sampled VmHWM), hwm_kib, anon_kib, file_kib (peaks seen by the sampler), signaled.
 * Env RTIME_CAP_KIB: if the sampled RSS exceeds it the child is SIGKILLed (capped=1) so a measurement run never takes the container down. */
static long field(const char *buf, const char *name){ const char *p=strstr(buf,name); return p? atol(p+strlen(name)) : 0; }
int main(int argc,char**argv){
  struct timespec a,b; clock_gettime(CLOCK_MONOTONIC,&a);
  long cap = getenv("RTIME_CAP_KIB")? atol(getenv("RTIME_CAP_KIB")) : 0; int capped=0;
  pid_t p=fork();
  if(!p){execvp(argv[2],argv+2);perror("exec");_exit(127);}
  long hwm=0, anon=0, file=0, rss_peak=0; int st; struct rusage ru; char path[64], buf[4096];
  snprintf(path,sizeof path,"/proc/%d/status",p);
  for(;;){
    pid_t w=wait4(p,&st,WNOHANG,&ru);
    if(w==p) break;
    FILE*f=fopen(path,"r");
    if(f){ size_t n=fread(buf,1,sizeof buf-1,f); buf[n]=0; fclose(f);
      long h=field(buf,"VmHWM:"), r=field(buf,"VmRSS:"), an=field(buf,"RssAnon:"), fi=field(buf,"RssFile:");
      if(h>hwm)hwm=h; if(an>anon)anon=an; if(fi>file)file=fi; if(r>rss_peak)rss_peak=r;
      if(cap && r>cap && !capped){ capped=1; kill(p,SIGKILL); } }
    usleep(20000);
  }
  clock_gettime(CLOCK_MONOTONIC,&b);
  int ec = WIFEXITED(st)?WEXITSTATUS(st):128+WTERMSIG(st);
  long mx = ru.ru_maxrss > hwm ? ru.ru_maxrss : hwm;
  FILE*f=fopen(argv[1],"w");
  fprintf(f,"exit=%d\nwall_s=%.2f\nmaxrss_kib=%ld\nhwm_kib=%ld\nanon_kib=%ld\nfile_kib=%ld\nsignaled=%d\ncapped=%d\n",ec,(b.tv_sec-a.tv_sec)+(b.tv_nsec-a.tv_nsec)/1e9,mx,hwm,anon,file,WIFSIGNALED(st)?WTERMSIG(st):0,capped);
  fclose(f); return ec;
}
