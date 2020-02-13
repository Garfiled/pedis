#include <unistd.h>
#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <pthread.h>
#include <poll.h>
#include <sys/epoll.h>

#include <string>
#include <iostream>
#include <vector>
#include <map>
#include <queue>

#include "threadsafe_queue.h"

#define PORT 6379
#define MAX_EVENTS 500
#define THREAD_NUM 1

class PedisMgr
{
public:
    threadsafe_queue<int> q;
    std::map<std::string,std::string> _data;

};

void* worker(void* argv);
void acceptConn(int socket_fd,int epoll_fd);


int myAtoi(char* p,int end,int* val);
int parseSubStr(char* cmd,int start,int end);
int parseCmdVal(char* cmd_buf,int* start_p,int cmd_length,std::string& cmd_p);
int parseCmd(char* cmd_buf,int cmd_length,std::vector<std::string>& cmd);

int main(int argc, char const *argv[])
{
    int socket_fd,epoll_fd;
    struct sockaddr_in address;
    int addrlen = sizeof(address);

    if ((socket_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0)
    {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    if (bind(socket_fd, (struct sockaddr*)&address, sizeof(address))<0)
    {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }
    if (listen(socket_fd, 5) < 0)
    {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    epoll_fd = epoll_create(MAX_EVENTS);
    struct epoll_event event;
    struct epoll_event eventList[MAX_EVENTS];
    event.events = EPOLLIN|EPOLLET;
    event.data.fd = socket_fd;

    //add Event
    if(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, socket_fd, &event) < 0)
    {
        perror("epoll add failed");
        exit(EXIT_FAILURE);
    }
    PedisMgr* pMgr = new PedisMgr();
    // 创建线程池
    pthread_t pids[THREAD_NUM];
    for (int i=0;i<THREAD_NUM;i++)
    {
        int ret = pthread_create(&pids[i],NULL,worker,pMgr);
        if (ret!=0)
        {
            perror("create thread failed");
            exit(EXIT_FAILURE);
        }
    }


    //epoll
    while(1)
    {
        //epoll_wait
        int ret = epoll_wait(epoll_fd, eventList, MAX_EVENTS, 3000);

        if(ret < 0)
        {
            std::cout << "epoll error " << ret << std::endl;
            break;
        } else if(ret == 0)
        {
            continue;
        }

        //直接获取了事件数量,给出了活动的流,这里是和poll区别的关键
        int i = 0;
        for(i=0; i<ret; i++)
        {
            //错误退出
            if ((eventList[i].events & EPOLLERR) || (eventList[i].events & EPOLLHUP) || !(eventList[i].events & EPOLLIN))
            {
                std::cout << "epoll event error" << std::endl;
                close (eventList[i].data.fd);
                continue;
            }

            if (eventList[i].data.fd == socket_fd)
            {
                acceptConn(socket_fd,epoll_fd);
            }else{
                pMgr->q.push(eventList[i].data.fd);
            }
        }
    }

    close(epoll_fd);
    close(socket_fd);


    return 0;
}


void acceptConn(int socket_fd,int epoll_fd) {
    struct sockaddr_in address;
    socklen_t addrlen = sizeof(struct sockaddr_in);
    bzero(&address, addrlen);

    int client_fd = accept(socket_fd, (struct sockaddr *) &address, &addrlen);

    if (client_fd < 0) {
        std::cout << "accept error " << client_fd << std::endl;
        return;
    }
    //将新建立的连接添加到EPOLL的监听中
    struct epoll_event event;
    event.data.fd = client_fd;
    event.events = EPOLLIN | EPOLLET;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &event);
}


void* worker(void* args)
{
    PedisMgr* pMgr = (PedisMgr*)args;
    char cmd_buf[1024*16]={0};
    int cmd_length=0;
    int start=0;
    int client_fd;
    while (1)
    {
        pMgr->q.wait_and_pop(client_fd);
        start = 0;
        cmd_length = read(client_fd,cmd_buf, sizeof(cmd_buf));
        std::cout << "read:" << cmd_buf << std::endl;
        if (cmd_length==0)
        {
            close(client_fd);
            continue;
        }
        std::vector<std::string> cmd;
        int ret = parseCmd(cmd_buf,cmd_length,cmd);
        if (ret!=0)
        {
            std::cout << "handleCmd err " << ret << std::endl;
            continue;
        }

        for (int i=0;i<cmd.size();i++)
        {
            std::cout << cmd[i] << " ";
        }
        std::cout << std::endl;

        if (cmd.size()==1)
        {
            if (cmd[0] == "COMMAND")
            {
                send(client_fd,"+OK\r\n",5,0);
            } else if (cmd[0] == "quit" || cmd[0] == "QUIT")
            {
                close(client_fd);
                std::cout << "client " << client_fd << " quit" << std::endl;
                continue;
            } else {
                std::cout << "unsupport cmd " << cmd[0] << std::endl;
                send(client_fd,"-unsupport cmd\r\n",16,0);
            }
            continue;
        } else if (cmd.size()==2)
        {
            if (cmd[0]=="get")
            {
                if (pMgr->_data.end()!=pMgr->_data.find(cmd[1]))
                {
                    std::string getRet;
                    getRet.append("$");
                    getRet.append(std::to_string(pMgr->_data[cmd[1]].size()));
                    getRet.append("\r\n");
                    getRet.append(pMgr->_data[cmd[1]]);
                    getRet.append("\r\n");

                    send(client_fd,getRet.c_str(),getRet.size(),0);
                } else {
                    send(client_fd,"$-1\r\n",5,0);
                }
            } else {
                std::cout << "unsupport cmd:" << cmd[0] << " " << cmd[1] << std::endl;
                send(client_fd,"-unsupport cmd\r\n",16,0);
            }
            continue;
        } else if (cmd.size()==3)
        {
            if (cmd[0]=="set")
            {
                pMgr->_data[cmd[1]]=cmd[2];
                send(client_fd,"+OK\r\n",5,0);
            } else {
                std::cout << "unsupport cmd:" << cmd[0] << " " << cmd[1] << " " << cmd[2] << std::endl;
                send(client_fd,"-unsupport cmd\r\n",16,0);
            }
            continue;
        } else {
            std::cout << "unsupport cmd all" << std::endl;
            send(client_fd,"-unsupport cmd\r\n",16,0);
        }
    }
}

int parseCmd(char* cmd_buf,int cmd_length,std::vector<std::string>& cmd)
{
    int start=0;
    if (cmd_buf[0]!='*')
    {
        std::cout << "cmdCnt flag err " << cmd_buf[0] << std::endl;
        return -1;
    }

    start+=1;
    int subLen = parseSubStr(cmd_buf,start,cmd_length);
    int cmdCnt;
    int ret = myAtoi(cmd_buf+start,subLen,&cmdCnt);
    if (ret!=0)
    {
        std::cout << "cmdCnt atoi err " << ret << std::endl;
        return ret;
    }

    if (cmdCnt<=0)
    {
        std::cout << "cmdCnt value err:" << cmdCnt<< std::endl;
        return -100;
    }

    start+=subLen+2;
    cmd.resize(cmdCnt);
    for (int i=0;i<cmdCnt;i++)
    {
        ret = parseCmdVal(cmd_buf,&start,cmd_length,cmd[i]);
        if (ret<0)
        {
            std::cout << "parseCmd err:" << ret<< std::endl;
            return ret*1000;
        }
    }
    return 0;
}

int parseCmdVal(char* cmd_buf,int* start_p,int cmd_length,std::string& cmd_p)
{
    int start = *start_p;
    if (start>=cmd_length)
        return -1;
    if (cmd_buf[start]!='$')
        return -2;
    start += 1;
    int len = parseSubStr(cmd_buf,start,cmd_length);
    if (len<=0)
        return -3;
    int cmdLen;
    int ret = myAtoi(cmd_buf+start,len,&cmdLen);
    if (cmdLen<0)
    {
        return ret*10;
    }
    start+=len+2;
    cmd_p.append(cmd_buf+start,cmdLen);
    start+=cmdLen+2;
    *start_p = start;
    return 0;
}

int myAtoi(char* p,int end,int* val)
{
    int value= 0;
    int sign = 1;
    if (end<=0)
        return -1;
    for (int i=0;i<end;i++)
    {
        if (p[i]=='-')
            sign = -1;
        else if (p[i]<'0' || p[i]>'9')
            return -2;
        else
            value = value * 10 + p[i]-'0';
    }
    *val = value;
    return 0;
}

int parseSubStr(char* cmd,int start,int end)
{
    for (int i = start;i<end-1;i++)
    {
        if (cmd[i]=='\r' && cmd[i+1]=='\n')
        {
            return i-start;
        }
    }
    return -1;
}