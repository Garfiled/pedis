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
#define CMD_LEN_LIMIT 500*1024*1024

#define ERR_CMD_CNT_FLAG     100001
#define ERR_CMD_CNT_VAL      100002
#define ERR_CMD_NOT_COMPLETE 100003
#define ERR_CMD_SUB_LEN      100004
#define ERR_CMD_LEN          100005
#define ERR_CMD_VAL          100006

class Client
{
public:
    Client(int client_fd,int size)
    {
        fd = client_fd;
        queryBuf = new char[size];
        queryCap = size;
        queryLen = 0;
        queryStart = 0;
    }
    int fd;
    char* queryBuf;
    int queryStart;
    int queryLen;
    int queryCap;
};

class CmdInfo
{
public:
    CmdInfo(int client_fd,std::vector<std::string>* cmd):fd(client_fd),args(cmd){};
    CmdInfo()
    {
        fd = 0;
        args = nullptr;
    }
    int fd;
    std::vector<std::string>* args;
};

class Pedis
{
public:
    std::map<int,Client*> client;
    threadsafe_queue<CmdInfo> q;
    std::map<std::string,std::string> _data;

};

void* worker(void* argv);
void acceptConn(int socket_fd,int epoll_fd,Pedis* pedis);
void processConn(Pedis* pedis,Client* cc);

// 解析redis协议
int myAtoi(char* p,int end,int* val);
int parseSubStr(char* cmd,int start,int end);
int parseCmdVal(char* cmd_buf,int* start_p,int cmd_length,std::string& cmd_p);
int parseCmd(char* cmd_buf,int* start_p,int cmd_length,std::vector<std::string>* cmd);

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

    if(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, socket_fd, &event) < 0)
    {
        perror("epoll add failed");
        exit(EXIT_FAILURE);
    }

    Pedis* pedis = new Pedis();

    // 创建线程池
    pthread_t pids[THREAD_NUM];
    for (int i=0;i<THREAD_NUM;i++)
    {
        int ret = pthread_create(&pids[i],NULL,worker,pedis);
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

        for(int i=0; i<ret; i++)
        {
            if ((eventList[i].events & EPOLLERR) || (eventList[i].events & EPOLLHUP) || !(eventList[i].events & EPOLLIN))
            {
                std::cout << "epoll event error" << std::endl;
                close (eventList[i].data.fd);
                continue;
            }

            if (eventList[i].data.fd == socket_fd)
            {
                acceptConn(socket_fd,epoll_fd,pedis);
            }else{
                if (pedis->client.end()==pedis->client.find(eventList[i].data.fd))
                {
                    std::cout << "client not found " << eventList[i].data.fd << std::endl;
                    continue;
                }

                processConn(pedis,pedis->client[eventList[i].data.fd]);
            }
        }
    }

    close(epoll_fd);
    close(socket_fd);

    delete pedis;

    return 0;
}

void processConn(Pedis* pedis,Client* client)
{
    Client cc = *client;
    int n = read(cc.fd,cc.queryBuf+cc.queryLen,cc.queryCap-cc.queryLen);
    if (n==0)
    {
        close(cc.fd);
        pedis->client.erase(cc.fd);
        return;
    }
    cc.queryLen+=n;

    // 开始解析命令，并决定命令到worker线程的路由

    int ret;
    int start = cc.queryStart;
    while (true)
    {
        std::vector<std::string>* cmd = new std::vector<std::string>;
        ret = parseCmd(cc.queryBuf,&start,cc.queryLen,cmd);
        if (ret==0)
        {
            cc.queryStart = start;
            pedis->q.push(CmdInfo(cc.fd,cmd));
            if (start>=cc.queryLen) {
                cc.queryLen = 0;
                cc.queryStart = 0;
                break;
            }
        } else if (ret==ERR_CMD_NOT_COMPLETE)
        {
            if (cc.queryLen>=cc.queryCap)
            {
                if (cc.queryLen<CMD_LEN_LIMIT)
                {
                    char* newBuf = new char[cc.queryLen*2];
                    memcpy(newBuf,cc.queryBuf,cc.queryLen);
                    delete cc.queryBuf;
                    cc.queryBuf = newBuf;
                    cc.queryCap = cc.queryLen*2;
                    break;
                } else {
                    std::cout << "query buffer full" << std::endl;
                    close(cc.fd);
                    pedis->client.erase(cc.fd);
                    return;
                }
            } else
            {
                break;
            }
        } else {
            close(cc.fd);
            pedis->client.erase(cc.fd);
            return;
        }
    }
    return;
}


void acceptConn(int socket_fd,int epoll_fd,Pedis* pedis)
{
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

    pedis->client[client_fd] = new Client(client_fd,4096);
}


void* worker(void* args)
{
    Pedis* pedis = (Pedis*)args;
    while (1)
    {
        CmdInfo cmdInfo;
        pedis->q.wait_and_pop(cmdInfo);
        std::vector<std::string> cmd = *(cmdInfo.args);
        int client_fd = cmdInfo.fd;
        // 执行命令
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
                if (pedis->_data.end()!=pedis->_data.find(cmd[1]))
                {
                    std::string getRet;
                    getRet.append("$");
                    getRet.append(std::to_string(pedis->_data[cmd[1]].size()));
                    getRet.append("\r\n");
                    getRet.append(pedis->_data[cmd[1]]);
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
                pedis->_data[cmd[1]]=cmd[2];
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

        delete cmdInfo.args;
    }
}

int parseCmd(char* cmd_buf,int* start_p,int cmd_length,std::vector<std::string>* cmd)
{
    int start = *start_p;
    if (cmd_buf[start]!='*')
        return ERR_CMD_CNT_FLAG;

    start+=1;
    if (start>=cmd_length)
        return ERR_CMD_NOT_COMPLETE;

    int subLen = parseSubStr(cmd_buf,start,cmd_length);
    if (subLen<0)
    {
        return ERR_CMD_NOT_COMPLETE;
    } else if (subLen==0)
    {
        return ERR_CMD_CNT_VAL;
    }
    int cmdCnt;
    int ret = myAtoi(cmd_buf+start,subLen,&cmdCnt);
    if (ret!=0)
        return ERR_CMD_CNT_VAL;

    if (cmdCnt<=0)
        return ERR_CMD_CNT_VAL;

    start+=subLen+2;
    if (start>=cmd_length)
        return ERR_CMD_NOT_COMPLETE;

    cmd->resize(cmdCnt);
    for (int i=0;i<cmdCnt;i++)
    {
        ret = parseCmdVal(cmd_buf,&start,cmd_length,(*cmd)[i]);
        if (ret!=0)
        {
            return ret;
        }
    }
    *start_p = start;
    return 0;
}

int parseCmdVal(char* cmd_buf,int* start_p,int cmd_length,std::string& cmd_p)
{
    int start = *start_p;
    if (start>=cmd_length)
        return ERR_CMD_NOT_COMPLETE;
    if (cmd_buf[start]!='$')
        return ERR_CMD_SUB_LEN;
    start += 1;
    if (start>=cmd_length)
        return ERR_CMD_NOT_COMPLETE;
    int len = parseSubStr(cmd_buf,start,cmd_length);
    if (len<0) {
        return ERR_CMD_NOT_COMPLETE;
    } else if (len==0) {
        return ERR_CMD_SUB_LEN;
    }
    int cmdLen;
    int ret = myAtoi(cmd_buf+start,len,&cmdLen);
    if (ret!=0)
        return ERR_CMD_SUB_LEN;

    if (cmdLen<=0)
        return ERR_CMD_SUB_LEN;
    start+=len+2;

    if (cmd_length-start<cmdLen+2)
        return ERR_CMD_NOT_COMPLETE;
    if (cmd_buf[start+cmdLen]!='\r' || cmd_buf[start+cmdLen+1]!='\n')
        return ERR_CMD_VAL;

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