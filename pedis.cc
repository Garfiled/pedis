#include <unistd.h>
#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <pthread.h>
#include <string>
#include <iostream>
#include <vector>
#include <map>

#define PORT 6379

using namespace std;

class ClientMgr
{
public:
    int fd;

};

void* handle(void* conn);
int parseMsgSub(char* cmd,int start,int end);
int handleCmd(char* cmd_buf,int cmd_length,std::vector<std::string>& cmd);
int myAtoi(char* p,int end,int* val);
int parseCmd(char* cmd_buf,int* start_p,int cmd_length,std::string& cmd_p);

int main(int argc, char const *argv[])
{
    int socket_fd,client_fd;
    struct sockaddr_in address;
    int addrlen = sizeof(address);

    // Creating socket file descriptor
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
    if (listen(socket_fd, 3) < 0)
    {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    while (true)
    {
        if ((client_fd = accept(socket_fd, (struct sockaddr *)&address,(socklen_t*)&addrlen))<0)
        {
            perror("accept");
            exit(EXIT_FAILURE);
        }

        ClientMgr *client = new ClientMgr();
        client->fd = client_fd;

        pthread_t pid;
        int ret = pthread_create(&pid,NULL,handle,client);
        if (ret!=0)
        {
            perror("create thread");
            exit(EXIT_FAILURE);
        }
    }

    return 0;
}


void* handle(void* args)
{
    ClientMgr *client = (ClientMgr*)args;
    int n;
    char cmd_buf[1024*16]={0};
    int cmd_length=0;
    int start=0;
    std::map<std::string,std::string> m;
    while (true)
    {
        start = 0;
        cmd_length = 0;
        n = read(client->fd,cmd_buf+cmd_length, sizeof(cmd_buf)-cmd_length);
        if (n==0)
        {
            break;
        }
        printf("client:%d msg:%s\n",client->fd,cmd_buf+cmd_length);
        cmd_length+=n;
        if (cmd_length>= sizeof(cmd_buf))
        {
            std::cout << "cmd buffer full" << std::endl;
            break;
        }
        std::vector<std::string> cmd;
        int ret = handleCmd(cmd_buf,cmd_length,cmd);
        if (ret!=0)
        {
            std::cout << "handleCmd err " << ret << std::endl;
            break;
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
                send(client->fd,"+OK\r\n",5,0);
            } else if (cmd[0] == "quit" || cmd[0] == "QUIT")
            {
                break;
            } else {
                std::cout << "unsupport cmd " << cmd[0] << std::endl;
                send(client->fd,"-unsupport cmd\r\n",16,0);
            }
            continue;
        } else if (cmd.size()==2)
        {
            if (cmd[0]=="get")
            {
                if (m.end()!=m.find(cmd[1]))
                {
                    std::string getRet;
                    getRet.append("$");
                    getRet.append(std::to_string(m[cmd[1]].size()));
                    getRet.append("\r\n");
                    getRet.append(m[cmd[1]]);
                    getRet.append("\r\n");

                    send(client->fd,getRet.c_str(),getRet.size(),0);
                } else {
                    send(client->fd,"$-1\r\n",5,0);
                }
            } else {
                std::cout << "unsupport cmd:" << cmd[0] << " " << cmd[1] << std::endl;
                send(client->fd,"-unsupport cmd\r\n",16,0);
            }
            continue;
        } else if (cmd.size()==3)
        {
            if (cmd[0]=="set")
            {
                m[cmd[1]]=cmd[2];
                send(client->fd,"+OK\r\n",5,0);
            } else {
                std::cout << "unsupport cmd:" << cmd[0] << " " << cmd[1] << " " << cmd[2] << std::endl;
                send(client->fd,"-unsupport cmd\r\n",16,0);
            }
            continue;
        } else {
            std::cout << "unsupport cmd all" << std::endl;
            send(client->fd,"-unsupport cmd\r\n",16,0);
        }


    }
    std::cout << client->fd << " quit" << std::endl;
    return NULL;
}

int handleCmd(char* cmd_buf,int cmd_length,std::vector<std::string>& cmd)
{
    int start=0;
    if (cmd_buf[0]!='*')
    {
        std::cout << "cmdCnt flag err " << cmd_buf[0] << std::endl;
        return -1;
    }

    start+=1;
    int subLen = parseMsgSub(cmd_buf,start,cmd_length);
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
        ret = parseCmd(cmd_buf,&start,cmd_length,cmd[i]);
        if (ret<0)
        {
            std::cout << "parseCmd err:" << ret<< std::endl;
            return ret*1000;
        }
    }
    return 0;
}

int parseCmd(char* cmd_buf,int* start_p,int cmd_length,std::string& cmd_p)
{
    int start = *start_p;
    if (start>=cmd_length)
        return -1;
    if (cmd_buf[start]!='$')
        return -2;
    start += 1;
    int len = parseMsgSub(cmd_buf,start,cmd_length);
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

int parseMsgSub(char* cmd,int start,int end)
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
