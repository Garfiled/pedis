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
#include <fstream>
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

class RecordMeta
{
public:
    RecordMeta(int offset,int size):rec_offset(offset),rec_size(size){};
    RecordMeta():rec_offset(0),rec_size(0){};
    int rec_offset;
    int rec_size;
};


class Pedis
{
public:
    std::map<int,Client*> client;
    threadsafe_queue<CmdInfo> q;
    std::map<std::string,RecordMeta> records;

    std::fstream* db_file;
    int db_size;

    void send(int fd);
    void sendOkStat(int fd);
    void sendUnSupportStat(int fd);
    void sendString(int fd,std::string&);
    void sendCharStar(int fd,char* buf,int n);

    void handleCmdSet(int fd,std::string& key,std::string& val);
    void handleCmdGet(int fd,std::string&);

    int init(std::string& path);
};


void* worker(void* argv);
void acceptConn(int socket_fd,int epoll_fd,Pedis* pedis);
void processQuery(Pedis* pedis,Client* cc);

// 解析redis协议
int myAtoi(char* p,int end,int* val);
int parseSubStr(char* cmd,int start,int end);
int parseCmdVal(char* cmd_buf,int* start_p,int cmd_length,std::string& cmd_p);
int parseCmd(char* cmd_buf,int* start_p,int cmd_length,std::vector<std::string>* cmd);

const char* pedis_db_header_magic = "Pedis";
const char* pedis_record_header_magic = "aa55";

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

    std::string path("./pedis.db");
    int ret = pedis->init(path);
    if (ret!=0)
    {
        std::cout << "pedis init failed " << ret << std::endl;
        exit(EXIT_FAILURE);
    }
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

                processQuery(pedis,pedis->client[eventList[i].data.fd]);
            }
        }
    }

    close(epoll_fd);
    close(socket_fd);

    delete pedis;

    return 0;
}

void processQuery(Pedis* pedis,Client* client)
{
    int n = read(client->fd,client->queryBuf+client->queryLen,client->queryCap-client->queryLen);
    if (n==0)
    {
        close(client->fd);
        pedis->client.erase(client->fd);
        delete client;
        return;
    }
    client->queryLen+=n;

    // 开始解析命令，并决定命令到worker线程的路由

    int ret;
    int start = client->queryStart;
    while (true)
    {
        std::vector<std::string>* cmd = new std::vector<std::string>;
        // 结果存至cmd，如 [set name liu] 或 [set name]
        ret = parseCmd(client->queryBuf,&start,client->queryLen,cmd);
        if (ret==0)
        {
            client->queryStart = start;

            // 将命令发送到任务队列
            // TODO
            // 根据业务场景的不同，按命令保序、key、读写等方式决定命令到worker线程的路由方式
            pedis->q.push(CmdInfo(client->fd,cmd));

            if (start>=client->queryLen) {
                client->queryLen = 0;
                client->queryStart = 0;
                break;
            }
        } else if (ret==ERR_CMD_NOT_COMPLETE)
        {
            if (client->queryLen>=client->queryCap)
            {
                if (client->queryLen<CMD_LEN_LIMIT)
                {
                    char* newBuf = new char[client->queryLen*2];
                    memcpy(newBuf,client->queryBuf,client->queryLen);
                    delete client->queryBuf;
                    client->queryBuf = newBuf;
                    client->queryCap = client->queryLen*2;
                    break;
                } else {
                    std::cout << "query buffer full" << std::endl;
                    close(client->fd);
                    pedis->client.erase(client->fd);
                    return;
                }
            } else
            {
                break;
            }
        } else {
            close(client->fd);
            pedis->client.erase(client->fd);
            delete client;
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
    event.events = EPOLLIN;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &event);

    pedis->client[client_fd] = new Client(client_fd,4096);
}

void Pedis::sendOkStat(int fd)
{
    ::send(fd,"+OK\r\n",5,0);
}

void Pedis::sendUnSupportStat(int fd)
{
    ::send(fd,"-unsupport cmd\r\n",16,0);
}

void Pedis::sendString(int fd,std::string& str)
{
    std::string ret;
    ret.reserve(str.size()+5+4);
    ret.append("$");
    ret.append(std::to_string(str.size()));
    ret.append("\r\n");
    ret.append(str);
    ret.append("\r\n");

    ::send(fd,ret.c_str(),ret.size(),0);
}

void Pedis::sendCharStar(int fd,char* buf,int n)
{
    std::string ret;
    ret.reserve(n+5+4);
    ret.append("$");
    ret.append(std::to_string(n));
    ret.append("\r\n");
    ret.append(buf,n);
    ret.append("\r\n");

    ::send(fd,ret.c_str(),ret.size(),0);
}

int Pedis::init(std::string& path)
{
    std::cout << "pedis init " << path << std::endl;
    {
        std::ifstream infile(path);
        if (!infile.is_open())
        {
            std::ofstream ofile(path);
            ofile.close();
        } else {
            infile.close();
        }

    }

    std::fstream* file = new std::fstream();
    file->open(path, std::ios::out | std::ios::in|std::ios::binary);
    if (!file->is_open())
    {
        std::cout << "open db file failed" << std::endl;
        return -1;
    }
    this->db_file = file;
    this->db_file->seekg(0,std::ios::end);
    this->db_size = this->db_file->tellg();
    if (this->db_size==0)
    {
        this->db_file->seekp(0,std::ios::beg);
        char buf[4096];
        memcpy(buf,"Pedis",5);
        this->db_file->write(buf,sizeof(buf));
        this->db_file->sync();
        this->db_size = sizeof(buf);
    }


    this->db_file->seekg(0,std::ios::beg);
    char buf[4096];
    this->db_file->read(buf,sizeof(buf));
    if (strncmp(buf,pedis_db_header_magic,5)!=0)
    {
        std::cout << "file Pedis header err" << std::endl;
        return -1;
    }

    int start = 4096;
    short record_key_len = 0;
    int record_val_size = 0;
    int record_total = 0;
    int record_size = 0;
    while (start < this->db_size)
    {
        this->db_file->seekg(start,std::ios::beg);
        this->db_file->read(buf,sizeof(buf));
        // check magic number
        if (strncmp(buf,pedis_record_header_magic,4) !=0)
        {
            std::cout << "file record magic err "<< start << std::endl;
            return -1;
        }
        record_key_len = *((short*)&buf[4]);
        if (record_key_len>2048)
        {
            std::cout << "file key length too long "<< record_key_len << std::endl;
            return -1;
        }

        record_val_size = *((int*)&buf[4+2+record_key_len]);

        std::string key(&buf[4+2],record_key_len);
        record_total = 4+2+record_key_len+4+record_val_size;
        if (record_total%4096>0)
        {
            record_size = (record_total/4096+1)*4096;
        } else
        {
            record_size = record_total;
        }
        if (start+record_size<=this->db_size)
        {
            RecordMeta recordMeta(start,record_size);
            this->records[key] =recordMeta;
        } else {
            std::cout << "file record length err " << std::endl;
            return -1;
        }
        start += record_size;
    }
    std::cout << "open " << path << " ok" << std::endl;

    return 0;
}

void Pedis::handleCmdSet(int client_fd, std::string& key,std::string& val) {
    int record_total = 4+2+key.size()+4+val.size();
    int record_size;
    if (record_total%4096>0)
    {
        record_size = (record_total/4096+1)*4096;
    } else
    {
        record_size = record_total;
    }

    char *buf = new char[record_size];
    short key_size = key.size();
    int val_size = val.size();
    memcpy(buf, "aa55", 4);
    memcpy(buf + 4, &key_size, 4);
    memcpy(buf + 4 + 2, key.c_str(), key_size);
    memcpy(buf + 4 + 2 + key_size, &val_size, 4);
    memcpy(buf + 4 + 2 + key_size + 4, val.c_str(),val.size());

    this->db_file->seekp(this->db_size, std::ios::beg);
    this->db_file->write(buf, record_size);

    this->records[key]=RecordMeta(this->db_size,record_size);
    this->db_size += record_size;
    this->sendOkStat(client_fd);
    delete buf;
}

// TODO
// 实现流式返回
// direct IO
// CRC check
void Pedis::handleCmdGet(int client_fd, std::string &key)
{
    if (this->records.end()!=this->records.find(key))
    {
        RecordMeta recMeta = this->records[key];

        char* buf = new char[recMeta.rec_size];
        this->db_file->seekg(recMeta.rec_offset,std::ios::beg);
        this->db_file->read(buf,recMeta.rec_size);

        if (strncmp(buf,pedis_record_header_magic,4) !=0)
        {
            ::send(client_fd,"record magic err",16,0);
            return;
        }

        short record_key_size = *((short*)&buf[4]);
        int record_val_size = *((int*)&buf[4+2+record_key_size]);
        this->sendCharStar(client_fd,buf+4+2+record_key_size+4,record_val_size);

        delete buf;
        return;
    }
    ::send(client_fd,"$-1\r\n",5,0);
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
                pedis->sendOkStat(client_fd);
            } else if (cmd[0] == "quit" || cmd[0] == "QUIT")
            {
                close(client_fd);
                std::cout << "client " << client_fd << " quit" << std::endl;
            } else {
                pedis->sendUnSupportStat(client_fd);
            }
            continue;
        } else if (cmd.size()==2)
        {
            if (cmd[0]=="get")
            {
                pedis->handleCmdGet(client_fd,cmd[1]);
            } else {
                pedis->sendUnSupportStat(client_fd);
            }
            continue;
        } else if (cmd.size()==3)
        {
            if (cmd[0]=="set")
            {
                pedis->handleCmdSet(client_fd,cmd[1],cmd[2]);
            } else {
                pedis->sendUnSupportStat(client_fd);
            }
            continue;
        } else {
            pedis->sendUnSupportStat(client_fd);
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