## Pedis

implement redis protocol to store and read pictures(KB) on disk

## Use
```
g++ pedis.cc -std=c++11 -o pedis -lpthread

./pedis
redis-cli set mypicture_key xxx
redis-cli get mypicture_key
```
## Features
- redis protocol parser
- epoll

## Todo
- support hset hget with picture key group
- support cmd router define mode
- directIO read
- stream read
- multi thread


