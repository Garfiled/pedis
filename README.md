# pedis

redis protocol to store and read pictures(KB) on disk


```
g++ pedis.cc -std=c++11 -o pedis -lpthread
```

```c
redis-cli set mypicture xxx
redis-cli get mypicture
```

