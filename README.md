# heka-redis-plugin
only support redis list

RedisListInput worked with "RPOP" 

RedisListOutput worked with "LPUSH"


edit the cmake/plugin_loader.cmake file and add
```
add_external_plugin(git https://github.com/liuyangc3/heka-redis-plugin.git master)
```
or (if you don't need all the plugins)
```
add_external_plugin(git https://github.com/liuyangc3/heka-redis-plugin.git master input)
```
