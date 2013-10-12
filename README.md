Memcached 异步客户端 for tornado
===================================

### demo:

```python
import tornado.ioloop
import tornado.web
import tornado.gen as gen
import tornadoasyncmemcache as memcache
import time

ccs = memcache.ClientPool(['127.0.0.1:11211'], maxclients=100)


class MainHandler(tornado.web.RequestHandler):

    @tornado.web.asynchronous
    @gen.engine
    def get(self):
        test_data = yield gen.Task(ccs.get, 'test_data')
        if not test_data:
            time_str = time.strftime('%Y-%m-%d %H:%M:%S')

            yield gen.Task(ccs.set, 'test_data', 'Hello world @ %s' % time_str)
            test_data = yield gen.Task(ccs.get, 'test_data')

        self.write(test_data)
        self.finish()


application = tornado.web.Application([
    (r"/", MainHandler),
], debug=False)

if __name__ == "__main__":
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()
```

### 更新记录：

- 加入任务队列，在 100 连接数的情况下，能支持大于 100 的并发

> `ab -n 3000 -c 1000 http://127.0.0.1:8888/` 测试通过
