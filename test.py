#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date: 2013-10-12 17:39:33
# @Author: vfasky (vfasky@gmail.com)
# @Link: http://vfasky.com
# @Version: $Id$

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
