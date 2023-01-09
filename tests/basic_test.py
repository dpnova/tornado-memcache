import tornadoasyncmemcache
import tornado.testing
import tornado.gen
import tornado.ioloop

import time
import greenlet

class BaseGreenletCase(tornado.testing.AsyncTestCase):
    '''Base Test case that wraps runs with greenlets
    '''
    def __init__(self, *args, **kwargs):
        super(BaseGreenletCase, self).__init__(*args, **kwargs)
        self._origTestMethodName = self._testMethodName
        self._testMethodName = 'wrapped_run'

    @tornado.testing.gen_test
    def wrapped_run(self, result=None):
        testMethod = getattr(self, self._origTestMethodName)
        gr = greenlet.greenlet(testMethod)
        gr.switch()

        # wait for greenlet to complete
        while True:
            if gr.dead:
                break
            yield tornado.gen.moment

class BasicMemcachedTest(BaseGreenletCase):
    def test_basic(self):
        key = 'foo'
        value = 'bar'
        client = tornadoasyncmemcache.MemcachedClient('127.0.0.1')
        self.assertTrue(client.do('set', key, value))
        self.assertEqual(value, client.do('get', key))

    def test_incr_decr(self):
        key = 'foo'
        value = 0
        client = tornadoasyncmemcache.MemcachedClient('127.0.0.1')
        self.assertTrue(client.do('set',key,value))
        self.assertEqual(1, client.do('incr',key))
        self.assertEqual(0, client.do('decr',key))

if __name__ == '__main__':
    tornado.testing.main()
