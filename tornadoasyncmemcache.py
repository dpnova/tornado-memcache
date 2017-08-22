#!/usr/bin/env python

import sys
import socket
import time
import types
from tornado import iostream, ioloop
from functools import partial
import collections
import functools
import greenlet
import logging
import os

"""
    # Minimal example to show how to use the client. This is a lower level client
    # that only connects to a single host. Managing multiple connections is left up
    # to the calling code.

    # The client will allow up to pool_size concurrent requests/connections at a time
    # If more concurrent requests are issued, they will queue up and be scheduled
    # according to the ioloop. A queued request will wait up to wait_queue_timeout
    # before raising a timeout exception.


    cc = MemcachedClient('localhost:11211')
    cc.do('set','key','value')
    assert cc.do('get','key') == 'value'

"""


class MemcachedClient(object):

    CMDS = set(['get', 'replace', 'set', 'decr', 'incr', 'delete',
            'get_many','set_many','append','prepend',
            'delete_many', 'add','touch'])

    def __init__(self,
                 server,
                 pool_size=5,
                 wait_queue_timeout=5,
                 connect_timeout=5,
                 net_timeout=2):

        self._server = server
        self._pool_size = pool_size
        self._wait_queue_timeout = wait_queue_timeout
        self._connect_timeout = connect_timeout
        self._net_timeout = net_timeout

        self._clients = self._create_clients()
        self.pool = GreenletBoundedSemaphore(self._pool_size)

    def _create_clients(self):
        return collections.deque([
            Client(self._server,
                   connect_timeout=self._connect_timeout,
                   net_timeout=self._net_timeout) for i in xrange(self._pool_size)])

    def _execute_command(self, client, cmd, *args, **kwargs):
        kwargs['callback'] = partial(self._gen_cb, c=client)
        return getattr(client, cmd)(*args, **kwargs)

    #wraps _execute_command to reinitialize clients in case of server disconnection
    def do(self, cmd, *args, **kwargs):
        if cmd not in self.CMDS:
            raise Exception('Command %s not supported' % cmd)

        if not self._clients:
            self._clients = self._create_clients()

        if not self.pool.acquire(timeout=self._wait_queue_timeout):
            raise AsyncMemcachedException('Timed out waiting for connection')

        try:
            client = self._clients.popleft()
            if not client:
                raise Exception(
                    "Acquired semaphore without client in free list, something weird is happening")
            return self._execute_command(client, cmd, *args, **kwargs)
        except IOError as e:
            if e.message == 'Stream is closed':
                client.reconnect()
                return self._execute_command(client, cmd, *args, **kwargs)
            raise
        finally:
            self._clients.append(client)
            self.pool.release()

    def _gen_cb(self, response, c, *args, **kwargs):
        return response

class AsyncMemcachedException(Exception):
    pass

class Client(object):
    """
    Object representing a pool of memcache servers.

    See L{memcache} for an overview.

    In all cases where a key is used, the key can be either:
        1. A simple hashable type (string, integer, etc.).
        2. A tuple of C{(hashvalue, key)}.  This is useful if you want to avoid
        making this module calculate a hash value.  You may prefer, for
        example, to keep all of a given user's objects on the same memcache
        server, so you could use the user's unique id as the hash value.

    @group Setup: __init__, set_servers, forget_dead_hosts, disconnect_all, debuglog
    @group Insertion: set, add, replace
    @group Retrieval: get, get_multi
    @group Integers: incr, decr
    @group Removal: delete
    @sort: __init__, set_servers, forget_dead_hosts, disconnect_all, debuglog,\
           set, add, replace, get, get_multi, incr, decr, delete
    """
    _FLAG_PICKLE  = 1<<0
    _FLAG_INTEGER = 1<<1
    _FLAG_LONG    = 1<<2

    def __init__(self, server, connect_timeout=5, net_timeout=5, io_loop=None):
        io_loop = io_loop or ioloop.IOLoop.current()
        self.io_loop = io_loop
        self.connect_timeout = connect_timeout
        self.net_timeout = net_timeout
        self.set_server(server)

    def set_server(self, server):
        """
        Set the pool of servers used by this client.

        @param servers: an array of servers.
        Servers can be passed in two forms:
            1. Strings of the form C{"host:port"}, which implies a default weight of 1.
            2. Tuples of the form C{("host:port", weight)}, where C{weight} is
            an integer weight value.
        """
        self.server = MemcachedConnection(
            server,
            connect_timeout=self.connect_timeout,
            net_timeout=self.net_timeout
        )

    def _get_server(self, key):
        if self.server.connect():
            return self.server, key

    def reconnect(self):
        self.server.close()
        self.server.connect()

    def delete(self, key, time=0, callback=None):
        '''Deletes a key from the memcache.

        @return: Nonzero on success.
        @rtype: int
        '''
        server, key = self._get_server(key)
        if time:
            cmd = "delete %s %d" % (key, time)
        else:
            cmd = "delete %s" % key

        return server.send_cmd(cmd, callback=partial(self._delete_send_cb,server, callback))

    def _delete_send_cb(self, server, callback):
        return server.expect("DELETED",callback=partial(self._expect_cb, callback=callback))

    def incr(self, key, delta=1, callback=None):
        """
        Sends a command to the server to atomically increment the value for C{key} by
        C{delta}, or by 1 if C{delta} is unspecified.  Returns None if C{key} doesn't
        exist on server, otherwise it returns the new value after incrementing.

        Note that the value for C{key} must already exist in the memcache, and it
        must be the string representation of an integer.

        >>> mc.set("counter", "20")  # returns 1, indicating success
        1
        >>> mc.incr("counter")
        21
        >>> mc.incr("counter")
        22

        Overflow on server is not checked.  Be aware of values approaching
        2**32.  See L{decr}.

        @param delta: Integer amount to increment by (should be zero or greater).
        @return: New value after incrementing.
        @rtype: int
        """
        return self._incrdecr("incr", key, delta, callback=callback)

    def decr(self, key, delta=1, callback=None):
        """
        Like L{incr}, but decrements.  Unlike L{incr}, underflow is checked and
        new values are capped at 0.  If server value is 1, a decrement of 2
        returns 0, not -1.

        @param delta: Integer amount to decrement by (should be zero or greater).
        @return: New value after decrementing.
        @rtype: int
        """
        return self._incrdecr("decr", key, delta, callback=callback)

    def _incrdecr(self, cmd, key, delta, callback):
        server, key = self._get_server(key)
        cmd = "%s %s %d" % (cmd, key, delta)

        return server.send_cmd(cmd, callback=partial(self._send_incrdecr_check_cb,server, callback))

    def _send_incrdecr_cb(self, server, callback):
        return server.readline(callback=partial(self._send_incrdecr_check_cb, callback=callback))

    def _send_incrdecr_check_cb(self, line, callback):
        return self.finish(partial(callback,int(line)))

    def append(self, key, val, time=0, callback=None):
        return self._set("append", key, val, time, callback)

    def prepend(self, key, val, time=0, callback=None):
        return self._set("prepend", key, val, time, callback)

    def add(self, key, val, time=0, callback=None):
        '''
        Add new key with value.

        Like L{set}, but only stores in memcache if the key doesn't already exist.

        @return: Nonzero on success.
        @rtype: int
        '''
        return self._set("add", key, val, time, callback)

    def replace(self, key, val, time=0, callback=None):
        '''Replace existing key with value.

        Like L{set}, but only stores in memcache if the key already exists.
        The opposite of L{add}.

        @return: Nonzero on success.
        @rtype: int
        '''
        return self._set("replace", key, val, time, callback)

    def cas(self, key, value, cas, time=0, callback=None):
        return self._set("cas",key,value,time,callback,cas=cas)

    def set(self, key, val, time=0, callback=None):
        '''Unconditionally sets a key to a given value in the memcache.

        The C{key} can optionally be an tuple, with the first element being the
        hash value, if you want to avoid making this module calculate a hash value.
        You may prefer, for example, to keep all of a given user's objects on the
        same memcache server, so you could use the user's unique id as the hash
        value.

        @return: Nonzero on success.
        @rtype: int
        '''
        return self._set("set", key, val, time, callback)

    def set_many(self, values, time=0, callback=None):
        for key,val in values.iteritems():
            self.set(key,val,time=time,callback=lambda x: x)

        return callback(None)

    def delete_many(self, keys, callback=None):
        for key in keys:
            self.delete(key,callback=lambda x: x)

        return callback(None)

    def _set(self, cmd, key, val, time, callback, cas=None):
        server, key = self._get_server(key)

        flags = 0
        if isinstance(val, types.StringTypes):
            pass
        elif isinstance(val, int):
            flags |= Client._FLAG_INTEGER
            val = "%d" % val
        elif isinstance(val, long):
            flags |= Client._FLAG_LONG
            val = "%d" % val
        else:
            # A bit odd to silently string it, but that's what pymemcache
            # does. Ideally we should be raising an exception here.
            val = str(val)

        extra = ''
        if cas is not None:
            extra += ' ' + cas

        fullcmd = "%s %s %d %d %d%s\r\n%s" % (cmd, key, flags, time, len(val), extra, val)
        response = server.send_cmd(fullcmd, callback=partial(
            self._set_send_cb, server=server, callback=callback))

        response = response.strip()
        if response == 'STORED':
            return True
        elif response == 'NOT_STORED':
            return False
        elif response == 'NOT_FOUND':
            return None
        elif response == 'EXISTS':
            return False
        else:
            self.server.close()
            raise AsyncMemcachedException("Unknown response")

    def touch(self, key, time=0, callback=None):
        server, key = self._get_server(key)
        return server.send_cmd("touch %s %d" % (key, time),
            callback=partial(self._set_send_cb, server=server, callback=callback)).startswith('TOUCHED')

    def _set_send_cb(self, server, callback):
        return server.expect("STORED", callback=partial(self._expect_cb, value=None, callback=callback))

    def get(self, key, callback):
        '''Retrieves a key from the memcache.

        @return: The value or None.
        '''
        server, key = self._get_server(key)

        return server.send_cmd("get %s" % key, partial(self._get_send_cb, server=server, callback=callback))

    def get_many(self, keys, callback):
        server, keys = self._get_server(keys)
        return server.send_cmd('get' + ' ' + ' '.join(keys), partial(self._get_many_send_cb, server=server, callback=callback))

    def _get_many_send_cb(self, server, callback):
        return self._expectvalues(server, callback=partial(self._get_expectvals_cb, server=server, callback=callback))

    def _get_send_cb(self, server, callback):
        return self._expectvalue(server, line=None, callback=partial(self._get_expectval_cb, server=server, callback=callback))

    def _get_expectval_cb(self, rkey, flags, rlen, done, server, callback):
        if not rkey:
            return self.finish(partial(callback,None))
        return self._recv_value(server, flags, rlen, partial(self._get_recv_cb, server=server, callback=callback))

    def _get_expectvals_cb(self, rkey, flags, rlen, done, server, callback):
        if not rkey:
            return self.finish(partial(callback,(None,None,done)))
        return self._recv_value(server, flags, rlen, partial(self._get_many_recv_cb, rkey=rkey, server=server, callback=callback))

    def _get_recv_cb(self, value, server, callback):
        return server.expect("END", partial(self._expect_cb, value=value, callback=callback))

    def _get_many_recv_cb(self, value, rkey, server, callback):
        return rkey, self._expect_cb(value=value, callback=callback), False

    def _expect_cb(self, read_value=None, value=None, callback=None):
        if value is None:
            value = read_value
        return self.finish(partial(callback,value))

    def _expectvalue(self, server, line=None, callback=None):
        if not line:
            return server.readline(partial(self._expectvalue_cb, callback=callback))
        else:
            return self._expectvalue_cb(line, callback)

    def _expectvalues(self, server, callback=None):
        result = {}
        while True:
            key, val, done = server.readline(partial(self._expectvalue_cb, callback=callback))
            if done:
                break
            result[key] = val
        return result

    def _expectvalue_cb(self, line, callback):
        if line.startswith('VALUE'):
            resp, rkey, flags, len = line.split()
            flags = int(flags)
            rlen = int(len)
            return callback(rkey, flags, rlen, False)
        elif line.startswith('END'):
            return callback(None, None, None, True)
        else:
            return callback(None, None, None, True)

    def _recv_value(self, server, flags, rlen, callback):
        rlen += 2 # include \r\n
        return server.recv(rlen, partial(self._recv_value_cb,rlen=rlen, flags=flags, callback=callback))

    def _recv_value_cb(self, buf, flags, rlen, callback):
        if len(buf) != rlen:
            raise AsyncMemcachedException("received %d bytes when expecting %d" % (len(buf), rlen))

        if len(buf) == rlen:
            buf = buf[:-2]  # strip \r\n

        if flags == 0:
            val = buf
        elif flags & Client._FLAG_INTEGER:
            val = int(buf)
        elif flags & Client._FLAG_LONG:
            val = long(buf)

        return self.finish(partial(callback,val))

    def finish(self, callback):
        return callback()

class MemcachedIOStream(iostream.IOStream):
    def can_read_sync(self, num_bytes):
        return self._read_buffer_size >= num_bytes

def _check_deadline(cleanup_cb=None):
    gr = greenlet.getcurrent()
    if hasattr(gr, 'is_deadlined') and \
            gr.is_deadlined():
        if cleanup_cb:
            cleanup_cb()
        try:
            gr.do_deadline()
        except AttributeError:
            logging.exception(
                'Greenlet %s has \'is_deadlined\' but not \'do_deadline\'')

def green_sock_method(method):
    """Wrap a GreenletSocket method to pause the current greenlet and arrange
       for the greenlet to be resumed when non-blocking I/O has completed.
    """
    @functools.wraps(method)
    def _green_sock_method(self, *args, **kwargs):
        self.child_gr = greenlet.getcurrent()
        main = self.child_gr.parent
        assert main, "Using async client in non-async environment. Must be on a child greenlet"

        # Run on main greenlet
        def closed(gr):
            # The child greenlet might have died, e.g.:
            # - An operation raised an error within PyMongo
            # - PyMongo closed the MotorSocket in response
            # - GreenletSocket.close() closed the IOStream
            # - IOStream scheduled this closed() function on the loop
            # - PyMongo operation completed (with or without error) and
            #       its greenlet terminated
            # - IOLoop runs this function
            if not gr.dead:
                gr.throw(socket.error("Close called, killing memcached operation"))

        # send the error to this greenlet if something goes wrong during the
        # query
        self.stream.set_close_callback(functools.partial(closed, self.child_gr))

        try:
            # Add timeout for closing non-blocking method call
            if self.timeout and not self.timeout_handle:
                self.timeout_handle = self.io_loop.add_timeout(
                    time.time() + self.timeout, self._switch_and_close)

            # method is GreenletSocket.send(), recv(), etc. method() begins a
            # non-blocking operation on an IOStream and arranges for
            # callback() to be executed on the main greenlet once the
            # operation has completed.
            method(self, *args, **kwargs)

            # Pause child greenlet until resumed by main greenlet, which
            # will pass the result of the socket operation (data for recv,
            # number of bytes written for sendall) to us.
            socket_result = main.switch()

            return socket_result
        except socket.error:
            raise
        except IOError, e:
            # If IOStream raises generic IOError (e.g., if operation
            # attempted on closed IOStream), then substitute socket.error,
            # since socket.error is what PyMongo's built to handle. For
            # example, PyMongo will catch socket.error, close the socket,
            # and raise AutoReconnect.
            raise socket.error(str(e))
        finally:
            # do this here in case main.switch throws

            # Remove timeout handle if set, since we've completed call
            if self.timeout_handle:
                self.io_loop.remove_timeout(self.timeout_handle)
                self.timeout_handle = None

            # disable the callback to raise exception in this greenlet on socket
            # close, since the greenlet won't be around to raise the exception
            # in (and it'll be caught on the next query and raise an
            # AutoReconnect, which gets handled properly)
            self.stream.set_close_callback(None)

            def cleanup_cb():
                self.stream.close()

            _check_deadline(cleanup_cb)

    return _green_sock_method

class GreenletSocket(object):
    """Replace socket with a class that yields from the current greenlet, if
    we're on a child greenlet, when making blocking calls, and uses Tornado
    IOLoop to schedule child greenlet for resumption when I/O is ready.

    We only implement those socket methods actually used by pymongo.
    """
    def __init__(self, sock, io_loop, use_ssl=False):
        self.use_ssl = use_ssl
        self.io_loop = io_loop
        self.timeout = None
        self.timeout_handle = None
        if self.use_ssl:
            raise Exception("SSL isn't supported")
        else:
            self.stream = MemcachedIOStream(sock, io_loop=io_loop)

    def setsockopt(self, *args, **kwargs):
        self.stream.socket.setsockopt(*args, **kwargs)

    def settimeout(self, timeout):
        self.timeout = timeout

    def _switch_and_close(self):
        # called on timeout to switch back to child greenlet
        self.close()
        if self.child_gr is not None:
            self.child_gr.throw(IOError("Socket timed out"))

    @green_sock_method
    def connect(self, pair):
        # do the connect on the underlying socket asynchronously...
        self.stream.connect(pair, greenlet.getcurrent().switch)

    @green_sock_method
    def write(self, data):
        # do the send on the underlying socket synchronously...
        try:
            self.stream.write(data, greenlet.getcurrent().switch)
        except IOError as e:
            raise socket.error(str(e))

        if self.stream.closed():
            raise socket.error("connection closed")

    def recv(self, num_bytes):
        # if we have enough bytes in our local buffer, don't yield
        if self.stream.can_read_sync(num_bytes):
            return self.stream._consume(num_bytes)
        # else yield while we wait on Mongo to send us more
        else:
            return self.recv_async(num_bytes)

    @green_sock_method
    def recv_async(self, num_bytes):
        # do the recv on the underlying socket... come back to the current
        # greenlet when it's done
        return self.stream.read_bytes(num_bytes, greenlet.getcurrent().switch)

    @green_sock_method
    def read_until(self, *args, **kwargs):
        return self.stream.read_until(*args, callback=greenlet.getcurrent().switch, **kwargs)

    def close(self):
        # since we're explicitly handling closing here, don't raise an exception
        # via the callback
        self.stream.set_close_callback(None)

        sock = self.stream.socket
        try:
            try:
                self.stream.close()
            except KeyError:
                # Tornado's _impl (epoll, kqueue, ...) has already removed this
                # file descriptor from its dict.
                pass
        finally:
            # Sometimes necessary to avoid ResourceWarnings in Python 3:
            # specifically, if the fd is closed from the OS's view, then
            # stream.close() throws an exception, but the socket still has an
            # fd and so will print a ResourceWarning. In that case, calling
            # sock.close() directly clears the fd and does not raise an error.
            if sock:
                sock.close()

    def fileno(self):
        return self.stream.socket.fileno()

class GreenletSemaphore(object):
    """
        Tornado IOLoop+Greenlet-based Semaphore class
    """

    def __init__(self, value=1, io_loop=None):
        if value < 0:
            raise ValueError("semaphore initial value must be >= 0")
        self._value = value
        self._waiters = []
        self._waiter_timeouts = {}

        self._ioloop = io_loop if io_loop else ioloop.IOLoop.current()

    def _handle_timeout(self, timeout_gr):
        if len(self._waiters) > 1000:
            logging.error('waiters size: %s on pid: %s', len(self._waiters),
                    os.getpid())
        # should always be there, but add some safety just in case
        if timeout_gr in self._waiters:
            self._waiters.remove(timeout_gr)

        if timeout_gr in self._waiter_timeouts:
            self._waiter_timeouts.pop(timeout_gr)

        timeout_gr.switch()

    def acquire(self, blocking=True, timeout=None):
        if not blocking and timeout is not None:
            raise ValueError("can't specify timeout for non-blocking acquire")

        current = greenlet.getcurrent()
        parent = current.parent
        assert parent, "Must be called on child greenlet"

        start_time = time.time()

        # if the semaphore has a postive value, subtract 1 and return True
        if self._value > 0:
            self._value -= 1
            return True
        elif not blocking:
            # non-blocking mode, just return False
            return False
        # otherwise, we don't get the semaphore...
        while True:
            self._waiters.append(current)
            if timeout:
                callback = functools.partial(self._handle_timeout, current)
                self._waiter_timeouts[current] = \
                        self._ioloop.add_timeout(time.time() + timeout,
                                                 callback)

            # yield back to the parent, returning when someone releases the
            # semaphore
            #
            # because of the async nature of the way we yield back, we're
            # not guaranteed to actually *get* the semaphore after returning
            # here (someone else could acquire() between the release() and
            # this greenlet getting rescheduled). so we go back to the loop
            # and try again.
            #
            # this design is not strictly fair and it's possible for
            # greenlets to starve, but it strikes me as unlikely in
            # practice.
            try:
                parent.switch()
            finally:
                # need to wake someone else up if we were the one
                # given the semaphore
                def _cleanup_cb():
                    if self._value > 0:
                        self._value -= 1
                        self.release()
                _check_deadline(_cleanup_cb)

            if self._value > 0:
                self._value -= 1
                return True

            # if we timed out, just return False instead of retrying
            if timeout and (time.time() - start_time) >= timeout:
                return False

    __enter__ = acquire

    def release(self):
        self._value += 1

        if self._waiters:
            waiting_gr = self._waiters.pop(0)

            # remove the timeout
            if waiting_gr in self._waiter_timeouts:
                timeout = self._waiter_timeouts.pop(waiting_gr)
                self._ioloop.remove_timeout(timeout)

            # schedule the waiting greenlet to try to acquire
            self._ioloop.add_callback(waiting_gr.switch)

    def __exit__(self, t, v, tb):
        self.release()

    @property
    def counter(self):
        return self._value


class GreenletBoundedSemaphore(GreenletSemaphore):
    """Semaphore that checks that # releases is <= # acquires"""
    def __init__(self, value=1):
        GreenletSemaphore.__init__(self, value)
        self._initial_value = value

    def release(self):
        if self._value >= self._initial_value:
            raise ValueError("Semaphore released too many times")
        return GreenletSemaphore.release(self)

class MemcachedConnection(object):

    def __init__(self, host, connect_timeout=5, net_timeout=5, io_loop=None):
        if host.find(":") > 0:
            self.ip, self.port = host.split(":")
            self.port = int(self.port)
        else:
            self.ip, self.port = host, 11211

        self.conn_timeout = connect_timeout
        self.net_timeout = net_timeout
        self.socket = None
        self.timeout = None
        self.timeout_handle = None
        self.io_loop = io_loop if io_loop else ioloop.IOLoop.current()

    def connect(self):
        if self._get_socket():
            return 1
        return 0

    def _get_socket(self):
        if self.socket:
            return self.socket

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        green_sock = GreenletSocket(sock, self.io_loop)

        green_sock.settimeout(self.conn_timeout)
        green_sock.connect((self.ip, self.port))
        green_sock.settimeout(self.net_timeout)

        self.socket = green_sock
        return green_sock

    def close(self):
        if self.socket:
            self.socket.close()
            self.socket = None

    def send_cmd(self, cmd, callback):
        self.socket.write(cmd+"\r\n")
        return callback()

    def readline(self, callback):
        resp = self.socket.read_until("\r\n")
        return callback(resp)

    def expect(self, text, callback):
        return self.readline(partial(self._expect_cb, text=text, callback=callback))

    def _expect_cb(self, data, text, callback):
        return callback(read_value=data)

    def recv(self, rlen, callback):
        resp = self.socket.recv(rlen)
        return callback(resp)

    def __str__(self):
        d = ''
        if self.deaduntil:
            d = " (dead until %d)" % self.deaduntil
        return "%s:%d%s" % (self.ip, self.port, d)
