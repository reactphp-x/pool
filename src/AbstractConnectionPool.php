<?php


namespace Reactphp\Framework\Pool;

use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\Promise\Deferred;
use function React\Promise\reject;
use function React\Promise\resolve;
use React\Promise\Timer\TimeoutException;

abstract class AbstractConnectionPool implements ConnectionPoolInterface
{

    protected $min_connections;
    protected $max_connections;

    protected $keep_alive;

    protected $max_wait_queue;
    protected $current_connections = 0;
    protected $wait_timeout = 0;
    protected $idle_connections = [];
    protected $wait_queue;
    protected $loop;
    protected $closed;


    public function __construct(
        $config = [],
        LoopInterface $loop = null,
    ) {
        $this->loop = $loop ?: Loop::get();
        $this->min_connections = $config['min_connections'] ?? 1;
        $this->max_connections = $config['max_connections'] ?? 10;
        $this->keep_alive = $config['keep_alive'] ?? 60;
        $this->max_wait_queue = $config['max_wait_queue'] ?? 100;
        $this->wait_timeout = $config['wait_timeout'] ?? 1;
        $this->wait_queue = new \SplObjectStorage;
        $this->idle_connections = new \SplObjectStorage;
    }


    public function getConnection()
    {
        if ($this->closed) {
            return reject(new Exception('pool is closed'));
        }

        if ($this->idle_connections->count() > 0) {
            $this->idle_connections->rewind();
            $connection = $this->idle_connections->current();
            if ($timer = $this->idle_connections[$connection]['timer']) {
                Loop::cancelTimer($timer);
            }
            if ($ping = $this->idle_connections[$connection]['ping']) {
                Loop::cancelTimer($ping);
                $ping = null;
            }
            $this->idle_connections->detach($connection);
            return resolve($connection);
        }

        if ($this->current_connections < $this->max_connections) {
            $this->current_connections++;
            return resolve($this->createConnection());
        }

        if ($this->max_wait_queue && $this->wait_queue->count() >= $this->max_wait_queue) {
            return reject(new Exception("over max_wait_queue: " . $this->max_wait_queue . '-current quueue:' . $this->wait_queue->count()));
        }

        $deferred = new Deferred();
        $this->wait_queue->attach($deferred);

        if (!$this->wait_timeout) {
            return $deferred->promise();
        }

        $that = $this;

        return \React\Promise\Timer\timeout($deferred->promise(), $this->wait_timeout, $this->loop)->then(null, function ($e) use ($that, $deferred) {

            $that->wait_queue->detach($deferred);

            if ($e instanceof TimeoutException) {
                throw new \RuntimeException(
                    'wait timed out after ' . $e->getTimeout() . ' seconds (ETIMEDOUT)' . 'and wait queue ' . $that->wait_queue->count() . ' count',
                    \defined('SOCKET_ETIMEDOUT') ? \SOCKET_ETIMEDOUT : 110
                );
            }
            throw $e;
        });
    }

    public function releaseConnection($connection)
    {
        if ($this->closed) {
            $this->_close($connection);
            $this->current_connections--;
            return;
        }

        if ($this->wait_queue->count() > 0) {
            $this->wait_queue->rewind();
            $deferred = $this->wait_queue->current();
            $deferred->resolve($connection);
            $this->wait_queue->detach($deferred);
            return;
        }


        $ping = null;
        $timer = Loop::addTimer($this->keep_alive, function () use ($connection, &$ping) {
            if ($this->idle_connections->count() > $this->min_connections) {
                $this->_quit($connection);
                $this->idle_connections->detach($connection);
                $this->current_connections--;
            } else {
                $ping = Loop::addPeriodicTimer($this->keep_alive, function () use ($connection, &$ping) {
                   $this->_ping($connection)->then(null, function($e) use ($ping){
                       if ($ping) {
                           Loop::cancelTimer($ping);
                       }
                       $ping = null;
                   });
                });
                $this->_ping($connection)->then(null, function($e) use ($ping){
                    if ($ping) {
                        Loop::cancelTimer($ping);
                    }
                    $ping = null;
                });

            }
        });

        $this->idle_connections->attach($connection, [
            'timer' => $timer,
            'ping' => &$ping
        ]);
    }

    public function close()
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;

        while ($this->idle_connections->count() > 0) {
            $this->idle_connections->rewind();
            $connection = $this->idle_connections->current();
            if ($timer = $this->idle_connections[$connection]['timer']) {
                Loop::cancelTimer($timer);
            }
            if ($ping = $this->idle_connections[$connection]['ping']) {
                Loop::cancelTimer($ping);
                $ping = null;
            }
            $this->idle_connections->detach($connection);
            $this->_close($connection);
            $this->current_connections--;
        }
    }


    protected function _ping($connection)
    {
        $that = $this;
        return $connection->ping()->then(function () use ($connection, $that) {
            if (!$that->idle_connections->contains($connection)) {
                $that->releaseConnection($connection);
            }
        }, function ($e) use ($connection, $that) {
            if ($that->idle_connections->contains($connection)) {
                $that->idle_connections->detach($connection);
            }
            $that->current_connections--;
            throw $e;
        });
    }

    public function getPoolCount()
    {
        return $this->current_connections;
    }

    public function getWaitCount()
    {
        return $this->wait_queue->count();
    }

    public function idleConnectionCount()
    {
        return $this->idle_connections->count();
    }

    protected function _quit($connection)
    {
        $connection->quit();
    }

    protected function _close($connection)
    {
        $connection->close();
    }

    abstract protected function createConnection();
}