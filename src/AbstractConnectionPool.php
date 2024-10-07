<?php


namespace ReactphpX\Pool;

use React\EventLoop\Loop;
use React\Promise;
use React\Promise\Deferred;
use function React\Async\async;

abstract class AbstractConnectionPool implements ConnectionPoolInterface
{

    protected $pool;

    private int $currentConnections = 0;
    private $keepAliveTimer;

    private $closed = false;

    private $quitting = false;
    private $quitDeferred = null;

    private $queue = [];
    private $prioritizes = [];

    public function __construct(
        private string $uri,
        private int $minConnections = 2,
        private int $maxConnections = 10,
        private int $waitQueue = 100,
        private int $waitTimeout = 0,
    ) {
        $this->pool = new \SplObjectStorage();
    }

    public function getConnection($prioritize = 0)
    {

        if ($this->closed || $this->quitting) {
            return \React\Promise\reject(new Exception('Connection closed'));
        }

        if ($this->pool->count() > 0) {
            $this->pool->rewind();
            $connection = $this->pool->current();
            $this->pool->detach($connection);
            return \React\Promise\resolve($connection);
        }

        if ($this->currentConnections >= $this->maxConnections) {
            if ($this->waitQueue > 0 && count($this->queue) >= $this->waitQueue) {
                return Promise\reject(new \OverflowException('Max Queue reached'));
            }
            // get next queue position
            $queue = &$this->queue;
            $prioritizes = &$this->prioritizes;
            $queue[] = null;
            \end($queue);
            $id = \key($queue);
            $prioritizes[$id] = $prioritize;
            arsort($prioritizes);
            $deferred = new Deferred(function ($_, $reject) use (&$queue, &$prioritizes, $id) {
                unset($queue[$id]);
                unset($prioritizes[$id]);
                $reject(new \RuntimeException('Cancelled queued next handler'));
            });

            $queue[$id] = $deferred;

            if ($this->waitTimeout <= 0) {
                return $deferred->promise();
            }
            return \React\Promise\Timer\timeout($deferred->promise(), $this->waitTimeout)->then(null, function ($error) use (&$queue, &$prioritizes, $id) {
                unset($queue[$id]);
                unset($prioritizes[$id]);
                throw $error;
            });
        }

        return \React\Promise\resolve($this->createConnection());
    }

    public function releaseConnection($connection)
    {

        if ($this->closed) {
            $connection->close();
            return;
        }

        if ($this->queue) {
            reset($this->prioritizes);
            $id = key($this->prioritizes);
            $first = $this->queue[$id];
            unset($this->queue[$id]);
            unset($this->prioritizes[$id]);
            $first->resolve($connection);
            return;
        }

        $this->pool->attach($connection);
        if ($this->quitting && $this->pool->count() === $this->currentConnections) {
            if ($this->quitDeferred) {
                $this->quitDeferred->resolve(null);
            }
        }
    }

    public function close()
    {

        if ($this->closed) {
            return;
        }
        $this->closed = true;
        $this->quitting = false;
        $this->quitDeferred = null;
        if ($this->keepAliveTimer) {
            Loop::cancelTimer($this->keepAliveTimer);
            $this->keepAliveTimer = null;
        }

        if ($this->pool->count() > 0) {
            foreach ($this->pool as $connection) {
                $connection->close();
            }
        }

        if ($this->queue) {
            foreach ($this->queue as $id => $deferred) {
                $deferred->reject(new Exception('Connection closed'));
            }
        }

    }

    public function quit()
    {
        if ($this->closed || $this->quitting) {
            return \React\Promise\reject(new Exception('Connection closed'));
        }

        $this->quitting = true;
        if ($this->keepAliveTimer) {
            Loop::cancelTimer($this->keepAliveTimer);
            $this->keepAliveTimer = null;
        }

        if ($this->pool->count() === $this->currentConnections) {
            $this->close();
            return \React\Promise\resolve(null);
        }

        $deferred = new Deferred();
        $this->quitDeferred = $deferred;
        return $deferred->promise()->then(function () {
            $this->close();
        });
    }

    public function keepAlive($time = 30)
    {
        if ($this->closed || $this->quitting) {
            throw new Exception('Connection closed');
        }

        if ($this->keepAliveTimer) {
            Loop::cancelTimer($this->keepAliveTimer);
            $this->keepAliveTimer = null;
        }

        $this->keepAliveTimer = Loop::addPeriodicTimer($time, function () {
            async(function () {
                if ($this->closed || $this->quitting) {
                    return;
                }
                if ($this->minConnections <= 0) {
                    return;
                }
                if ($this->currentConnections - $this->pool->count() >= $this->minConnections) {
                    return;
                }
                $pingCount = $this->minConnections - ($this->currentConnections - $this->pool->count());
                $this->pool->rewind();
                $i = 0;
                foreach ($this->pool as $connection) {
                    $i++;
                    $this->getConnection()->then(function ($connection) {
                        return $connection->ping()->then(function ($result) use ($connection) {
                            $this->releaseConnection($connection);
                            return $result;
                        }, function ($error) use ($connection) {
                            $this->releaseConnection($connection);
                            throw $error;
                        });
                    });
                    if ($i >= $pingCount) {
                        break;
                    }
                }
            })();
        });
    }
 

    abstract protected function createConnection();
}