<?php

namespace Reactphp\Framework\Pool;

interface ConnectionPoolInterface
{
    public function getConnection();
    public function releaseConnection($connection);
}