<?php

namespace ReactphpX\Pool;

interface ConnectionPoolInterface
{
    public function getConnection($prioritize = 0);
    public function releaseConnection($connection);
}