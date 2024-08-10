<?php

namespace ReactphpX\Pool;

interface ConnectionPoolInterface
{
    public function getConnection();
    public function releaseConnection($connection);
}