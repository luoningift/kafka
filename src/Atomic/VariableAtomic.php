<?php
declare(strict_types=1);
/**
 * This file is part of Hyperf.
 *
 * @link     https://www.hyperf.io
 * @document https://doc.hyperf.io
 * @contact  group@hyperf.io
 * @license  https://github.com/hyperf/hyperf/blob/master/LICENSE
 */

namespace HKY\Kafka\Atomic;

class VariableAtomic
{
    private $atomic = 0;

    public function __construct()
    {
    }

    public function set(int $number)
    {
        $this->atomic = $number;
    }

    public function add(int $number)
    {
        $this->atomic += $number;
    }

    public function get()
    {
        return $this->atomic;
    }
}