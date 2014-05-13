<?php

class Eschrade_Queue_Model_QueueResponse
{
	protected $queueName;

	public function getQueueName()
	{
		return $this->queueName;
	}

	public function setQueueName($queueName)
	{
		$this->queueName = $queueName;
	}
	
}