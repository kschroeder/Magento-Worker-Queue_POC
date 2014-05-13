<?php

//define('AMQP_DEBUG', true);

abstract class Eschrade_Queue_Model_AbstractJob
{
	protected $serviceManager;
	protected $jobRequiresResponse = false;
	protected $jobResponseObject;
	protected $elapsedJobExecutionTime;
	
	protected abstract function job();
	
	public function execute()
	{
		$startTime = microtime(true);
		$this->job();
		$this->setElapsedJobExecutionTime((microtime(true) - $startTime));
	}

	public function getElapsedJobExecutionTime() {
		return $this->elapsedJobExecutionTime;
	}

	public function setElapsedJobExecutionTime($elapsedJobExecutionTime) {
		$this->elapsedJobExecutionTime = $elapsedJobExecutionTime;
	}

	public function getJobRequiresResponse() {
		return $this->jobRequiresResponse;
	}

	public function setJobRequiresResponse($jobRequiresResponse) {
		$this->jobRequiresResponse = $jobRequiresResponse;
	}

	public function getJobResponseObject() {
		return $this->jobResponseObject;
	}

	public function setJobResponseObject(Eschrade_Queue_Model_QueueResponse $jobResponseObject) {
		$this->jobResponseObject = $jobResponseObject;
	}

	public function setServiceManager(Eschrade_Queue_Model_ServiceManager $manager)
	{
		$this->serviceManager = $manager;
	}
	
	public function run($requiresResponse = false)
	{
		$this->jobRequiresResponse = $requiresResponse;
		$manager = $this->serviceManager;
		if (!$manager instanceof Eschrade_Queue_Model_ServiceManager) {
			$manager = Mage::getSingleton('eschrade_queue/serviceManager');
		}
		$response = $manager->queue($this);
		return $response;
	}
}