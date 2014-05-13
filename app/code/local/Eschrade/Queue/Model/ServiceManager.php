<?php
use Predis\Client;
use Predis\Connection\ConnectionException;

class Eschrade_Queue_Model_ServiceManager
{
	const CONFIG_QUEUE_NAME = 'eschrade_queue/queue/queue_name'; 
	const CONFIG_QUEUE_HOST = 'eschrade_queue/queue/queue_host'; 
	const CONFIG_QUEUE_PORT = 'eschrade_queue/queue/queue_port'; 
	const CONFIG_QUEUE_RESULT_QUEUE = 'eschrade_queue/queue/result_name'; 
	const CONFIG_QUEUE_ENDPOINT_URL = 'eschrade_queue/queue/endpoint_url'; 
	
	private $host;
	private $port;
	private $name;
	private $serviceManager;
	private $connection;
	
	/**
	 * 
	 * @throws Eschrade_Queue_Model_QueueConnectionException
	 * @return \Predis\Client
	 */
	
	public function getConnection()
	{
		if (!$this->connection instanceof Client) {
			$host = Mage::getStoreConfig(self::CONFIG_QUEUE_HOST);
			$port = Mage::getStoreConfig(self::CONFIG_QUEUE_PORT);
			$worker = Mage::getStoreConfig(self::CONFIG_QUEUE_NAME);
				
			$errors = array();
			if (!$host) $errors[] = Mage::helper('eschrade_queue')->__('Missing queue host');
			if (!$port) $errors[] = Mage::helper('eschrade_queue')->__('Missing queue port');
			if (!$worker) $errors[] = Mage::helper('eschrade_queue')->__('Missing worker queue name');
				
			if ($errors)  {
				throw new Eschrade_Queue_Model_QueueConnectionException(implode("\n", $errors));
			}
				
			$this->connection = new Client(array(
			    'scheme' => 'tcp',
			    'host'   => $host,
			    'port'   => $port,
			));
		}
		return $this->connection;	
	}
	
	public function getHost() {
		return $this->host;
	}
	
	public function getPort() {
		return $this->port;
	}

	public function getName() {
		return $this->name;
	}

	public function getUsername() {
		return $this->username;
	}

	public function getPassword() {
		return $this->password;
	}

	public function setHost($host) {
		$this->host = $host;
	}

	public function setPort($port) {
		$this->port = $port;
	}

	public function setName($name) {
		$this->name = $name;
	}

	public function setUsername($username) {
		$this->username = $username;
	}

	public function setPassword($password) {
		$this->password = $password;
	}
	
	public function execute($rawSerializedData)
	{
		$obj = $this->decodeMessageBody($rawSerializedData);
		if ($obj instanceof Eschrade_Queue_Model_AbstractJob) {
			
			$obj->execute();
			if ($obj->getJobResponseObject() instanceof Eschrade_Queue_Model_QueueResponse) {
				
				$message = $this->encodeMessageBody($obj);
				$responseQueueName = $this->getResultQueueName($obj->getJobResponseObject());
				
				$this->getConnection()->lpush($responseQueueName, $message);
				
			}
		}
	}
	
	public function queue(Eschrade_Queue_Model_AbstractJob $job)
	{
		$connection = $this->getConnection();
		$responseQueue = null;
		if ($job->getJobRequiresResponse()) {
			$response  = Mage::getModel('eschrade_queue/queueResponse');
			$responseQueue = uniqid();
			$response->setQueueName($responseQueue);
			$job->setJobResponseObject($response);
		}
		
		$message = $this->encodeMessageBody($job);
		$name = Mage::getStoreConfig(self::CONFIG_QUEUE_NAME);
		$connection->lpush($name, $message);
		// This will either be null or populated
		return $job->getJobResponseObject();
	}
	
	public function getResultQueueName(Eschrade_Queue_Model_QueueResponse $response)
	{
		return Mage::getStoreConfig(self::CONFIG_QUEUE_RESULT_QUEUE) . '.' . $response->getQueueName();
	}
	
	public function getJobResults(array $responses = array(), $poll = true, $timeout = 60)
	{
		
		$messages = array();
		if ($poll) {
			foreach ($responses as $response) {
				$listNames = array();
				if ($response instanceof Eschrade_Queue_Model_QueueResponse) {	
					$listNames[] = $this->getResultQueueName($response);
				}
			}
			if (count($listNames) < 1) return array();
			try {
				$messages[] = call_user_func_array(array($this->getConnection(), 'lpop'), $listNames);
			} catch (ConnectionException $e) {} // errors include timeouts so we can't just throw an exception
		} else {
			$listNames = array();
			foreach ($responses as $response) {
				if ($response instanceof Eschrade_Queue_Model_QueueResponse) {
					$listNames[] = $this->getResultQueueName($response);
				}
				
			}
			if (count($listNames) < 1) return array();
			try {
				while (count($messages) !== count($listNames)) {
					$msg = $this->getConnection()->blpop($listNames, $timeout);
					$messages[] = $msg[1]; // First element is the queue name, second is the data
				}
			} catch (ConnectionException $e) {} // errors include timeouts so we can't just throw an exception
			
		}
		
		$objects = array();
		foreach ($messages as $message) {
			$obj = $this->decodeMessageBody($message);
			$objects[] = $obj;
		}
		return $objects;
	}
	
	public function getJobResult(Eschrade_Queue_Model_QueueResponse $response, $poll = true)
	{
		$objects = $this->getJobResults(array($response), $poll);
		return array_shift($objects);
	}
	
	protected function encodeMessageBody(Eschrade_Queue_Model_AbstractJob $job)
	{
		$objStr = serialize($job);
		// Done because apparently encrypt() doesn't like 0x00 values
		$objStr = base64_encode($objStr);
		$objStr = Mage::helper('core')->encrypt($objStr);
		$objStr = base64_encode($objStr);
		return $objStr;
	}
	
	protected function decodeMessageBody($rawSerializedData)
	{
		$objStr = base64_decode($rawSerializedData);
		$objStr = Mage::helper('core')->decrypt($objStr);
		// Because encrypt() does not like 0x00 characters
		$objStr = base64_decode($objStr);
		$obj = unserialize($objStr);
		return $obj;
	}

}