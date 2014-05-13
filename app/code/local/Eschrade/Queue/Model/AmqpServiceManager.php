<?php

use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Channel\AbstractChannel;
class AmqpEschrade_Queue_Model_ServiceManager
{
	const CONFIG_QUEUE_NAME = 'eschrade_queue/queue/queue_name'; 
	const CONFIG_QUEUE_HOST = 'eschrade_queue/queue/queue_host'; 
	const CONFIG_QUEUE_PORT = 'eschrade_queue/queue/queue_port'; 
	const CONFIG_QUEUE_USERNAME = 'eschrade_queue/queue/username'; 
	const CONFIG_QUEUE_PASSWORD = 'eschrade_queue/queue/password'; 
	const CONFIG_QUEUE_RESULT_QUEUE = 'eschrade_queue/queue/result_name'; 
	const CONFIG_QUEUE_ENDPOINT_URL = 'eschrade_queue/queue/endpoint_url'; 
	
	private $host;
	private $port;
	private $name;
	private $username;
	private $password;
	private $serviceManager;
	private $connection;
	private $channel;
	
	public function getConnection()
	{
		if (!$this->connection instanceof AMQPConnection) {
			$host = ($this->host === null)?Mage::getStoreConfig(self::CONFIG_QUEUE_HOST):$this->host;
			$port = ($this->port === null)?Mage::getStoreConfig(self::CONFIG_QUEUE_PORT):$this->port;
				
			$username = ($this->username === null)?Mage::getStoreConfig(self::CONFIG_QUEUE_USERNAME):$this->username;
			$password = ($this->password === null)?Mage::getStoreConfig(self::CONFIG_QUEUE_PASSWORD):$this->password;
				
			$errors = array();
			if (!$host) $errors[] = Mage::helper('eschrade_queue')->__('Missing queue host');
			if (!$port) $errors[] = Mage::helper('eschrade_queue')->__('Missing queue port');
			if (!$username) $errors[] = Mage::helper('eschrade_queue')->__('Missing queue username');
			if (!$password) $errors[] = Mage::helper('eschrade_queue')->__('Missing queue password');
				
			if ($errors)  {
				throw new Eschrade_Queue_Model_QueueConnectionException(implode("\n", $errors));
			}
				
			$this->connection = new AMQPConnection($host, $port, $username, $password);
			register_shutdown_function(array($this, 'shutdown'));
		}
		return $this->connection;	
	}
	
	public function shutdown()
	{
		if ($this->channel instanceof AbstractChannel) {
			$this->channel->close();
		}
		if ($this->connection instanceof AMQPConnection) {
			$this->connection->close();
		}

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
			if ($obj->getJobResponseObject() instanceof Eschrade_Queue_Model_QueueResponse) {
				error_log(get_class($obj) . ' pre ' . $obj->getJobResponseObject()->getQueueName());
			} else {
				error_log(get_class($obj) . ' has no queue');
			}
			$obj->execute();
			
			if ($obj->getJobResponseObject() instanceof Eschrade_Queue_Model_QueueResponse) {
				
				$message = new AMQPMessage($this->encodeMessageBody($obj));
				$queueName = $obj->getJobResponseObject()->getQueueName();
				$channel = $this->getChannel();
				$resultTopic = Mage::getStoreConfig(
					Eschrade_Queue_Model_ServiceManager::CONFIG_QUEUE_RESULT_QUEUE
				);
				$channel->queue_declare($queueName, false,false,false,false);
				$channel->queue_bind($queueName, $resultTopic, $queueName);
				$channel->basic_publish($message, $resultTopic, $queueName);
				error_log(get_class($obj) . ' pushed to ' . $queueName);
			}
		}
	}
	
	public function queue(Eschrade_Queue_Model_AbstractJob $job)
	{
		$channel = $this->getChannel();
		$responseQueue = null;
		if ($job->getJobRequiresResponse()) {
			$response  = new Eschrade_Queue_Model_QueueResponse();
			$responseQueue = uniqid();
			$response->setQueueName($responseQueue);
			$job->setJobResponseObject($response);
			error_log('Set to ' . $responseQueue);
		}
		error_log('Queued');
		$message = new AMQPMessage($this->encodeMessageBody($job), array(
			'content-type' => $responseQueue
		));
		$name = Mage::getStoreConfig(self::CONFIG_QUEUE_NAME);
		
		$channel->basic_publish($message, '', $name);
		
		// This will either be null or populated
		return $job->getJobResponseObject();
	}
	
	public function getJobResults(array $responses = array(), $poll = true)
	{
		$messages[] = array();
	
		if ($poll) {
			foreach ($responses as $response) {
				$channel = $this->getChannel();
				$message = $channel->basic_get($response->getQueueName());
				error_log('Read basic from ' . $response->getQueueName());
				$packet->delivery_info['channel']->queue_delete($response->getQueueName());
				error_log('Deleted ' . $packet->delivery_info['routing_key']);
				$messages[] = $message;
			}
		} else {
			
			$callback = function(AMQPMessage $packet) use (&$messages) {
				
				error_log('Read bulk from ' . $packet->delivery_info['routing_key'] . ' by ' . $packet->delivery_info['consumer_tag']);
				$messages[$packet->delivery_info['consumer_tag']] = $packet;
				$packet->delivery_info['channel']->basic_ack($packet->delivery_info['delivery_tag']);
				unset($packet->delivery_info['channel']->callbacks[$packet->delivery_info['consumer_tag']]);
				//$packet->delivery_info['channel']->queue_delete($packet->delivery_info['routing_key']);
// 				error_log('Deleted ' . $packet->delivery_info['routing_key']);
			};
			$resultTopic = Mage::getStoreConfig(
				Eschrade_Queue_Model_ServiceManager::CONFIG_QUEUE_RESULT_QUEUE
			);
			foreach ($responses as $response) {
				if (!$response instanceof Eschrade_Queue_Model_QueueResponse) continue;
				
				$channel = $this->getChannel();
				$consumerTag = uniqid();
// 				$channel->queue_declare($resultTopic$response->getQueueName());
				$channel->queue_declare($response->getQueueName(), false,false,false,false);
				//$channel->queue_bind($response->getQueueName(), $resultTopic);
				error_log('Bound to ' . $response->getQueueName() . ' with consumer tag ' . $consumerTag);
				$channel->basic_consume(
					$response->getQueueName(),
					$consumerTag,
					false, false, false, true,
					$callback
				);
				
			}
		
			while (count($channel->callbacks)) {
				try {
					$channel->wait();
				} catch (Exception $e) {
					error_log(Zend_Debug::dump($e->getTraceAsString()));
				}
				error_log('bulk wait ' . count($channel->callbacks));
				error_log(implode(',', array_keys($channel->callbacks)));
			}
			error_log('Got all messages (bulk) ' . count($responses));
		}
		$objects = array();
		foreach ($messages as $message) {
			$rawSerializedData = $message->body;
			if (strlen($rawSerializedData) == 0 ) continue;
			$obj = $this->decodeMessageBody($rawSerializedData);
			$objects[] = $obj;
		}
		return $objects;
	}
	
	public function getJobResult(Eschrade_Queue_Model_QueueResponse $response, $poll = true)
	{
		if (!$response instanceof Eschrade_Queue_Model_QueueResponse) return null;
		$channel = $this->getChannel();
		$message = null;
		if ($poll) {
			$channel->queue_declare($response->getQueueName(), false,false,false,false);
			$message = $channel->basic_get($response->getQueueName());	
			error_log('Read basic from ' . $response->getQueueName());
// 			$packet->delivery_info['channel']->queue_delete($response->getQueueName());
// 			error_log('Deleted ' . $packet->delivery_info['routing_key']);
				
		} else {

			$callback = function(AMQPMessage $packet) use (&$message) {
				$message = $packet;
				$packet->delivery_info['channel']->basic_ack($packet->delivery_info['delivery_tag']);
				unset($packet->delivery_info['channel']->callbacks[$packet->delivery_info['consumer_tag']]);
				error_log('Read simple from ' . $packet->delivery_info['routing_key'] . ' by ' . $packet->delivery_info['consumer_tag']);
				$packet->delivery_info['channel']->queue_delete($packet->delivery_info['routing_key']);
				error_log('Deleted ' . $packet->delivery_info['routing_key']);
				
			};
			$resultTopic = Mage::getStoreConfig(
				Eschrade_Queue_Model_ServiceManager::CONFIG_QUEUE_RESULT_QUEUE
			);
			
			
			$channel = $this->getChannel();
			$consumerTag = uniqid();
			$channel->queue_declare($response->getQueueName(), false,false,false,false);
			$channel->basic_consume(
				$response->getQueueName(),
				$consumerTag,
				false, false, false, true,
				$callback
			);
		
			while (count($channel->callbacks)) {
				$channel->wait();
				error_log('single wait ' . count($channel->callbacks));
			}
			error_log('Got all messages (single)');
		}
		if ($message) {
			$obj = $this->decodeMessageBody($message->body);
			return $obj;
		}
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
	
	/**
	 * 
	 * @param string $name
	 * @return \PhpAmqpLib\Channel\AMQPChannel
	 */
	
	public function getChannel()
	{
		if (!$this->channel instanceof AMQPChannel) {
			$resultTopic = Mage::getStoreConfig(
				Eschrade_Queue_Model_ServiceManager::CONFIG_QUEUE_RESULT_QUEUE
			);
			$name = Mage::getStoreConfig(self::CONFIG_QUEUE_NAME);
			$channel = $this->getConnection()->channel();
// 			$channel->exchange_delete($resultTopic);
// 			exit;
			$channel->exchange_declare($resultTopic, 'direct', false, false, false, false);
			$channel->queue_declare($name, false, true, false, false);
			$this->channel = $channel;
		}
		return $this->channel;
	}
	
}