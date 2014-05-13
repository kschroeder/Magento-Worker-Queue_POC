<?php

class Eschrade_Logger_Model_Logger
{
	
	public function log(Varien_Event_Observer $event)
	{
		if (!file_exists('/tmp/do.logger')) {
			return;
		}
		if (!file_exists('/tmp/queue.logger')) {
			$logger = 'http://magento.loc/logger.php?' . base64_encode(Mage::getUrl());
			file_get_contents($logger);
		} else {
			$logger = Mage::getModel('eschrade_logger/job');
			/* @var $logger Eschrade_Logger_Model_Job */
			$logger->setUrl(Mage::getUrl());
			$logger->run();
		}
	}
	
}