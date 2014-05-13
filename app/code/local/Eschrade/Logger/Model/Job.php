<?php

class Eschrade_Logger_Model_Job extends Eschrade_Queue_Model_AbstractJob
{
	protected $url;
	
	public function setUrl($url)
	{
		$this->url = $url;
	}
	
	protected function job()
	{
		$logger = 'http://magento.loc/logger.php?' . base64_encode($this->url);
		file_get_contents($logger);
	}
}


