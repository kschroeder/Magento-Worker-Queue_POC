<?php

class Eschrade_Queue_IndexController extends Mage_Core_Controller_Front_Action
{
	
	protected $_currentArea = 'queue';

	public function preDispatch()
	{
		$this->setFlag('index', self::FLAG_NO_START_SESSION, true);
		$this->setFlag('index', self::FLAG_NO_PRE_DISPATCH, true);
		parent::preDispatch();
	}
	
	public function postDispatch()
	{
		
	}
	
	public function indexAction()
	{

		$objectStr = file_get_contents('php://input');
		$manager = Mage::getSingleton('eschrade_queue/serviceManager');
		$manager->execute($objectStr);
	}
	
	
}