/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.jms.exceptions;


/**
 * This class is used for throwing exceptions
 * which comes during connection creation or
 * sending/receiving JMS Messages.
 *
 */
public class ConnectionException extends Exception {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ConnectionException(String message) {
		super(message);
	}
}
