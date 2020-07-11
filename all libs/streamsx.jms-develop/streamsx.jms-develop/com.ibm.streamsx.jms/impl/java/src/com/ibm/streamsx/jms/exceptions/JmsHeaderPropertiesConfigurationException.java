package com.ibm.streamsx.jms.exceptions;

/**
 * This exception is thrown whenever problems in
 * the configuration for accessing JMS Header
 * property values are found.
 *
 */
public class JmsHeaderPropertiesConfigurationException extends RuntimeException {
	
	private static final long serialVersionUID = 1L;

	public JmsHeaderPropertiesConfigurationException(String message) {
		super(message);
	}
}
