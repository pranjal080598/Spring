/*******************************************************************************
 * Copyright (C) 2019, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.jms;

/**
 * Parameter names for the JMS operators
 */
public class JMSOpConstants {

	public static final String PARAM_AUTH_APPCONFIGNAME						= "appConfigName";						//$NON-NLS-1$
	public static final String PARAM_AUTH_USERPROPNAME						= "userPropName";						//$NON-NLS-1$
	public static final String PARAM_AUTH_PWPROPNAME						= "passwordPropName";					//$NON-NLS-1$
	public static final String PARAM_AUTH_KEYSTORE							= "keyStore";							//$NON-NLS-1$
	public static final String PARAM_AUTH_KEYSTOREPW						= "keyStorePassword";					//$NON-NLS-1$
	public static final String PARAM_AUTH_TRUSTSTORE						= "trustStore";							//$NON-NLS-1$
	public static final String PARAM_AUTH_TRUSTSTOREPW						= "trustStorePassword";					//$NON-NLS-1$

	public static final String PARAM_CLASS_LIBS								= "classLibs";							//$NON-NLS-1$
	
	public static final String PARAM_JMS_HEADER_DESTINATION_O_ATTR_NAME		= "jmsDestinationOutAttributeName";		//$NON-NLS-1$
	public static final String PARAM_JMS_HEADER_DELIVERYMODE_O_ATTR_NAME	= "jmsDeliveryModeOutAttributeName";	//$NON-NLS-1$
	public static final String PARAM_JMS_HEADER_EXPIRATION_O_ATTR_NAME		= "jmsExpirationOutAttributeName";		//$NON-NLS-1$
	public static final String PARAM_JMS_HEADER_PRIORITY_O_ATTR_NAME		= "jmsPriorityOutAttributeName";		//$NON-NLS-1$
	public static final String PARAM_JMS_HEADER_MESSAGEID_O_ATTR_NAME		= "jmsMessageIDOutAttributeName";		//$NON-NLS-1$
	public static final String PARAM_JMS_HEADER_TIMESTAMP_O_ATTR_NAME		= "jmsTimestampOutAttributeName";		//$NON-NLS-1$
	public static final String PARAM_JMS_HEADER_CORRELATIONID_O_ATTR_NAME	= "jmsCorrelationIDOutAttributeName";	//$NON-NLS-1$
	public static final String PARAM_JMS_HEADER_REPLYTO_O_ATTR_NAME			= "jmsReplyToOutAttributeName";			//$NON-NLS-1$
	public static final String PARAM_JMS_HEADER_TYPE_O_ATTR_NAME			= "jmsTypeOutAttributeName";			//$NON-NLS-1$
	public static final String PARAM_JMS_HEADER_REDELIVERED_O_ATTR_NAME		= "jmsRedeliveredOutAttributeName";		//$NON-NLS-1$

	public static final String PARAM_JMS_HEADER_PROPS						= "jmsHeaderProperties";				//$NON-NLS-1$

	public static final String PARAM_JMS_HEADER_PROPS_O_ATTR_NAME			= "jmsHeaderPropertiesOutAttributeName";//$NON-NLS-1$
	
}
