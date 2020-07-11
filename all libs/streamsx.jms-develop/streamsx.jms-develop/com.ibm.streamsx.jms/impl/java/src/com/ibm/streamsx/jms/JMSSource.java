/*******************************************************************************
 * Copyright (C) 2013, 2019, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.jms;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.NamingException;
import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.LogLevel;
import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.model.CustomMetric;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.samples.patterns.ProcessTupleProducer;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streams.operator.state.StateHandler;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.jms.datagovernance.DataGovernanceUtil;
import com.ibm.streamsx.jms.datagovernance.IGovernanceConstants;
import com.ibm.streamsx.jms.exceptions.ConnectionException;
import com.ibm.streamsx.jms.exceptions.ParseConnectionDocumentException;
import com.ibm.streamsx.jms.helper.ConnectionDocumentParser;
import com.ibm.streamsx.jms.helper.JMSConnectionHelper;
import com.ibm.streamsx.jms.helper.JmsClasspathUtil;
import com.ibm.streamsx.jms.helper.JmsHeaderHelper;
import com.ibm.streamsx.jms.helper.PropertyAttributeType;
import com.ibm.streamsx.jms.helper.PropertyProvider;
import com.ibm.streamsx.jms.i18n.Messages;
import com.ibm.streamsx.jms.messagehandler.BytesMessageHandler;
import com.ibm.streamsx.jms.messagehandler.EmptyMessageHandler;
import com.ibm.streamsx.jms.messagehandler.JMSMessageHandlerImpl;
import com.ibm.streamsx.jms.messagehandler.MapMessageHandler;
import com.ibm.streamsx.jms.messagehandler.StreamMessageHandler;
import com.ibm.streamsx.jms.messagehandler.TextMessageHandler;
import com.ibm.streamsx.jms.types.MessageAction;
import com.ibm.streamsx.jms.types.MessageClass;
import com.ibm.streamsx.jms.types.PropertyType;
import com.ibm.streamsx.jms.types.ReconnectionPolicies;


@PrimitiveOperator(name = "JMSSource", namespace = "com.ibm.streamsx.jms", description = JMSSource.DESC)
@OutputPorts ({
		@OutputPortSet(	cardinality = 1,
						optional = false,
						windowPunctuationOutputMode = OutputPortSet.WindowPunctuationOutputMode.Free,
						description = "\\n" +  //$NON-NLS-1$
								"The `JMSSource` operator has one mandatory output and one optional output port.\\n" +  //$NON-NLS-1$
								"If only the mandatory output port is specified, the operator submits a tuple for each message\\n" +  //$NON-NLS-1$
								"that is successfully read from the messaging provider.\\n" +  //$NON-NLS-1$
								"The mandatory output port is mutating and its punctuation mode is Free."),  //$NON-NLS-1$
		@OutputPortSet(	cardinality = 1,
						optional = true,
						windowPunctuationOutputMode = OutputPortSet.WindowPunctuationOutputMode.Free,
						description = "\\n" +  //$NON-NLS-1$
								"The `JMSSource` operator has one optional output port, which submits tuples when an error occurs.\\n" +  //$NON-NLS-1$
								"\\n" +  //$NON-NLS-1$
								"If both optional and mandatory output ports are specified, the operator submits a tuple to the mandatory port\\n" +  //$NON-NLS-1$
								"for each message that is read successfully and a tuple to the optional port if an error occurs when reading a message.\\n" +  //$NON-NLS-1$
								"The tuple submitted to the optional port contains an error message for each message that could not be read successfully.\\n" +  //$NON-NLS-1$
								"The optional output port has a single attribute of type rstring that contains this error message.\\n" +  //$NON-NLS-1$
								"The optional output port is mutating and its punctuation mode is Free.")  //$NON-NLS-1$
})
@Icons(location16 = "icons/JMSSource_16.gif", location32 = "icons/JMSSource_32.gif")
@Libraries({"impl/lib/com.ibm.streamsx.jms.jar", "opt/downloaded/*"})
public class JMSSource extends ProcessTupleProducer implements StateHandler{	
	//The JMSSource operator converts a message JMS queue or topic to stream

	private static final String CLASS_NAME = JMSSource.class.getName();

	/**
	 * Create a {@code Logger} specific to this class that will write to the SPL
	 * log facility as a child of the {@link LoggerNames#LOG_FACILITY}
	 * {@code Logger}. The {@code Logger} uses a
	 */
	private static final Logger logger = Logger.getLogger(LoggerNames.LOG_FACILITY + "." + CLASS_NAME, "com.ibm.streamsx.jms.i18n.JMSMessages"); //$NON-NLS-1$ //$NON-NLS-2$
	private static final Logger tracer = Logger.getLogger(CLASS_NAME);
	
	// variable to hold the output port
	private StreamingOutput<OutputTuple> dataOutputPort;

	// Variables required by the optional error output port

	// hasErrorPort signifies if the operator has error port defined or not
	// assuming in the beginning that the operator does not have a error output
	// port by setting hasErrorPort to false
	// further down in the code, if the no of output ports is 2, we set to true
	// We send data to error ouput port only in case where hasErrorPort is set
	// to true which implies that the opeator instance has a error output port
	// defined.
	private boolean hasErrorPort = false;
	// Variable to specify error output port
	private StreamingOutput<OutputTuple> errorOutputPort;

	// Create an instance of class JMSConnectionhelper which is responsible for
	// creating and maintaining connection and receiving messages from JMS
	// Provider
	JMSConnectionHelper jmsConnectionHelper;

	// Variable to hold the message type as received in the connections
	// document.
	private MessageClass messageType;
	// Variable to specify if the JSMProvider is Apache Active MQ or WebSphere
	// MQ
	// set to true for Apache Active MQ.
	private boolean isAMQ;
	
	// consistent region context
    private ConsistentRegionContext consistentRegionContext;

	// Variables to hold performance metrices for JMSSource

	// nMessagesRead is the number of messages read successfully.
	// nMessagesDropped is the number of messages dropped.
	// nReconnectionAttempts is the number of reconnection attempts made before
	// a successful connection.

	Metric nMessagesRead;
	Metric nMessagesDropped;
	Metric nReconnectionAttempts;
	
	// when in consistent region, this parameter is used to indicate max time the receive method should block
	public static final long RECEIVE_TIMEOUT = 500l;
	
	// initialize the metrices.
	@CustomMetric(kind = Metric.Kind.COUNTER, description="The number of messages that are read successfully from a queue or topic.")
	public void setnMessagesRead(Metric nMessagesRead) {
		this.nMessagesRead = nMessagesRead;
	}

	@CustomMetric(kind = Metric.Kind.COUNTER, description="The number of messages that are dropped in the application.")
	public void setnMessagesDropped(Metric nMessagesDropped) {
		this.nMessagesDropped = nMessagesDropped;
	}

	@CustomMetric(kind = Metric.Kind.COUNTER, description="The number of reconnection attempts that are made before a successful connection occurs.")
	public void setnReconnectionAttempts(Metric nReconnectionAttempts) {
		this.nReconnectionAttempts = nReconnectionAttempts;
	}

	// operator parameters

	// This optional parameter codepage specifies the code page of the target
	// system using which ustring conversions have to be done for a BytesMessage
	// type.
	// If present, it must have exactly one value that is a String constant. If
	// the parameter is absent, the operator will use the default value of to
	// UTF-8
	private String codepage = "UTF-8"; //$NON-NLS-1$
	// This mandatory parameter access specifies access specification name.
	private String access;
	// This mandatory parameter connection specifies name of the connection
	// specification containing a JMS element
	private String connection;
	// This optional parameter connectionDocument specifies the pathname of a
	// file containing the connection information.
	// If present, it must have exactly one value that is a String constant.
	// If the parameter is absent, the operator will use the default location
	// file path etc/connections.xml (with respect to the application directory)
	private String connectionDocument;
	// This optional parameter reconnectionBound specifies the number of
	// successive connections that
	// will be attempted for this operator.
	// It is an optional parameter of type uint32.
	// It can appear only when the reconnectionPolicy parameter is set to
	// BoundedRetry and cannot appear otherwise.If not present the default value
	// is taken to be 5.
	private int reconnectionBound = 5;
	// This optional parameter reconnectionPolicy specifies the reconnection
	// policy that would be applicable during initial/intermittent connection
	// failures.
	// The valid values for this parameter are NoRetry, BoundedRetry and
	// InfiniteRetry.
	// If not specified, it is set to BoundedRetry with a reconnectionBound of 5
	// and a period of 60 seconds.
	private ReconnectionPolicies reconnectionPolicy = ReconnectionPolicies.valueOf("BoundedRetry"); //$NON-NLS-1$
	
	// This optional parameter period specifies the time period in seconds which
	// the operator will wait before trying to reconnect.
	// It is an optional parameter of type float64.
	// If not specified, the default value is 60.0. It must appear only when the
	// reconnectionPolicy parameter is specified
	private double period = 60.0;
	
	// Declaring the JMSMEssagehandler,
	private JMSMessageHandlerImpl messageHandlerImpl;
	
	// Specify after how many messages are received, the operator should establish consistent region
	private int triggerCount = 0;

	// instance of JMSSourceCRState to hold variables required for consistent region
	private JMSSourceCRState crState = null;
	
	private String messageSelector;
	
	private boolean initalConnectionEstablished = false;
	
	// Values to handle access to JMS Header values
	private String jmsDestinationOutAttributeName;
	private String jmsDeliveryModeOutAttributeName;
	private String jmsExpirationOutAttributeName;
	private String jmsPriorityOutAttributeName;
	private String jmsMessageIDOutAttributeName;
	private String jmsTimestampOutAttributeName;
	private String jmsCorrelationIDOutAttributeName;
	private String jmsReplyToOutAttributeName;
	private String jmsTypeOutAttributeName;
	private String jmsRedeliveredOutAttributeName;
	 
	private static List<String> jmsHeaderValOutAttributeNames = Arrays.asList(JMSOpConstants.PARAM_JMS_HEADER_DESTINATION_O_ATTR_NAME,
																		JMSOpConstants.PARAM_JMS_HEADER_DELIVERYMODE_O_ATTR_NAME,
																		JMSOpConstants.PARAM_JMS_HEADER_EXPIRATION_O_ATTR_NAME,
																		JMSOpConstants.PARAM_JMS_HEADER_PRIORITY_O_ATTR_NAME,
																		JMSOpConstants.PARAM_JMS_HEADER_MESSAGEID_O_ATTR_NAME,
																		JMSOpConstants.PARAM_JMS_HEADER_TIMESTAMP_O_ATTR_NAME, 
																		JMSOpConstants.PARAM_JMS_HEADER_CORRELATIONID_O_ATTR_NAME,
																		JMSOpConstants.PARAM_JMS_HEADER_REPLYTO_O_ATTR_NAME,
																		JMSOpConstants.PARAM_JMS_HEADER_TYPE_O_ATTR_NAME,
																		JMSOpConstants.PARAM_JMS_HEADER_REDELIVERED_O_ATTR_NAME);
	
	// Flag to signal if the operator accesses JMS Header values
	private boolean operatorAccessesJMSHeaderValues = false;

	
	// Values to handle access to JMS Header property values
	private List<String> jmsHeaderProperties;
	
	// Attribute name in output tuple that receives the map of received JMS Header Properties values
	private String jmsHeaderPropertiesOutAttributeName;

	// Index of attribute in output tuple that receives the map of received JMS Header Properties values
	private int jmsHeaderPropOutAttributeIndex = -1;

	// The broken down JMS Header property / attribute / type triplets
	private List<PropertyAttributeType> patTriplets = null;

	// Flag to signal if the operator accesses JMS Header property values
	private boolean operatorAccessesJMSHeaderPropertyValues = false;
	
	// Flag to signal if the operator accesses JMS Header property values generically (w/o property names)
	private boolean operatorGenericallyAccessesJMSHeaderPropertyValues = false;
	
	
	private Object resetLock = new Object();
	
	 // application configuration name
    private String appConfigName ;
    
    // user property name stored in application configuration
    private String userPropName;
    
    // password property name stored in application configuration
    private String passwordPropName;

    
    private boolean sslConnection = false;

    private boolean sslDebug = false;

    private String keyStore;
    
    private String trustStore;
    
    private String keyStorePassword;
    
    private String trustStorePassword;
    
    // List of class library paths to load
    private List<String> classLibs = null;

    
    
    @Parameter(optional = true, description = "This parameter specifies whether the operator should attempt to connect using SSL. If this parameter is specified, then the *keyStore*, *keyStorePassword* and *trustStore* parameters must also be specified. The default value is `false`.")
    public void setSslConnection(boolean sslConnection) {
		this.sslConnection = sslConnection;
	}

    public boolean isSslConnection() {
		return sslConnection;
	}

    @Parameter(optional=true, description="If SSL/TLS protocol debugging is enabled, all protocol data and information is logged to the console. Use this to debug TLS connection problems. The default is 'false'. ")  //$NON-NLS-1$
	public void setSslDebug(boolean sslDebug) {
		this.sslDebug = sslDebug;
	}

	public boolean isSslDebug() {
		return sslDebug;
	}

    public String getTrustStorePassword() {
		return trustStorePassword;
	}
    
    @Parameter(optional = true, description = "This parameter specifies the password for the trustStore given by the *trustStore* parameter. The *sslConnection* parameter must be set to `true` for this parameter to have any effect.")
    public void setTrustStorePassword(String trustStorePassword) {
		this.trustStorePassword = trustStorePassword;
	}
    
    public String getTrustStore() {
		return trustStore;
	}
    
    @Parameter(optional = true, description = "This parameter specifies the path to the trustStore. If a relative path is specified, the path is relative to the application directory. The *sslConnection* parameter must be set to true for this parameter to have any effect.")
    public void setTrustStore(String trustStore) {
		this.trustStore = trustStore;
	}
    
    public String getKeyStorePassword() {
		return keyStorePassword;
	}
    
    @Parameter(optional = true, description = "This parameter specifies the password for the keyStore given by the *keyStore* parameter. The *sslConnection* parameter must be set to `true` for this parameter to have any effect.")
    public void setKeyStorePassword(String keyStorePassword) {
		this.keyStorePassword = keyStorePassword;
	}
    
    public String getKeyStore() {
		return keyStore;
	}
    
    @Parameter(optional = true, description = "This parameter specifies the path to the keyStore. If a relative path is specified, the path is relative to the application directory. The *sslConnection* parameter must be set to true for this parameter to have any effect.")
    public void setKeyStore(String keyStore) {
		this.keyStore = keyStore;
	}    

	public String getAppConfigName() {
		return appConfigName;
	}
    
	@Parameter(optional = true, description = "This parameter specifies the name of application configuration that stores client credential information, the credential specified via application configuration overrides the one specified in connections file.")
	public void setAppConfigName(String appConfigName) {
		this.appConfigName = appConfigName;
	}

	public String getUserPropName() {
		return userPropName;
	}
    
	@Parameter(optional = true, description = "This parameter specifies the property name of user name in the application configuration. If the appConfigName parameter is specified and the userPropName parameter is not set, a compile time error occurs.")
	public void setUserPropName(String userPropName) {
		this.userPropName = userPropName;
	}

	public String getPasswordPropName() {
		return passwordPropName;
	}
    
	@Parameter(optional = true, description = "This parameter specifies the property name of password in the application configuration. If the appConfigName parameter is specified and the passwordPropName parameter is not set, a compile time error occurs.")
	public void setPasswordPropName(String passwordPropName) {
		this.passwordPropName = passwordPropName;
	}

    public List<String> getClassLibs() {
		return classLibs;
	}
    
	@Parameter(optional = true, description = JMSOpDescriptions.CLASS_LIBS)  //$NON-NLS-1$
    public void setClassLibs(List<String> classLibs) {
		this.classLibs = classLibs;
	}
    
	public String getJmsDestinationOutAttributeName() {
		return jmsDestinationOutAttributeName;
	}

	@Parameter(optional = true, description = "Output attribute on output data stream to assign JMSDestination to, the specified attribute in output stream must be of type rstring." )
	public void setJmsDestinationOutAttributeName(String jmsDestinationOutAttributeName) {
		this.jmsDestinationOutAttributeName = jmsDestinationOutAttributeName;
	}

	public String getJmsDeliveryModeOutAttributeName() {
		return jmsDeliveryModeOutAttributeName;
	}

	@Parameter(optional = true, description = "Output attribute on output data stream to assign JMSDeliveryMode to, the specified attribute in output stream must be of type int32." )
	public void setJmsDeliveryModeOutAttributeName(String jmsDeliveryModeOutAttributeName) {
		this.jmsDeliveryModeOutAttributeName = jmsDeliveryModeOutAttributeName;
	}

	public String getJmsExpirationOutAttributeName() {
		return jmsExpirationOutAttributeName;
	}

	@Parameter(optional = true, description = "Output attribute on output data stream to assign JMSExpiration to, the specified attribute in output stream must be of type int64." )
	public void setJmsExpirationOutAttributeName(String jmsExpirationOutAttributeName) {
		this.jmsExpirationOutAttributeName = jmsExpirationOutAttributeName;
	}

	public String getJmsPriorityOutAttributeName() {
		return jmsPriorityOutAttributeName;
	}

	@Parameter(optional = true, description = "Output attribute on output data stream to assign JMSPriority to, the specified attribute in output stream must be of type int32." )
	public void setJmsPriorityOutAttributeName(String jmsPriorityOutAttributeName) {
		this.jmsPriorityOutAttributeName = jmsPriorityOutAttributeName;
	}

	public String getJmsMessageIDOutAttributeName() {
		return jmsMessageIDOutAttributeName;
	}

	@Parameter(optional = true, description = "Output attribute on output data stream to assign JMSMessageID to, the specified attribute in output stream must be of type rstring.")
	public void setJmsMessageIDOutAttributeName(String jmsMessageIDOutAttributeName) {
		this.jmsMessageIDOutAttributeName = jmsMessageIDOutAttributeName;
	}

	public String getJmsTimestampOutAttributeName() {
		return jmsTimestampOutAttributeName;
	}

	@Parameter(optional = true, description = "Output attribute on output data stream to assign JMSTimestamp to, the specified attribute in output stream must be of type int64." )
	public void setJmsTimestampOutAttributeName(String jmsTimestampOutAttributeName) {
		this.jmsTimestampOutAttributeName = jmsTimestampOutAttributeName;
	}

	public String getJmsCorrelationIDOutAttributeName() {
		return jmsCorrelationIDOutAttributeName;
	}

	@Parameter(optional = true, description = "Output attribute on output data stream to assign JMSCorrelationID to, the specified attribute in output stream must be of type rstring." )
	public void setJmsCorrelationIDOutAttributeName(String jmsCorrelationIDOutAttributeName) {
		this.jmsCorrelationIDOutAttributeName = jmsCorrelationIDOutAttributeName;
	}

	public String getJmsReplyToOutAttributeName() {
		return jmsReplyToOutAttributeName;
	}

	@Parameter(optional = true, description = "Output attribute on output data stream to assign JMSReplyTo to, the specified attribute in output stream must be of type rstring." )
	public void setJmsReplyToOutAttributeName(String jmsReplyToOutAttributeName) {
		this.jmsReplyToOutAttributeName = jmsReplyToOutAttributeName;
	}

	public String getJmsTypeOutAttributeName() {
		return jmsTypeOutAttributeName;
	}

	@Parameter(optional = true, description = "Output attribute on output data stream to assign JMSType to, the specified attribute in output stream must be of type rstring." )	//$NON-NLS-1$
	public void setJmsTypeOutAttributeName(String jmsTypeOutAttributeName) {
		this.jmsTypeOutAttributeName = jmsTypeOutAttributeName;
	}

	public String getJmsRedeliveredOutAttributeName() {
		return jmsRedeliveredOutAttributeName;
	}

	@Parameter(optional = true, description = "Output attribute on output data stream to assign JMSRedelivered to, the specified attribute in output stream must be of type boolean." )	//$NON-NLS-1$
	public void setJmsRedeliveredOutAttributeName(String jmsRedeliveredOutAttributeName) {
		this.jmsRedeliveredOutAttributeName = jmsRedeliveredOutAttributeName;
	}

    public List<String> getJmsHeaderProperties() {
        return jmsHeaderProperties;
    }

    @Parameter(optional = true, description = JMSOpDescriptions.JMS_HEADER_PROPERTIES_DESC)
    public void setJmsHeaderProperties(List<String> jmsHeaderProperties) {
        this.jmsHeaderProperties = jmsHeaderProperties;
    }
    
    public String getJmsHeaderPropertiesOutAttributeName() {
        return jmsHeaderPropertiesOutAttributeName;
    }

    @Parameter(optional = true, description = JMSOpDescriptions.JMS_HEADER_PROPERTIES_O_ATTR_NAME_DESC)
    public void setJmsHeaderPropertiesOutAttributeName(String jmsHeaderPropertiesOutAttributeName) {
        this.jmsHeaderPropertiesOutAttributeName = jmsHeaderPropertiesOutAttributeName;
    }
    
	public String getMessageSelector() {
		return messageSelector;
	}

	@Parameter(optional = true, description = "This optional parameter is used as JMS Message Selector.")	//$NON-NLS-1$
	public void setMessageSelector(String messageSelector) {
		this.messageSelector = messageSelector;
	}

	public int getTriggerCount() {
		return triggerCount;
	}

	@Parameter(optional = true, description = "\\n" +  //$NON-NLS-1$
			"This optional parameter specifies how many messages are submitted before the JMSSource operator starts to drain the pipeline and establish a consistent state.\\n" +  //$NON-NLS-1$
			"This parameter must be greater than zero and must be set if the JMSSource operator is the start operator of an operatorDriven consistent region.")  //$NON-NLS-1$
	public void setTriggerCount(int triggerCount) {
		this.triggerCount = triggerCount;
	}

	@Parameter(optional = false, description = "This mandatory parameter identifies the access specification name.")	//$NON-NLS-1$
	public void setAccess(String access) {
		this.access = access;
	}

	@Parameter(optional = false, description = "This mandatory parameter identifies the name of the connection specification that contains an JMS element.")
	public void setConnection(String connection) {
		this.connection = connection;
	}

	@Parameter(optional = true, description = "\\n" +  //$NON-NLS-1$ 
			"This optional parameter specifies the code page of the target system that is used to convert ustring for a Bytes message type.\\n" +  //$NON-NLS-1$
			"If this parameter is specified, it must have exactly one value, which is a String constant.\\n" +  //$NON-NLS-1$
			"If the parameter is not specified, the operator uses the default value of UTF8.\\n")  //$NON-NLS-1$
	public void setCodepage(String codepage) {
		this.codepage = codepage;
	}

	@Parameter(optional = true, description = "\\n" +  //$NON-NLS-1$
			"This is an optional parameter that specifies the reconnection policy.\\n" +  //$NON-NLS-1$
			"The valid values are `NoRetry`, `InfiniteRetry`, and `BoundedRetry`.\\n" +  //$NON-NLS-1$
			"If the parameter is not specified, the reconnection policy is set to `BoundedRetry` with a **reconnectionBound** of `5`\\n" +  //$NON-NLS-1$
			"and a **period** of 60 seconds.\\n")  //$NON-NLS-1$
	public void setReconnectionPolicy(String reconnectionPolicy) {
		this.reconnectionPolicy = ReconnectionPolicies.valueOf(reconnectionPolicy);
	}

	@Parameter(optional = true, description = "\\n" +  //$NON-NLS-1$
			"This optional parameter specifies the number of successive connections that are attempted for an operator.\\n" +  //$NON-NLS-1$
			"You can use this parameter only when the **reconnectionPolicy** parameter is specified and set to `BoundedRetry`,\\n" +  //$NON-NLS-1$
			"otherwise a run time error occurs. If the **reconnectionBound** parameter is specified\\n" +  //$NON-NLS-1$
			"and the **reconnectionPolicy** parameter is not set, a compile time error occurs.\\n" +  //$NON-NLS-1$
			"The default value for the **reconnectionBound** parameter is `5`.\\n")  //$NON-NLS-1$
	public void setReconnectionBound(int reconnectionBound) {
		this.reconnectionBound = reconnectionBound;
	}

	@Parameter(optional = true, description = "\\n" +  //$NON-NLS-1$
			"This optional parameter specifies the time period in seconds the operator waits before it tries to reconnect.\\n" +  //$NON-NLS-1$
			"You can use this parameter only when the **reconnectionPolicy** parameter is specified,\\n" +  //$NON-NLS-1$
			"otherwise a compile time error occurs. The default value for the **period** parameter is `60`.\\n")  //$NON-NLS-1$
	public void setPeriod(double period) {
		this.period = period;
	}

	@Parameter(optional = true, description="\\n" +  //$NON-NLS-1$
			"This optional parameter specifies the path name of the file that contains the connection and access specifications,\\n" +  //$NON-NLS-1$
			"which are identified by the **connection** and **access** parameters.\\n" +  //$NON-NLS-1$
			"If this parameter is not specified, the operator uses the file that is in the default location `./etc/connections.xml`.")  //$NON-NLS-1$
	public void setConnectionDocument(String connectionDocument) {
		this.connectionDocument = connectionDocument;
	}
	
	// Class to hold various variables for consistent region
	private class JMSSourceCRState {
		
		// Last fully processed message
		private Message lastMsgSent;
		
		// Counters for counting number of received messages
		private int msgCounter;
		
		// Flag to indicate a checkpoint has been made.
		private boolean isCheckpointPerformed;
		
		private List<String> msgIDWIthSameTS;
		
		JMSSourceCRState() {
			lastMsgSent = null;
			msgCounter = 0;
			isCheckpointPerformed = false;
			msgIDWIthSameTS = new ArrayList<String>();
		}

		public List<String> getMsgIDWIthSameTS() {
			return msgIDWIthSameTS;
		}

		public boolean isCheckpointPerformed() {
			return isCheckpointPerformed;
		}

		public void setCheckpointPerformed(boolean isCheckpointPerformed) {
			this.isCheckpointPerformed = isCheckpointPerformed;
		}

		public void increaseMsgCounterByOne() {
			this.msgCounter++;
		}

		public Message getLastMsgSent() {
			return lastMsgSent;
		}

		public void setLastMsgSent(Message lastMsgSent) {
			
			try {
				if(this.lastMsgSent != null && this.lastMsgSent.getJMSTimestamp() != lastMsgSent.getJMSTimestamp()) {
					this.msgIDWIthSameTS.clear();
				}
				
				this.msgIDWIthSameTS.add(lastMsgSent.getJMSMessageID());
			} catch (JMSException e) {
				
			}		
			this.lastMsgSent = lastMsgSent;	
			
		}

		public int getMsgCounter() {
			return msgCounter;
		}
		
		public void acknowledgeMsg() throws JMSException {
			if(lastMsgSent != null) {
				lastMsgSent.acknowledge();
			}
		}
		
		public void reset() {
			lastMsgSent = null;
			msgCounter = 0;
			isCheckpointPerformed = false;
			msgIDWIthSameTS = new ArrayList<String>();
		}
		
	}
	
	public String getConnectionDocument() {
		
		if (connectionDocument == null)
		{
			connectionDocument = getOperatorContext().getPE().getApplicationDirectory() + "/etc/connections.xml"; //$NON-NLS-1$
		}
		
		// if relative path, convert to absolute path
		if (!connectionDocument.startsWith("/")) //$NON-NLS-1$
		{
			connectionDocument = getOperatorContext().getPE().getApplicationDirectory() + File.separator + connectionDocument;
		}
		
		return connectionDocument;
	}

	// Add the context checks

	@ContextCheck(compile = true)
	public static void checkInConsistentRegion(OperatorContextChecker checker) {
		ConsistentRegionContext consistentRegionContext = checker.getOperatorContext().getOptionalContext(ConsistentRegionContext.class);
		OperatorContext context = checker.getOperatorContext();
		
		if(consistentRegionContext != null && consistentRegionContext.isTriggerOperator() && !context.getParameterNames().contains("triggerCount")) { //$NON-NLS-1$
			checker.setInvalidContext(Messages.getString("TRIGGERCOUNT_PARAM_MUST_BE_SET_WHEN_CONSISTENT_REGION_IS_OPERATOR_DRIVEN"), new String[] {}); //$NON-NLS-1$
		}
	}
	
	/*
	 * The method checkErrorOutputPort validates that the stream on error output
	 * port contains the mandatory attribute of type rstring which will contain
	 * the error message.
	 */
	@ContextCheck
	public static void checkErrorOutputPort(OperatorContextChecker checker) {
		OperatorContext context = checker.getOperatorContext();
		// Check if the error port is defined or not
		if (context.getNumberOfStreamingOutputs() == 2) {
			StreamingOutput<OutputTuple> streamingOutputErrorPort = context
					.getStreamingOutputs().get(1);
			// The optional error output port can have only one attribute of
			// type rstring
			if (streamingOutputErrorPort.getStreamSchema().getAttributeCount() != 1) {
				logger.log(LogLevel.ERROR, "ERROR_PORT_ATTR_COUNT_MAX"); //$NON-NLS-1$
			}
			if (streamingOutputErrorPort.getStreamSchema().getAttribute(0)
					.getType().getMetaType() != Type.MetaType.RSTRING) {
				logger.log(LogLevel.ERROR, "ERROR_PORT_ATTR_TYPE"); //$NON-NLS-1$
			}

		}
	}

	/*
	 * The method checkParametersRuntime validates that the reconnection policy
	 * parameters are appropriate
	 */
	@ContextCheck(compile = false)
	public static void checkParametersRuntime(OperatorContextChecker checker) {
		
		tracer.log(TraceLevel.TRACE, "Begin checkParametersRuntime()"); //$NON-NLS-1$

		OperatorContext context = checker.getOperatorContext();

		if ((context.getParameterNames().contains("reconnectionBound"))) { // reconnectionBound //$NON-NLS-1$
			// reconnectionBound value should be non negative.
			if (Integer.parseInt(context
					.getParameterValues("reconnectionBound").get(0)) < 0) { //$NON-NLS-1$

				logger.log(LogLevel.ERROR, "REC_BOUND_NEG"); //$NON-NLS-1$
				checker.setInvalidContext(
						Messages.getString("PARAM_VALUE_SHOULD_BE_GREATER_OR_EQUAL_TO_ZERO"),	//$NON-NLS-1$
						new String[] { "reconnectionBound", context.getParameterValues("reconnectionBound").get(0) }); //$NON-NLS-1$
			}
			if (context.getParameterNames().contains("reconnectionPolicy")) { //$NON-NLS-1$
				// reconnectionPolicy can be either InfiniteRetry, NoRetry,
				// BoundedRetry
				ReconnectionPolicies reconPolicy = ReconnectionPolicies
						.valueOf(context
								.getParameterValues("reconnectionPolicy") //$NON-NLS-1$
								.get(0).trim());
				// reconnectionBound can appear only when the reconnectionPolicy
				// parameter is set to BoundedRetry and cannot appear otherwise
				if (reconPolicy != ReconnectionPolicies.BoundedRetry) {
					logger.log(LogLevel.ERROR, "REC_BOUND_NOT_ALLOWED");	//$NON-NLS-1$
					checker.setInvalidContext(
							Messages.getString("RECONNECTIONBOUND_CAN_APPEAR_ONLY_WHEN_RECONNECTIONPOLICY_IS_BOUNDEDRETRY"), //$NON-NLS-1$
							new String[] { context.getParameterValues("reconnectionBound").get(0) }); //$NON-NLS-1$

				}
			}
		}
		// If initDelay parameter os specified then its value should be non
		// negative.
		if (context.getParameterNames().contains("initDelay")) { //$NON-NLS-1$
			if (Integer.valueOf(context.getParameterValues("initDelay").get(0)) < 0) { //$NON-NLS-1$
				logger.log(LogLevel.ERROR, "INIT_DELAY_NEG"); //$NON-NLS-1$
				checker.setInvalidContext(
						Messages.getString("PARAM_VALUE_SHOULD_BE_GREATER_OR_EQUAL_TO_ZERO"), //$NON-NLS-1$
						new String[] { "initDelay", context.getParameterValues("initDelay").get(0).trim() }); //$NON-NLS-1$
			}
		}
		
		if(context.getParameterNames().contains("triggerCount")) { //$NON-NLS-1$
			if(Integer.valueOf(context.getParameterValues("triggerCount").get(0)) < 1) { //$NON-NLS-1$
				logger.log(LogLevel.ERROR, "TRIGGERCOUNT_VALUE_SHOULD_BE_GREATER_THAN_ZERO", new String[] { context.getParameterValues("triggerCount").get(0).trim() } ); //$NON-NLS-1$ //$NON-NLS-2$
				checker.setInvalidContext(
						Messages.getString("TRIGGERCOUNT_VALUE_SHOULD_BE_GREATER_THAN_ZERO"), //$NON-NLS-1$
						new String[] { context.getParameterValues("triggerCount").get(0).trim() }); //$NON-NLS-1$
			}
		}
		
		for(String jmsHeaderValOutAttributeName : jmsHeaderValOutAttributeNames ) {
	        if (checker.getOperatorContext().getParameterNames().contains(jmsHeaderValOutAttributeName)) { 
	    		
	    		List<String> parameterValues = checker.getOperatorContext().getParameterValues(jmsHeaderValOutAttributeName); 
	    		String outAttributeName = parameterValues.get(0);
		    	List<StreamingOutput<OutputTuple>> outputPorts = checker.getOperatorContext().getStreamingOutputs();
		    	if (outputPorts.size() > 0)
		    	{
		    		StreamingOutput<OutputTuple> outputPort = outputPorts.get(0);
		    		StreamSchema streamSchema = outputPort.getStreamSchema();
		    		boolean check = checker.checkRequiredAttributes(streamSchema, outAttributeName);
		    		if (check) {
		    			switch (jmsHeaderValOutAttributeName) {
		    			case JMSOpConstants.PARAM_JMS_HEADER_DELIVERYMODE_O_ATTR_NAME:
		    			case JMSOpConstants.PARAM_JMS_HEADER_PRIORITY_O_ATTR_NAME:
		    				checker.checkAttributeType(streamSchema.getAttribute(outAttributeName), MetaType.INT32);
		    				break;
		    			case JMSOpConstants.PARAM_JMS_HEADER_EXPIRATION_O_ATTR_NAME:
		    			case JMSOpConstants.PARAM_JMS_HEADER_TIMESTAMP_O_ATTR_NAME:
		    				checker.checkAttributeType(streamSchema.getAttribute(outAttributeName), MetaType.INT64);
		    				break;
		    			case JMSOpConstants.PARAM_JMS_HEADER_REDELIVERED_O_ATTR_NAME:
		    				checker.checkAttributeType(streamSchema.getAttribute(outAttributeName), MetaType.BOOLEAN);
		    				break;
		    			default:
		    				checker.checkAttributeType(streamSchema.getAttribute(outAttributeName), MetaType.RSTRING);
		    			}
		    		}
		    	}
	    	}
		}
		
		
        if (checker.getOperatorContext().getParameterNames().contains(JMSOpConstants.PARAM_JMS_HEADER_PROPS_O_ATTR_NAME)) { 
    		
    		List<String> parameterValues = checker.getOperatorContext().getParameterValues(JMSOpConstants.PARAM_JMS_HEADER_PROPS_O_ATTR_NAME); 
    		String outAttributeName = parameterValues.get(0);

    		List<StreamingOutput<OutputTuple>> outputPorts = checker.getOperatorContext().getStreamingOutputs();
	    	if (outputPorts.size() > 0)
	    	{
	    		StreamingOutput<OutputTuple> outputPort = outputPorts.get(0);
	    		StreamSchema streamSchema = outputPort.getStreamSchema();
	    		boolean check = checker.checkRequiredAttributes(streamSchema, outAttributeName);
	    		if (check) {
	    			checker.checkAttributeType(streamSchema.getAttribute(outAttributeName), MetaType.MAP);
	    		}
	    	}
    	}


        if((checker.getOperatorContext().getParameterNames().contains(JMSOpConstants.PARAM_AUTH_APPCONFIGNAME))) { //$NON-NLS-1$
        	String appConfigName = checker.getOperatorContext().getParameterValues(JMSOpConstants.PARAM_AUTH_APPCONFIGNAME).get(0); //$NON-NLS-1$
			String userPropName = checker.getOperatorContext().getParameterValues(JMSOpConstants.PARAM_AUTH_USERPROPNAME).get(0); //$NON-NLS-1$
			String passwordPropName = checker.getOperatorContext().getParameterValues(JMSOpConstants.PARAM_AUTH_PWPROPNAME).get(0); //$NON-NLS-1$
			
			
			PropertyProvider provider = new PropertyProvider(checker.getOperatorContext().getPE(), appConfigName);
			
			String userName = provider.getProperty(userPropName, false);
			String password = provider.getProperty(passwordPropName, false);
			
			if(userName == null || userName.trim().length() == 0) {
				logger.log(LogLevel.ERROR, "PROPERTY_NOT_FOUND_IN_APP_CONFIG", new String[] {userPropName, appConfigName}); //$NON-NLS-1$
				checker.setInvalidContext(
						Messages.getString("PROPERTY_NOT_FOUND_IN_APP_CONFIG"), //$NON-NLS-1$
						new Object[] {userPropName, appConfigName});
			}
			
			if(password == null || password.trim().length() == 0) {
				logger.log(LogLevel.ERROR, "PROPERTY_NOT_FOUND_IN_APP_CONFIG", new String[] {passwordPropName, appConfigName}); //$NON-NLS-1$
				checker.setInvalidContext(Messages.getString("PROPERTY_NOT_FOUND_IN_APP_CONFIG"), //$NON-NLS-1$
																new Object[] {passwordPropName, appConfigName});
			}
        }
        
        tracer.log(TraceLevel.TRACE, "End checkParametersRuntime()"); //$NON-NLS-1$		
	}

	// add check for reconnectionPolicy is present if either period or
	// reconnectionBound is specified
	@ContextCheck(compile = true)
	public static void checkParameters(OperatorContextChecker checker) {
		checker.checkDependentParameters("period", "reconnectionPolicy"); //$NON-NLS-1$ //$NON-NLS-2$
		checker.checkDependentParameters("reconnectionBound", "reconnectionPolicy"); //$NON-NLS-1$ //$NON-NLS-2$
		
		// Make sure if appConfigName is specified then both userPropName and passwordPropName are needed
		checker.checkDependentParameters(JMSOpConstants.PARAM_AUTH_APPCONFIGNAME, JMSOpConstants.PARAM_AUTH_USERPROPNAME, JMSOpConstants.PARAM_AUTH_PWPROPNAME); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		checker.checkDependentParameters(JMSOpConstants.PARAM_AUTH_USERPROPNAME, JMSOpConstants.PARAM_AUTH_APPCONFIGNAME, JMSOpConstants.PARAM_AUTH_PWPROPNAME); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		checker.checkDependentParameters(JMSOpConstants.PARAM_AUTH_PWPROPNAME, JMSOpConstants.PARAM_AUTH_APPCONFIGNAME, JMSOpConstants.PARAM_AUTH_USERPROPNAME); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	@Override
	public synchronized void initialize(OperatorContext context)
			throws ParserConfigurationException, InterruptedException,
			IOException, ParseConnectionDocumentException, SAXException,
			NamingException, ConnectionException, Exception {
		
		

		tracer.log(TraceLevel.TRACE, "Begin initialize()"); //$NON-NLS-1$
		
		tracer.log(TraceLevel.TRACE, "Calling super class initialization"); //$NON-NLS-1$
		super.initialize(context);
		tracer.log(TraceLevel.TRACE, "Returned from super class initialization"); //$NON-NLS-1$
		
		consistentRegionContext = context.getOptionalContext(ConsistentRegionContext.class);
		
		JmsClasspathUtil.setupClassPaths(context);
		
		// set SSL system properties
		if(isSslConnection()) {
			
			tracer.log(TraceLevel.TRACE, "Setting up SSL connection"); //$NON-NLS-1$

			// System.setProperty("com.ibm.jsse2.overrideDefaultTLS","true");
			
			if (isSslDebug())
				System.setProperty("javax.net.debug","true");

			if(context.getParameterNames().contains(JMSOpConstants.PARAM_AUTH_KEYSTORE))
				System.setProperty("javax.net.ssl.keyStore", getAbsolutePath(getKeyStore()));				
			if(context.getParameterNames().contains(JMSOpConstants.PARAM_AUTH_KEYSTOREPW))
				System.setProperty("javax.net.ssl.keyStorePassword", getKeyStorePassword());				
			if(context.getParameterNames().contains(JMSOpConstants.PARAM_AUTH_TRUSTSTORE))
				System.setProperty("javax.net.ssl.trustStore",  getAbsolutePath(getTrustStore()));			
			if(context.getParameterNames().contains(JMSOpConstants.PARAM_AUTH_TRUSTSTOREPW))
				System.setProperty("javax.net.ssl.trustStorePassword",  getTrustStorePassword());
		}
		
		if(consistentRegionContext != null) {
			crState = new JMSSourceCRState();
		}

		// create connection document parser object (which is responsible for
		// parsing the connection document)
		ConnectionDocumentParser connectionDocumentParser = new ConnectionDocumentParser();
		// check if the error output port is specified or not

		if (context.getNumberOfStreamingOutputs() == 2) {
			hasErrorPort = true;
			errorOutputPort = getOutput(1);
		}

		// set the data output port
		dataOutputPort = getOutput(0);

		StreamSchema streamSchema = getOutput(0).getStreamSchema();

		// check if connections file is valid, if any of the below checks fail,
		// the operator throws a runtime error and abort

		connectionDocumentParser.parseAndValidateConnectionDocument(getConnectionDocument(),
																	connection,
																	access,
																	streamSchema,
																	false,
																	context.getPE().getApplicationDirectory());

		// codepage parameter can come only if message class is bytes
		if (connectionDocumentParser.getMessageType() != MessageClass.bytes &&
			context.getParameterNames().contains("codepage")) //$NON-NLS-1$
		{
			throw new ParseConnectionDocumentException(Messages.getString("CODEPAGE_APPEARS_ONLY_WHEN_MSG_CLASS_IS_BYTES")); //$NON-NLS-1$
		}

		// populate the message type and isAMQ
		messageType = connectionDocumentParser.getMessageType();
		isAMQ = connectionDocumentParser.isAMQ();

		// parsing connection document is successful,
		// we can go ahead and create connection
		// isProducer, false implies Consumer
        PropertyProvider propertyProvider = null;
		if(getAppConfigName() != null) {
			propertyProvider = new PropertyProvider(context.getPE(), getAppConfigName());
		}
		
		jmsConnectionHelper = new JMSConnectionHelper(connectionDocumentParser, reconnectionPolicy,
				reconnectionBound, period, false, 0, 0, nReconnectionAttempts, logger, (consistentRegionContext != null), 
				messageSelector, propertyProvider, userPropName , passwordPropName, null);

		// Create the appropriate JMS message handlers as specified by the
		// messageType.

		switch (connectionDocumentParser.getMessageType()) {
			case map:
				messageHandlerImpl = new MapMessageHandler(
						connectionDocumentParser.getNativeSchemaObjects());
				break;
			case stream:
				messageHandlerImpl = new StreamMessageHandler(
						connectionDocumentParser.getNativeSchemaObjects());
				break;
			case bytes:
				messageHandlerImpl = new BytesMessageHandler(
						connectionDocumentParser.getNativeSchemaObjects(), codepage);
				break;
			case empty:
				messageHandlerImpl = new EmptyMessageHandler(
						connectionDocumentParser.getNativeSchemaObjects());
				break;
			case text:
				messageHandlerImpl = new TextMessageHandler(connectionDocumentParser.getNativeSchemaObjects());
				break;
			default:
				throw new RuntimeException(Messages.getString("NO_VALID_MSG_CLASS_SPECIFIED")); //$NON-NLS-1$
		}
		
		// register for data governance
		registerForDataGovernance(connectionDocumentParser.getProviderURL(), connectionDocumentParser.getDestination());
		
		// Check if this operator accesses JMS Header values
		for(String jmsHeaderValOutAttributeName : jmsHeaderValOutAttributeNames ) {
	        if (context.getParameterNames().contains(jmsHeaderValOutAttributeName)) {
	        	operatorAccessesJMSHeaderValues = true;
	        }
		}
		
		checkPrepareJmsHeaderPropertiesAccess(context, streamSchema);

		tracer.log(TraceLevel.TRACE, "End initialize()"); //$NON-NLS-1$
	}

	
	/**
	 * Check and prepare this operator's access to JMS Header property values.
	 * 
	 * @param context	This operator's context data
	 */
	private void checkPrepareJmsHeaderPropertiesAccess(OperatorContext context, StreamSchema streamSchema) {
 
        if((context.getParameterNames().contains(JMSOpConstants.PARAM_JMS_HEADER_PROPS))) {
        	
        	operatorAccessesJMSHeaderPropertyValues = true;
        	patTriplets = new ArrayList<PropertyAttributeType>();
        	
        	JmsHeaderHelper.prepareJmsHeaderPropertiesAccess(context, streamSchema, patTriplets, logger, tracer);
        }
        
        if((context.getParameterNames().contains(JMSOpConstants.PARAM_JMS_HEADER_PROPS_O_ATTR_NAME))) {
        	operatorGenericallyAccessesJMSHeaderPropertyValues = true;
        	
        	List<String> jmsHeaderPropOutAttributeName = context.getParameterValues(JMSOpConstants.PARAM_JMS_HEADER_PROPS_O_ATTR_NAME);
        	jmsHeaderPropOutAttributeIndex = streamSchema.getAttributeIndex(jmsHeaderPropOutAttributeName.get(0));
        }
        
	}

	
	protected String getAbsolutePath(String filePath) {
		if(filePath == null) 
			return null;
		
		Path p = Paths.get(filePath);
		if(p.isAbsolute()) {
			return filePath;
		} else {
			File f = new File (getOperatorContext().getPE().getApplicationDirectory(), filePath);
			return f.getAbsolutePath();
		}
	}
	
	private void registerForDataGovernance(String providerURL, String destination) {
		logger.log(TraceLevel.INFO, "JMSSource - Registering for data governance with providerURL: " + providerURL //$NON-NLS-1$
				+ " destination: " + destination); //$NON-NLS-1$
		DataGovernanceUtil.registerForDataGovernance(this, destination, IGovernanceConstants.ASSET_JMS_MESSAGE_TYPE,
				providerURL, IGovernanceConstants.ASSET_JMS_SERVER_TYPE, true, "JMSSource"); //$NON-NLS-1$
	}

	/* (non-Javadoc)
	 * @see com.ibm.streams.operator.samples.patterns.ProcessTupleProducer#process()
	 */
	@Override
	protected void process() throws IOException, ConnectionException
	{
		boolean	isInConsistentRegion	= consistentRegionContext != null;
		boolean	isTriggerOperator		= isInConsistentRegion && consistentRegionContext.isTriggerOperator();
		
		// create the initial connection.
	    try
	    {
	    	jmsConnectionHelper.createInitialConnection();
			if(isInConsistentRegion) {
				notifyResetLock(true);
		    }
	    }
	    catch (InterruptedException ie)
	    {
	        return;
		}
	    catch (ConnectionException ce)
	    {
			
			if(isInConsistentRegion) {
				notifyResetLock(false);
			}
			// Initial connection fails to be created.
			// throw the exception.
			throw ce;
		}

		long timeout				= JMSSource.RECEIVE_TIMEOUT;
		long sessionCreationTime	= 0;

		while (!Thread.interrupted())
		{
			// read a message from the consumer
			try
			{
				if(isInConsistentRegion)
				{
					consistentRegionContext.acquirePermit();
					
					// A checkpoint has been made, thus acknowledging the last sent message
					if(crState.isCheckpointPerformed()) {
						
						try {
							crState.acknowledgeMsg();
						} catch (Exception e) {
							consistentRegionContext.reset();
						} finally {
							crState.reset();
						}
				
					}
				}

				Message msg = null;
				try
				{
				    msg = jmsConnectionHelper.receiveMessage(timeout);
				}
				catch (ConnectionException | InterruptedException ceie)
				{
				    throw ceie;
				}
				catch (/*JMS*/Exception e)
				{
				    // receive retry after reconnection attempt failed
				    continue;
                }
				
				if (msg == null)
				{
					continue;
				}
				
				// here we definitely have a JMS message. If exceptions happen here, we lose a message.
				// nMessagesRead indicates the number of messages which we have
				// read from the JMS Provider successfully
				nMessagesRead.incrementValue(1);
				try
				{
    				if(isInConsistentRegion)
    				{
    					// following section takes care of possible duplicate messages
    					// i.e connection re-created due to failure causing unacknowledged message to be delivered again
    					// we don't want to process duplicate messages again.
    					if(crState.getLastMsgSent() == null)
    					{
    						sessionCreationTime = jmsConnectionHelper.getSessionCreationTime();
    					}
    					else
    					{
    						// if session has been re-created and message is duplicate,ignore
    						if(jmsConnectionHelper.getSessionCreationTime() > sessionCreationTime && 
    						   isDuplicateMsg(msg, crState.getLastMsgSent().getJMSTimestamp(), crState.getMsgIDWIthSameTS())) {
    						    logger.log(LogLevel.INFO, "IGNORED_DUPLICATED_MSG", msg.getJMSMessageID()); //$NON-NLS-1$
    							continue;
    						}
    					}
    				}
    				
    				
    				OutputTuple dataTuple = dataOutputPort.newTuple();
				
    				// convert the message to the output Tuple using the appropriate message handler
    				MessageAction returnVal = messageHandlerImpl.convertMessageToTuple(msg, dataTuple);
    				
    				// take an action based on the return type
    				switch (returnVal) {
    				// the message type is incorrect
    				case DISCARD_MESSAGE_WRONG_TYPE:
    					nMessagesDropped.incrementValue(1);
    
    					logger.log(LogLevel.WARN, "DISCARD_WRONG_MESSAGE_TYPE", //$NON-NLS-1$
    							new Object[] { messageType });
    
    					// If the error output port is defined, redirect the error
    					// to error output port
    					if (hasErrorPort) {
    						sendOutputErrorMsg(Messages.getString("DISCARD_WRONG_MESSAGE_TYPE", new Object[] { messageType })); //$NON-NLS-1$
    					}
    					break;
    				// if unexpected end of message has been reached
    				case DISCARD_MESSAGE_EOF_REACHED:
    					nMessagesDropped.incrementValue(1);
    					logger.log(LogLevel.WARN, "DISCARD_MSG_TOO_SHORT"); //$NON-NLS-1$
    					// If the error output port is defined, redirect the error
    					// to error output port
    					if (hasErrorPort) {
    						sendOutputErrorMsg(Messages.getString("DISCARD_MSG_TOO_SHORT")); //$NON-NLS-1$
    					}
    					break;
    				// Mesage is read-only
    				case DISCARD_MESSAGE_UNREADABLE:
    					nMessagesDropped.incrementValue(1);
    					logger.log(LogLevel.WARN, "DISCARD_MSG_UNREADABLE"); //$NON-NLS-1$
    					// If the error output port is defined, redirect the error
    					// to error output port
    					if (hasErrorPort) {
    						sendOutputErrorMsg(Messages.getString("DISCARD_MSG_UNREADABLE")); //$NON-NLS-1$
    					}
    					break;
    				case DISCARD_MESSAGE_MESSAGE_FORMAT_ERROR:
    					nMessagesDropped.incrementValue(1);
    					logger.log(LogLevel.WARN,
    							"DISCARD_MESSAGE_MESSAGE_FORMAT_ERROR"); //$NON-NLS-1$
    					// If the error output port is defined, redirect the error
    					// to error output port
    					if (hasErrorPort) {
    						sendOutputErrorMsg(Messages.getString("DISCARD_MESSAGE_MESSAGE_FORMAT_ERROR")); //$NON-NLS-1$
    					}
    					break;
    				// the message was read successfully
    				case SUCCESSFUL_MESSAGE:
    					handleJmsHeaderValues(msg, dataTuple);
    					writeJmsHeaderPropertyValuesIntoTuple(msg, dataTuple);
    					dataOutputPort.submit(dataTuple);
    					break;
    				default:
    				    nMessagesDropped.incrementValue(1);
    				    tracer.log(LogLevel.WARN, "No explicit handling for enum " + returnVal.getDeclaringClass().getSimpleName() + " value "
    				            + returnVal + " implemented in switch statement.", "JMSSource");
                        logger.log(LogLevel.WARN, "DISCARD_MESSAGE_MESSAGE_VARIABLE", "unexpected return value: " + returnVal); //$NON-NLS-1$
                        // If the error output port is defined, redirect the error
                        // to error output port
                        if (hasErrorPort) {
                            sendOutputErrorMsg(Messages.getString("DISCARD_MESSAGE_MESSAGE_VARIABLE", "unexpected return value: " + returnVal)); //$NON-NLS-1$
                        }
    				}
    				
    				// set last processed message
    				if(isInConsistentRegion) {
    					crState.setLastMsgSent(msg);
    				}
    				
    				// If the consistent region is driven by operator, then
    				// 1. increase message counter
    				// 2. Call make consistent region if message counter reached the triggerCounter specified by user
    			    if(isTriggerOperator) {
    					crState.increaseMsgCounterByOne();
    					
    					if(crState.getMsgCounter() == getTriggerCount()){
    						consistentRegionContext.makeConsistent();
    					}
    			    }
				}
				catch (Exception /*UnsupportedEncodingException | JMSException*/ e)
				{
				    nMessagesDropped.incrementValue(1);
	                tracer.log(LogLevel.WARN, "failed to convert JMS message to tuple: " + e.toString(), "JMSSource");
	                logger.log(LogLevel.WARN, "DISCARD_MESSAGE_MESSAGE_VARIABLE", e.toString()); //$NON-NLS-1$
	                // If the error output port is defined, redirect the error
	                // to error output port
	                if (hasErrorPort) {
	                    sendOutputErrorMsg(Messages.getString("DISCARD_MESSAGE_MESSAGE_VARIABLE", e.toString()));
	                }
				}
			}
			catch (InterruptedException e) {
			    // Thread has been interrupted, interrupted state is set; we will leave the while loop - no further action
			}
			catch (IOException | ConnectionException e) {
			    // problem resetting CR --> throw
			    // final problem with connection
			    throw e;
            }
			catch (Exception e) {
                // failed to send output tuple
                nMessagesDropped.incrementValue(1);
                tracer.log (LogLevel.WARN, "failed to submit output tuple: " + e.toString(), "JMSSource");
            }
			finally {
				if(consistentRegionContext != null) {
					consistentRegionContext.releasePermit();
				}
			}
		}
	}
	
	


	/**
	 * Handles the values of the JMSHeader of the current message. It first checks
	 * if there is an attribute in the Stream to write the header value into.
	 *  
	 * @param msg			The current JMS message.
	 * @param outTuple		The output tuple.
	 * @throws JMSException 
	 */
	private void handleJmsHeaderValues(Message msg, OutputTuple outTuple) throws JMSException {
		
		if(! operatorAccessesJMSHeaderValues ) return; 

		int jmsDestinationAttrIdx	= -1;
		int jmsDeliveryModeAttrIdx	= -1;
		int jmsExpirationAttrIdx	= -1;
		int jmsPriorityAttrIdx		= -1;
		int jmsMessageIDAttrIdx		= -1;
		int jmsTimestampAttrIdx		= -1;
		int jmsCorrelationIDAttrIdx	= -1;
		int jmsReplyToAttrIdx		= -1;
		int jmsTypeAttrIdx			= -1;
		int jmsRedeliveredAttrIdx	= -1;
		
		
		StreamSchema streamSchema = getOutput(0).getStreamSchema();

		
		if(this.getJmsDestinationOutAttributeName() != null) {
			jmsDestinationAttrIdx = streamSchema.getAttributeIndex(this.getJmsDestinationOutAttributeName());
		}
		if(jmsDestinationAttrIdx != -1 && msg.getJMSDestination() != null) {
			outTuple.setObject(jmsDestinationAttrIdx, new RString(getDestinationName(msg.getJMSDestination())));
		}
		
		if(this.getJmsDeliveryModeOutAttributeName() != null) {
			jmsDeliveryModeAttrIdx = streamSchema.getAttributeIndex(this.getJmsDeliveryModeOutAttributeName());
		}
		if(jmsDeliveryModeAttrIdx != -1) {
			outTuple.setObject(jmsDeliveryModeAttrIdx, new Integer(msg.getJMSDeliveryMode()));
		}
		
		if(this.getJmsExpirationOutAttributeName() != null) {
			jmsExpirationAttrIdx = streamSchema.getAttributeIndex(this.getJmsExpirationOutAttributeName());
		}
		if(jmsExpirationAttrIdx != -1) {
			outTuple.setObject(jmsExpirationAttrIdx, new Long(msg.getJMSExpiration()));
		}
		
		if(this.getJmsPriorityOutAttributeName() != null) {
			jmsPriorityAttrIdx = streamSchema.getAttributeIndex(this.getJmsPriorityOutAttributeName());
		}
		if(jmsPriorityAttrIdx != -1) {
			outTuple.setObject(jmsPriorityAttrIdx, new Integer(msg.getJMSPriority()));
		}
		
		if(this.getJmsMessageIDOutAttributeName() != null) {
			jmsMessageIDAttrIdx = streamSchema.getAttributeIndex(this.getJmsMessageIDOutAttributeName());
		}
		if(jmsMessageIDAttrIdx != -1 && msg.getJMSMessageID() != null) {
			outTuple.setObject(jmsMessageIDAttrIdx, new RString(msg.getJMSMessageID()));
		}

		if(this.getJmsTimestampOutAttributeName() != null) {
			jmsTimestampAttrIdx = streamSchema.getAttributeIndex(this.getJmsTimestampOutAttributeName());
		}
		if(jmsTimestampAttrIdx != -1) {
			outTuple.setObject(jmsTimestampAttrIdx, new Long(msg.getJMSTimestamp()));
		}
		
		if(this.getJmsCorrelationIDOutAttributeName() != null) {
			jmsCorrelationIDAttrIdx = streamSchema.getAttributeIndex(this.getJmsCorrelationIDOutAttributeName());
		}
		if(jmsCorrelationIDAttrIdx != -1 && msg.getJMSCorrelationID() != null) {
			outTuple.setObject(jmsCorrelationIDAttrIdx, new RString(msg.getJMSCorrelationID()));
		}
		
		if(this.getJmsReplyToOutAttributeName() != null) {
			jmsReplyToAttrIdx = streamSchema.getAttributeIndex(this.getJmsReplyToOutAttributeName());
		}
		if(jmsReplyToAttrIdx != -1 && msg.getJMSReplyTo() != null) {
			outTuple.setObject(jmsReplyToAttrIdx, new RString(getDestinationName(msg.getJMSReplyTo())));
		}
				
		if(this.getJmsTypeOutAttributeName() != null) {
			jmsTypeAttrIdx = streamSchema.getAttributeIndex(this.getJmsTypeOutAttributeName());
		}
		if(jmsTypeAttrIdx != -1 && msg.getJMSType() != null) {
			outTuple.setObject(jmsTypeAttrIdx, new RString(msg.getJMSType()));
		}
		
		if(this.getJmsRedeliveredOutAttributeName() != null) {
			jmsRedeliveredAttrIdx = streamSchema.getAttributeIndex(this.getJmsRedeliveredOutAttributeName());
		}
		if(jmsRedeliveredAttrIdx != -1) {
			outTuple.setObject(jmsRedeliveredAttrIdx, new Boolean(msg.getJMSRedelivered()));
		}
	}


	/**
	 * Reads JMS Header property values from the current
	 * JMS message and stores them in output tuple attributes
	 * according to the mapping defined via 'jmsHeaderProperties'
	 * parameter.
	 *  
	 * @param msg			The current JMS message.
	 * @param outTuple		The output tuple.
	 */
	private void writeJmsHeaderPropertyValuesIntoTuple(Message msg, OutputTuple outTuple) {
		
		// If we do not access property values, return
		if( ! operatorAccessesJMSHeaderPropertyValues &&
			! operatorGenericallyAccessesJMSHeaderPropertyValues ) return;
		

		try {
			// If the message has no property values, return
			if(! msg.getPropertyNames().hasMoreElements()) return;

			
			// Handling JMS Header Properties by their configured name and type information 
			if( operatorAccessesJMSHeaderPropertyValues ) {
				for(PropertyAttributeType pat : patTriplets) {
					
					PropertyType	typeSpec	= pat.getTypeSpecification();
					String			propName	= pat.getPropertyName();
					int				attrIdx		= pat.getAttributeIdx();
					
					if(msg.propertyExists(propName)) {

						switch (typeSpec) {
						case BOOL:
							{
								boolean value = msg.getBooleanProperty(propName);
								outTuple.setObject(attrIdx, new Boolean(value));
							}
							break;
						case BYTE:
							{
								byte value = msg.getByteProperty(propName);
								outTuple.setObject(attrIdx, new Byte(value));
							}
							break;
						case SHORT:
							{
								short value = msg.getShortProperty(propName);
								outTuple.setObject(attrIdx, new Short(value));
							}
							break;
						case INT:
							{
								int value = msg.getIntProperty(propName);
								outTuple.setObject(attrIdx, new Integer(value));
							}
							break;
						case LONG:
							{
								long value = msg.getLongProperty(propName);
								outTuple.setObject(attrIdx, new Long(value));
							}
							break;
						case FLOAT:
							{
								float value = msg.getFloatProperty(propName);
								outTuple.setObject(attrIdx, new Float(value));
							}
							break;
						case DOUBLE:
							{
								double value = msg.getDoubleProperty(propName);
								outTuple.setObject(attrIdx, new Double(value));
							}
							break;
						case STRING:
							{
								String value = msg.getStringProperty(propName);
								outTuple.setObject(attrIdx, new RString(value));
							}
							break;
						case OBJECT:
							{
								Object value = msg.getObjectProperty(propName);
								outTuple.setObject(attrIdx, value);
							}
							break;
						}
					}
					else {
						tracer.log(TraceLevel.DEBUG, "JMS Header property not contained in current message: " + propName);	//$NON-NLS-1$
					}
				}
			}
			
			
			// Handling all JMS Header properties generically by folding them into one map
			if( operatorGenericallyAccessesJMSHeaderPropertyValues ) {
				
				@SuppressWarnings("rawtypes")
				Enumeration			propertyNames		= msg.getPropertyNames();
				Map<String,String>	receivedProperties	= new HashMap<String,String>();
				
				while(propertyNames.hasMoreElements()) {
					String	propertyName		= (String)propertyNames.nextElement();
					String	propertyValue		= msg.getObjectProperty(propertyName).toString();

					receivedProperties.put(propertyName, propertyValue);
				}

				outTuple.setObject(jmsHeaderPropOutAttributeIndex, receivedProperties);
			}
		}
		catch (JMSException e) {
			String errMsg = Messages.getString("ERROR_WHILE_READING_JMSHEADERPROPS", e.getMessage());	//$NON-NLS-1$
			logger.log(LogLevel.ERROR, errMsg);
		}
	}
	
	
	/**
	 * Determines and returns the destination name
	 *  
	 * @param destination	The destination to determine the name for.
	 * @return				The name of a Queue or Topic, or a string representation of the destination object.
	 * @throws JMSException
	 */
	private String getDestinationName(Destination destination) throws JMSException {
		if (destination instanceof Queue) return ((Queue)destination).getQueueName();
		if (destination instanceof Topic) return ((Topic)destination).getTopicName();
		return destination.toString();
	}


	// Send the error message on to the error output port if one is specified
	private void sendOutputErrorMsg(String errorMessage) {
		OutputTuple errorTuple = errorOutputPort.newTuple();
		String consolidatedErrorMessage = errorMessage;
		// set the error message
		errorTuple.setString(0, consolidatedErrorMessage);
		// submit the tuple.
		try {
		errorOutputPort.submit(errorTuple);
		} catch (Exception e) {
		    tracer.log (LogLevel.ERROR, "failed to submit error output tuple: " + e.toString(), "JMSSource");
		}
	}

	@Override
	public void shutdown() throws Exception {
		// close the connection.
		if (isAMQ) {
			super.shutdown();
			jmsConnectionHelper.closeConnection();
		} else {
			jmsConnectionHelper.closeConnection();
			super.shutdown();
		}

	}
	
	private boolean isInitialConnectionEstablished() throws InterruptedException {
		
		synchronized(resetLock) {
			if(initalConnectionEstablished) {
				return true;
			}
			
			resetLock.wait();
			return initalConnectionEstablished;
		}
	}
	
	private void notifyResetLock(boolean result) {
		if(consistentRegionContext != null) {
	    	synchronized(resetLock) {
		    	initalConnectionEstablished = result;
		    	resetLock.notifyAll();
		    }
	    }
	}

	@Override
	public void close() throws IOException {
		
	}

	@Override
	public void checkpoint(Checkpoint checkpoint) throws Exception {
		logger.log(LogLevel.INFO, "CHECKPOINT", checkpoint.getSequenceId()); //$NON-NLS-1$
	 
		crState.setCheckpointPerformed(true);
		
		ObjectOutputStream stream = checkpoint.getOutputStream();
		
		stream.writeBoolean(crState.getLastMsgSent() != null);
		
		if(crState.getLastMsgSent() != null) {
			stream.writeLong(crState.getLastMsgSent().getJMSTimestamp());
			stream.writeObject(crState.getMsgIDWIthSameTS());
		}
		
	}

	@Override
	public void drain() throws Exception {
		logger.log(LogLevel.INFO, "DRAIN"); //$NON-NLS-1$		
	}

	@SuppressWarnings("unchecked")
	@Override
	public void reset(Checkpoint checkpoint) throws Exception {
		logger.log(LogLevel.INFO, "RESET_CHECKPOINT", checkpoint.getSequenceId()); //$NON-NLS-1$
		
		if(!isInitialConnectionEstablished()) {
			throw new ConnectionException(Messages.getString("CONNECTION_TO_JMS_FAILED", new Object[]{})); //$NON-NLS-1$
		}
		
		// Reset consistent region variables and recover JMS session to make re-delivery of
		// unacknowledged message 
		jmsConnectionHelper.recoverSession();
				
		ObjectInputStream stream = checkpoint.getInputStream();
		boolean hasMsg = stream.readBoolean();
		
		if(hasMsg) {
			long lastSentMsgTS = stream.readLong();
			List<String> lastSentMsgIDs =  (List<String>) stream.readObject();
			
			deduplicateMsg(lastSentMsgTS, lastSentMsgIDs);
		}
		
		crState.reset();
	
	}
	
	private boolean isDuplicateMsg(Message msg, long lastSentMsgTs, List<String> lastSentMsgIDs) throws JMSException {
		boolean res = false;
		
		if(msg.getJMSTimestamp() < lastSentMsgTs) {
			res = true;
		}			
		else if(msg.getJMSTimestamp() == lastSentMsgTs) {
			
			if(lastSentMsgIDs.contains(msg.getJMSMessageID())) {
				res = true;
			}
			
		}
		
		return res;
		
	}
	
	private void deduplicateMsg(long lastSentMsgTs, List<String> lastSentMsgIDs) throws JMSException, ConnectionException, InterruptedException {
		logger.log(LogLevel.INFO, "DEDUPLICATE_MESSAGES"); //$NON-NLS-1$
		
		boolean stop = false;
		
		while(!stop) {
			
			Message msg = jmsConnectionHelper.receiveMessage(JMSSource.RECEIVE_TIMEOUT);
			
			if(msg == null) {
				return;
			}
			
			if(isDuplicateMsg(msg, lastSentMsgTs, lastSentMsgIDs)) {
				msg.acknowledge();
				logger.log(LogLevel.INFO, "IGNORED_DUPLICATED_MSG", msg.getJMSMessageID()); //$NON-NLS-1$
			}
			else {
				jmsConnectionHelper.recoverSession();
				stop = true;
			}
			
		}
		
	}

	@Override
	public void resetToInitialState() throws Exception {
		logger.log(LogLevel.INFO, "RESET_TO_INITIAL_STATE"); //$NON-NLS-1$
		
		if(!isInitialConnectionEstablished()) {
			throw new ConnectionException(Messages.getString("CONNECTION_TO_JMS_FAILED", new Object[]{})); //$NON-NLS-1$
		}
		
		jmsConnectionHelper.recoverSession();
		crState.reset();
	
	}

	@Override
	public void retireCheckpoint(long id) throws Exception {
		logger.log(LogLevel.INFO, "RETIRE_CHECKPOINT", id);		 //$NON-NLS-1$
	}

	public static final String DESC = "\\n" +  //$NON-NLS-1$ 
"The `JMSSource` operator reads data from a WebSphere MQ or an Apache Active MQ queue\\n" +  //$NON-NLS-1$
"or a topic and creates tuples from the read data.\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"The `JMSSource` operator converts each WebSphere MQ or Apache Active MQ message to a separate tuple\\n" +  //$NON-NLS-1$
"and sends it to the output stream. A single message is converted into a single tuple.\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"# SSL Support\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"The `JMSSource` operator provides support for SSL via these parameters: *sslConnection*, *keyStore*, *keyStorePassword* and *trustStore*.\\n" +  //$NON-NLS-1$ 
"When *sslConnection* is set to `true`, the *keyStore*, *keyStorePassword* and *trustStore* parameters must be set.\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"**Note:** The `JMSSource` operator configures SSL by setting the JVM system properties via calls to `System.property()`.\\n" +  //$NON-NLS-1$ 
"Java operators that are fused into the same PE share the same JVM. This implies that any other Java operators fused into the \\n" +  //$NON-NLS-1$
"same PE as the `JMSSource` operator will have these SSL properties set. If this is undesirable, then the `JMSSource` operator\\n" +  //$NON-NLS-1$
"should be placed into it's own PE.\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"# Behavior in a consistent region\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"The `JMSSource` operator can participate in a consistent region. The operator must be at the start of a consistent region.\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"The operator supports periodic and operator-driven consistent region policies. If the consistent region policy is set as operatorDriven, the triggerCount parameter must be specified. The operator initiates a checkpoint after number of tuples specified by the triggerCount parameter have been processed.\\n" +  //$NON-NLS-1$ 
"If the consistent region policy is set as periodic, the operator respects the period setting and establishes consistent states accordingly.\\n" +  //$NON-NLS-1$ 
"\\n" +  //$NON-NLS-1$
"When a message queue is consumed by multiple message consumers, i.e. multiple `JMSSource` instances are used to read messages from a same queue, then deterministic routing is required. This requirement can be achieved through the messageSelector parameter. For example, if an SPL application has two JMSSource operator instances and a JMS property named \\\"group\\\" is present on messages that can take value of either 'g1' or 'g2', then each JMSSource operator instance can be assigned in the following manner:\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"MyPersonNamesStream1 = JMSSource()\\n" +  //$NON-NLS-1$
"	{\\n" +  //$NON-NLS-1$
"		param\\n" +  //$NON-NLS-1$
"			connectionDocument : \\\"/home/streamsuser/connections/JMSconnections.xml\\\";\\n" +  //$NON-NLS-1$
"			connection         : \\\"amqConn\\\";\\n" +  //$NON-NLS-1$
"			access             : \\\"amqAccess\\\";\\n" +  //$NON-NLS-1$
"			messageSelector    : \\\"group = 'g1'\\\";\\n" +  //$NON-NLS-1$
"	}\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"MyPersonNamesStream2 = JMSSource()\\n" +  //$NON-NLS-1$
"    {\\n" +  //$NON-NLS-1$
"		param\\n" +  //$NON-NLS-1$
"	        connectionDocument : \\\"/home/streamsuser/connections/JMSconnections.xml\\\";\\n" +  //$NON-NLS-1$
"			connection         : \\\"amqConn\\\";\\n" +  //$NON-NLS-1$
"			access             : \\\"amqAccess\\\";\\n" +  //$NON-NLS-1$
"			messageSelector    : \\\"group = 'g2'\\\";\\n" +  //$NON-NLS-1$
"	}\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"# Exceptions\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"The following types of exceptions can occur:\\n" +  //$NON-NLS-1$
" * Run time errors that halt the operator execution.\\n" +  //$NON-NLS-1$
"   * The JMSSource operator throws an exception and terminates in the following cases.\\n" +  //$NON-NLS-1$
"     For some exceptions, the trace and log information is logged in the console logs\\n" +  //$NON-NLS-1$
"     and also output to the optional output port if the application is configured to use the optional port.\\n" +  //$NON-NLS-1$
"     * During the initial connection attempt or during transient reconnection failures,\\n" +  //$NON-NLS-1$
"       if the **reconnectionPolicy** is set to `NoRetry` and the operator does not have a successful connection,\\n" +  //$NON-NLS-1$
"       or the **reconnectionPolicy** is set to `BoundedRetry` and the operator does not have a successful connection\\n" +  //$NON-NLS-1$
"       after the number of attempts that are specified in the **reconnectionBound** parameter. Successive data is lost.\\n" +  //$NON-NLS-1$
"     * The queue name is unknown.\\n" +  //$NON-NLS-1$
"     * The queue manager name is unknown.\\n" +  //$NON-NLS-1$
"     * The operator is unable to connect to the host or the port.\\n" +  //$NON-NLS-1$
"     * A MapMessage, StreamMessage, or BytesMessage does not contain the attributes that are specified in the native schema.\\n" +  //$NON-NLS-1$
"     * A MapMessage or StreamMessage contains attributes whose data type does not match the data type\\n" +  //$NON-NLS-1$
"       that is specified in the native schema and the conversion fails.\\n" +  //$NON-NLS-1$
"     * The message_class attribute in the access specification is bytes, stream, or map, and the name\\n" +  //$NON-NLS-1$
"       and type attributes of the &lt;native_schema&gt; element do not match the name and type attribute in the output stream schema.\\n" +  //$NON-NLS-1$
"     * When the message_class attribute is bytes and in the &lt;native_schema&gt; element,\\n" +  //$NON-NLS-1$
"       the type attribute is string or bytes and the length attribute is missing.\\n" +  //$NON-NLS-1$
"     * An invalid value is specified for the message_class attribute of the access specification.\\n" +  //$NON-NLS-1$
"     * If an attribute occurs more than once in the &lt;native_schema&gt; element.\\n" +  //$NON-NLS-1$
"     * The **connectionDocument** parameter refers to an file that does not exist.\\n" +  //$NON-NLS-1$
"     * The **connectionDocument** parameter is not specified and the `connections.xml` file is not present in the default location.\\n" +  //$NON-NLS-1$
"     * The **jmsDestinationOutAttributeName** parameter is specified but the attribute is not found in output schema or the type of attribute is not a rstring type." +  //$NON-NLS-1$
"     * The **jmsDeliveryModeOutAttributeName** parameter is specified but the attribute is not found in output schema or the type of attribute is not a int32 type.\\n" +  //$NON-NLS-1$
"     * The **jmsExpirationOutAttributeName** parameter is specified but the attribute is not found in output schema or the type of attribute is not a int64 type.\\n" +  //$NON-NLS-1$
"     * The **jmsPriorityOutAttributeName** parameter is specified but the attribute is not found in output schema or the type of attribute is not a int32 type.\\n" +  //$NON-NLS-1$
"     * The **jmsMessageIDOutAttributeName** parameter is specified but the attribute is not found in output schema or the type of attribute is not a rstring type.\\n" +  //$NON-NLS-1$
"     * The **jmsTimestampOutAttributeName** parameter is specified but the attribute is not found in output schema or the type of attribute is not a int64 type.\\n" +  //$NON-NLS-1$
"     * The **jmsCorrelationIDOutAttributeName** parameter is specified but the attribute is not found in output schema or the type of attribute is not a rstring type.\\n" +  //$NON-NLS-1$
"     * The **jmsReplyToOutAttributeName** parameter is specified but the attribute is not found in output schema or the type of attribute is not a rstring type.\\n" +  //$NON-NLS-1$
"     * The **jmsTypeOutAttributeName** parameter is specified but the attribute is not found in output schema or the type of attribute is not a rstring type.\\n" +  //$NON-NLS-1$
"     * The **jmsRedeliveredOutAttributeName** parameter is specified but the attribute is not found in output schema or the type of attribute is not a boolean type.\\n" +  //$NON-NLS-1$
"     * The **jmsHeaderProperties** parameter is specified but the contained configuration is erroneous.\\n" +  //$NON-NLS-1$
"     * The **jmsHeaderPropertiesOutAttributeName** parameter is specified but the attribute is not found in output schema or the type of attribute is not map<ustring,ustring>." +  //$NON-NLS-1$
" * Run time errors that cause a message to be dropped and an error message to be logged.\\n" +  //$NON-NLS-1$
"   * The `JMSSource` operator throws an exception and discards the message in the following cases.\\n" +  //$NON-NLS-1$
"     The trace and log information for these exceptions is logged in the console logs\\n" +  //$NON-NLS-1$
"     and also output to the optional output port if the application is configured to use the optional port.\\n" +  //$NON-NLS-1$
"     * The `JMSSource` operator reads a message that does not match the message class in the &lt;native_schema&gt; element.\\n" +  //$NON-NLS-1$
"     * When a negative length (-2 or -4) is specified in the native schema for bytes and string data types\\n" +  //$NON-NLS-1$
"       and the message class is bytes, the operator expects the JMS message to start with a 2 or 4-byte length field.\\n" +  //$NON-NLS-1$
"       If there are insufficient bytes remaining in the JMS message,\\n" +  //$NON-NLS-1$
"       the operator discards the entire message and logs a run time error.\\n" +  //$NON-NLS-1$
"     * When a non-negative length is specified in the native schema for bytes and string,\\n" +  //$NON-NLS-1$
"       the operator attempts to read exactly that number of bytes from the BytesMessage.\\n" +  //$NON-NLS-1$
"       If there are insufficient bytes remaining in the JMS message,\\n" +  //$NON-NLS-1$
"       the operator discards the entire message and logs a run time error.\\n" +  //$NON-NLS-1$
"     * The **reconnectionBound** parameter is specified, but the **reconnectionPolicy** parameter is set\\n" +  //$NON-NLS-1$
"       to a value other than `BoundedRetry`.\\n" +  //$NON-NLS-1$
" * Compile time errors.\\n" +  //$NON-NLS-1$
"   * The `JMSSource` operator throws a compile time error in the following cases.\\n" +  //$NON-NLS-1$
"     The trace and log information for these exceptions is logged in the console logs\\n" +  //$NON-NLS-1$
"     and also output to the optional output port if the application is configured to use the optional port.\\n" +  //$NON-NLS-1$
"     * The mandatory parameters, **connection** and **access** are not specified.\\n" +  //$NON-NLS-1$
"     * The **period** parameter is specified but the **reconnectionPolicy** parameter is not specified.\\n" +  //$NON-NLS-1$
"     * The **reconnectionBound** parameter is specified, but the **reconnectionPolicy** parameter is not specified.\\n" +  //$NON-NLS-1$
"     * The environment variables **STREAMS_MESSAGING_WMQ_HOME** and **STREAMS_MESSAGING_AMQ_HOME** are not set\\n" +  //$NON-NLS-1$
"       to the locations where the WMQ and AMQ libraries are installed.\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"+ Examples\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"This example shows the use of multiple `JMSSource` operators with different parameter combinations.\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"	composite Main {\\n" +  //$NON-NLS-1$
"	graph\\n" +  //$NON-NLS-1$
"	// JMSSource operator with the default etc/connections.xml(relative to the application directory)\\n" +  //$NON-NLS-1$
"	// connections document\\n" +  //$NON-NLS-1$
"	stream &lt;int32 id, rstring fname, rstring lname&gt;\\n" +  //$NON-NLS-1$
"	MyPersonNamesStream  = JMSSource()\\n" +  //$NON-NLS-1$
"	{\\n" +  //$NON-NLS-1$
"		param\\n" +  //$NON-NLS-1$
"			connection : \\\"amqConn\\\";\\n" +  //$NON-NLS-1$
"			access     : \\\"amqAccess\\\";\\n" +  //$NON-NLS-1$
"	}\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"	// JMSSource operator with fully qualified name of connections.xml\\n" +  //$NON-NLS-1$
"	stream &lt;int32 id, rstring fname, rstring lname&gt;\\n" +  //$NON-NLS-1$
"	MyPersonNamesStream = JMSSource()\\n" +  //$NON-NLS-1$
"	{\\n" +  //$NON-NLS-1$
"		param\\n" +  //$NON-NLS-1$
"			connectionDocument : \\\"/home/streamsuser/connections/JMSconnections.xml\\\";\\n" +  //$NON-NLS-1$
"			connection         : \\\"amqConn\\\";\\n" +  //$NON-NLS-1$
"			access             : \\\"amqAccess\\\";\\n" +  //$NON-NLS-1$
"	}\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"	// JMSSource operator with optional output error port specified\\n" +  //$NON-NLS-1$
"	(stream &lt;int32 id, rstring fname, rstring lname&gt; MyPersonNamesStream ;\\n" +  //$NON-NLS-1$
"	stream &lt;rstring errorMessage&gt; ErrorStream) = JMSSource()\\n" +  //$NON-NLS-1$
"	{\\n" +  //$NON-NLS-1$
"		param\\n" +  //$NON-NLS-1$
"			connection : \\\"amqConn\\\";\\n" +  //$NON-NLS-1$
"			access     : \\\"amqAccess\\\";\\n" +  //$NON-NLS-1$
"	}\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"	// JMSSource operator with optional initDelay and reconnectionPolicy specified\\n" +  //$NON-NLS-1$
"	stream &lt;int32 id, rstring fname, rstring lname&gt;\\n" +  //$NON-NLS-1$
"	MyPersonNamesStream = JMSSource()\\n" +  //$NON-NLS-1$
"	{\\n" +  //$NON-NLS-1$
"		param\\n" +  //$NON-NLS-1$
"			connection         : \\\"amqConn\\\";\\n" +  //$NON-NLS-1$
"			access             : \\\"amqAccess\\\";\\n" +  //$NON-NLS-1$
"			reconnectionPolicy : \\\"NoRetry\\\";\\n" +  //$NON-NLS-1$
"			initDelay          : 10;\\n" +  //$NON-NLS-1$
"	}\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"	// JMSSource Operator with optional period and reconnectionPolicy specified\\n" +  //$NON-NLS-1$
"	stream &lt;int32 id, rstring fname, rstring lname&gt;\\n" +  //$NON-NLS-1$
"	MyPersonNamesStream = JMSSource()\\n" +  //$NON-NLS-1$
"	{\\n" +  //$NON-NLS-1$
"		param\\n" +  //$NON-NLS-1$
"		connection         : \\\"amqConn\\\";\\n" +  //$NON-NLS-1$
"		access             : \\\"amqAccess\\\";\\n" +  //$NON-NLS-1$
"		reconnectionPolicy : \\\"InfiniteRetry\\\";\\n" +  //$NON-NLS-1$
"		period             : 1.20;\\n" +  //$NON-NLS-1$
"	}\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"	// JMSSource operator with reconnectionPolicy specified as BoundedRetry\\n" +  //$NON-NLS-1$
"	stream &lt;int32 id, rstring fname, rstring lname&gt;\\n" +  //$NON-NLS-1$
"	MyPersonNamesStream = JMSSource()\\n" +  //$NON-NLS-1$
"	{\\n" +  //$NON-NLS-1$
"		param\\n" +  //$NON-NLS-1$
"			connection         : \\\"amqConn\\\";\\n" +  //$NON-NLS-1$
"			access             : \\\"amqAccess\\\";\\n" +  //$NON-NLS-1$
"			reconnectionPolicy : \\\"BoundedRetry\\\";\\n" +  //$NON-NLS-1$
"			reconnectionBound : 2;\\n" +  //$NON-NLS-1$
"			period: 1.20;\\n" +  //$NON-NLS-1$
"	}\\n" +  //$NON-NLS-1$
"	}\\n" +  //$NON-NLS-1$
"\\n"  //$NON-NLS-1$
;

}
