/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.jms;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.naming.NamingException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.xml.sax.SAXException;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.LogLevel;
import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.model.CustomMetric;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streams.operator.state.StateHandler;
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
import com.ibm.streamsx.jms.messagehandler.WBE22TextMessageHandler;
import com.ibm.streamsx.jms.messagehandler.WBETextMessageHandler;
import com.ibm.streamsx.jms.messagehandler.XMLTextMessageHandler;
import com.ibm.streamsx.jms.types.MessageClass;
import com.ibm.streamsx.jms.types.PropertyType;
import com.ibm.streamsx.jms.types.ReconnectionPolicies;



@PrimitiveOperator(name = "JMSSink", namespace = "com.ibm.streamsx.jms", description = JMSSink.DESC)
@InputPorts (
	@InputPortSet(	cardinality = 1,
					optional = false,
					windowingMode = InputPortSet.WindowMode.NonWindowed,
					windowPunctuationInputMode = InputPortSet.WindowPunctuationInputMode.Oblivious,
					description = "\\n" +  //$NON-NLS-1$
							"The `JMSSink` operator is configurable with a single input port.\\n" +  //$NON-NLS-1$ 
							"This input port is a data port and is required. The input port is non-mutating and its punctuation mode is Oblivious.\\n"  //$NON-NLS-1$
	)
)
@OutputPorts (
	@OutputPortSet(	cardinality = 1,
					optional = true,
					windowPunctuationOutputMode = OutputPortSet.WindowPunctuationOutputMode.Free,
					description = "\\n" +  //$NON-NLS-1$
							"The `JMSSink` operator is configurable with an optional output port that submits the error message\\n" +  //$NON-NLS-1$
							"and the tuple(optional) that caused this error.\\n" +  //$NON-NLS-1$
							"The optional output port is mutating and its punctuation mode is Free.\\n" +  //$NON-NLS-1$
							"This optional error output port contains an optional first attribute that contains the input tuple\\n" +  //$NON-NLS-1$
							"that caused the error and a second attribute of type rstring that contains the error message.\\n" +  //$NON-NLS-1$
							"Only one error message is sent for each failed tuple.\\n"  //$NON-NLS-1$
	)
)
@Icons(location16 = "icons/JMSSink_16.gif", location32 = "icons/JMSSink_32.gif")
@Libraries({"impl/lib/com.ibm.streamsx.jms.jar", "opt/downloaded/*"})
public class JMSSink extends AbstractOperator implements StateHandler{

	private static final String CLASS_NAME = JMSSink.class.getName();

	/**
	 * Create a {@code Logger} specific to this class that will write to the SPL
	 * log facility as a child of the {@link LoggerNames#LOG_FACILITY}
	 * {@code Logger}. The {@code Logger} uses a
	 */
	private static Logger logger = Logger.getLogger(LoggerNames.LOG_FACILITY + "." + CLASS_NAME, "com.ibm.streamsx.jms.i18n.JMSMessages"); //$NON-NLS-1$ //$NON-NLS-2$
    private static final Logger tracer = Logger.getLogger(CLASS_NAME);
	
	// property names used in message header
	public static final String OP_CKP_NAME_PROPERTITY = "StreamsOperatorCkpName"; //$NON-NLS-1$
	
	public static final String CKP_ID_PROPERTITY = "checkpointId"; //$NON-NLS-1$

	// Variables required by the optional error output port

	// hasErrorPort signifies if the operator has error port defined or not
	// assuming in the beginning that the operator does not have a error output
	// port by setting hasErrorPort to false
	// further down in the code, if the no of output ports is 2, we set to true
	// We send data to error ouput port only in case where hasErrorPort is set
	// to true which implies that the opeator instance has a error output port
	// defined.
	private boolean hasErrorPort = false;
	// Variable to specifiy if the error ouput port has the optional input port
	// tuple or not
	// set to false initially
	private boolean hasOptionalTupleInErrorPort = false;
	// Variable to specify error output port
	private StreamingOutput<OutputTuple> errorOutputPort;
	// Variable to hold the schema of the embedded tuple(if present) in the
	// error output port
	private StreamSchema embeddedSchema;

	// Create an instance of JMSConnectionhelper
	private JMSConnectionHelper jmsConnectionHelper;

	// Metrices
	// nTruncatedInserts: The number of tuples that had truncated attributes
	// while converting to a message
	// nFailedInserts: The number of failed inserts to the JMS Provider.
	// nReconnectionAttempts: The number of reconnection attempts made before a
	// successful connection.
	Metric nTruncatedInserts;
	Metric nFailedInserts;
	Metric nReconnectionAttempts;

	// Initialize the metrices

	@CustomMetric(kind = Metric.Kind.COUNTER, description = "The number of failed inserts to the WebSphere MQ or the Apache ActiveMQ. Failed insertions can occur when a message is dropped because of a run time error.")	//$NON-NLS-1$
	public void setnFailedInserts(Metric nFailedInserts) {
		this.nFailedInserts = nFailedInserts;
	}

	@CustomMetric(kind = Metric.Kind.COUNTER, description = "The number of tuples that had truncated attributes when they were converted to a message.")	//$NON-NLS-1$
	public void setnTruncatedInserts(Metric nTruncatedInserts) {
		this.nTruncatedInserts = nTruncatedInserts;
	}

	@CustomMetric(kind = Metric.Kind.COUNTER, description = "The number of reconnection attempts that are made before a successful connection.")	//$NON-NLS-1$
	public void setnReconnectionAttempts(Metric nReconnectionAttempts) {
		this.nReconnectionAttempts = nReconnectionAttempts;
	}

	// Operator parameters

	// This optional parameter codepage specifies the code page of the target
	// system using which ustring conversions have to be done for a BytesMessage type.
	// If present, it must have exactly one value that is a String constant. If
	// the parameter is absent, the operator will use the default value of to UTF-8
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
	// file path ../etc/connections.xml (with respect to the data directory)
	private String connectionDocument;
	
	// This optional parameter reconnectionBound specifies the number of
	// successive connections that will be attempted for this operator.
	// It is an optional parameter of type uint32.
	// It can appear only when the reconnectionPolicy parameter is set to
	// BoundedRetry and cannot appear otherwise.If not present the default value
	// is taken to be 5.
	private int reconnectionBound = 5;
	
	// This optional parameter reconnectionPolicy specifies the reconnection
	// policy that would be applicable during initial/intermittent connection failures.
	// The valid values for this parameter are NoRetry, BoundedRetry and InfiniteRetry.
	// If not specified, it is set to BoundedRetry with a reconnectionBound of 5
	// and a period of 60 seconds.
	private ReconnectionPolicies reconnectionPolicy = ReconnectionPolicies.valueOf("BoundedRetry"); //$NON-NLS-1$
	
	// This optional parameter period specifies the time period in seconds which
	// the operator will wait before trying to reconnect.
	// It is an optional parameter of type float64.
	// If not specified, the default value is 60.0. It must appear only when the
	// reconnectionPolicy parameter is specified
	private double period = 60.0;
	
	// This optional parameter maxMessageSendRetries specifies the number of successive 
	// retry that will be attempted for a message in case of failure on message send.
	// Default is 0.
	private int maxMessageSendRetries = 0;

	// This optional parameter messageSendRetryDelay specifies the time to wait before 
	// next delivery attempt. Default is 0 milliseconds
	private long messageSendRetryDelay = 0;

	// Declaring the JMSMEssagehandler,
	private JMSMessageHandlerImpl mhandler;
	
	// Variable to define if the connection attempted to the JMSProvider is the
	// first one.
	private boolean isInitialConnection = true;
	
	// consistent region context
    private ConsistentRegionContext consistentRegionContext;
    
    // CR queue name for storing checkpoint information
    private String consistentRegionQueueName;
    
    // variable to keep track of last successful check point sequence id.
    private long lastSuccessfulCheckpointId = 0;
    
    // unique id to identify messages on CR queue
    private String operatorUniqueID;

	
	// Values to handle access to JMS Header property values
	private List<String> jmsHeaderProperties;
	
	// The broken down JMS Header property / attribute / type triplets
	private List<PropertyAttributeType> patTriplets = null;

	// Flag to signal if the operator accesses JMS Header property values
	private boolean operatorAccessesJMSHeaderPropertyValues = false;
	
	
	// application configuration name
    private String appConfigName;
    
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

    
    
    public boolean isSslConnection() {
		return sslConnection;
	}

    @Parameter(optional = true, description = "This parameter specifies whether the operator should attempt to connect using SSL. If this parameter is specified, then the *keyStore*, *keyStorePassword* and *trustStore* parameters must also be specified. The default value is `false`.")  //$NON-NLS-1$ 
    public void setSslConnection(boolean sslConnection) {
		this.sslConnection = sslConnection;
	}

    @Parameter(optional=true, description="If SSL/TLS protocol debugging is enabled, all protocol data and information is logged to the console. Use this to debug TLS connection problems. The default is 'false'. ")  //$NON-NLS-1$
	public void setSslDebug(boolean sslDebug) {
		this.sslDebug = sslDebug;
	}

	public boolean isSslDebug() {
		return sslDebug;
	}

    // TODO: expressionMode was set to AttributeFree for trustStorePassword
    @Parameter(optional = true, description = "This parameter specifies the password for the trustStore given by the *trustStore* parameter. The *sslConnection* parameter must be set to `true` for this parameter to have any effect.")  //$NON-NLS-1$
    public void setTrustStorePassword(String trustStorePassword) {
		this.trustStorePassword = trustStorePassword;
	}
    
    public String getTrustStorePassword() {
		return trustStorePassword;
	}
    
    public String getTrustStore() {
		return trustStore;
	}
    // TODO: expressionMode was set to AttributeFree for trustStore
    @Parameter(optional = true, description = "This parameter specifies the path to the trustStore. If a relative path is specified, the path is relative to the application directory. The *sslConnection* parameter must be set to true for this parameter to have any effect.")  //$NON-NLS-1$
    public void setTrustStore(String trustStore) {
		this.trustStore = trustStore;
	}
    
    public String getKeyStorePassword() {
		return keyStorePassword;
	}
    
    // TODO: expressionMode was set to AttributeFree for keyStorePassword
    @Parameter(optional = true, description = "This parameter specifies the password for the keyStore given by the *keyStore* parameter. The *sslConnection* parameter must be set to `true` for this parameter to have any effect.")  //$NON-NLS-1$
    public void setKeyStorePassword(String keyStorePassword) {
		this.keyStorePassword = keyStorePassword;
	}
    
    public String getKeyStore() {
		return keyStore;
	}
    
    // TODO: expressionMode was set to AttributeFree for keyStore
    @Parameter(optional = true, description = "This parameter specifies the path to the keyStore. If a relative path is specified, the path is relative to the application directory. The *sslConnection* parameter must be set to true for this parameter to have any effect.")  //$NON-NLS-1$
    public void setKeyStore(String keyStore) {
		this.keyStore = keyStore;
	}
    
	public String getAppConfigName() {
		return appConfigName;
	}
    
	@Parameter(optional = true, description = "This parameter specifies the name of application configuration that stores client credential information, the credential specified via application configuration overrides the one specified in connections file.")  //$NON-NLS-1$
	public void setAppConfigName(String appConfigName) {
		this.appConfigName = appConfigName;
	}

	public String getUserPropName() {
		return userPropName;
	}
    
	@Parameter(optional = true, description = "This parameter specifies the property name of user name in the application configuration. If the appConfigName parameter is specified and the userPropName parameter is not set, a compile time error occurs.")  //$NON-NLS-1$
	public void setUserPropName(String userPropName) {
		this.userPropName = userPropName;
	}

	public String getPasswordPropName() {
		return passwordPropName;
	}
    
	@Parameter(optional = true, description = "This parameter specifies the property name of password in the application configuration. If the appConfigName parameter is specified and the passwordPropName parameter is not set, a compile time error occurs.")  //$NON-NLS-1$
	public void setPasswordPropName(String passwordPropName) {
		this.passwordPropName = passwordPropName;
	}

	@Parameter(optional = true, description = JMSOpDescriptions.CLASS_LIBS)  //$NON-NLS-1$
    public void setClassLibs(List<String> classLibs) {
		this.classLibs = classLibs;
	}
    
    public List<String> getClassLibs() {
		return classLibs;
	}
    
    public String getConsistentRegionQueueName() {
		return consistentRegionQueueName;
	}

	@Parameter(optional = true, description = "This is a required parameter if this operator is participating in a consistent region. This parameter specifies the queue to be used to store consistent region specific information and the operator will perform a JNDI lookup with the queue name specified at initialization state. The queue name specified must also exist on the same messaging server where this operator is establishing the connection.")  //$NON-NLS-1$
	public void setConsistentRegionQueueName(String consistentRegionQueueName) {
		this.consistentRegionQueueName = consistentRegionQueueName;
	}

	@Parameter(optional = false, description = "This mandatory parameter identifies the access specification name.")  //$NON-NLS-1$
	public void setAccess(String access) {
		this.access = access;
	}

	@Parameter(optional = false, description = "This mandatory parameter identifies the name of the connection specification that contains a JMS element.")  //$NON-NLS-1$
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
		"and a **period** of 60 seconds.")  //$NON-NLS-1$
	public void setReconnectionPolicy(String reconnectionPolicy) {
		this.reconnectionPolicy = ReconnectionPolicies
				.valueOf(reconnectionPolicy);
	}

	@Parameter(optional = true, description = "\\n" +  //$NON-NLS-1$
		"This optional parameter of type int32 specifies the number of successive connections that are attempted for an operator.\\n" +  //$NON-NLS-1$
		"You can use this parameter only when the **reconnectionPolicy** parameter is specified and set to `BoundedRetry`,\\n" +  //$NON-NLS-1$
		"otherwise a run time error occurs.\\n" +  //$NON-NLS-1$
		"If the **reconnectionBound** parameter is specified and the **reconnectionPolicy** parameter is not set,\\n" +  //$NON-NLS-1$
		"a compile time error occurs. The default value for the **reconnectionBound** parameter is `5`.\\n")  //$NON-NLS-1$
	public void setReconnectionBound(int reconnectionBound) {
		this.reconnectionBound = reconnectionBound;
	}

	@Parameter(optional = true, description = "\\n" +  //$NON-NLS-1$
		"This parameter specifies the time period in seconds the operator waits before it tries to reconnect.\\n" +  //$NON-NLS-1$
		"It is an optional parameter of type float64. You can use this parameter only when the **reconnectionPolicy** parameter is specified,\\n" +  //$NON-NLS-1$
		"otherwise a compile time error occurs. The default value for the **period** parameter is `60`.\\n")  //$NON-NLS-1$
	public void setPeriod(double period) {
		this.period = period;
	}
	
	@Parameter(optional = true, description = "\\n" +  //$NON-NLS-1$
		"This optional parameter specifies the number of successive retries that are attempted for a message\\n" +  //$NON-NLS-1$
		"if a failure occurs when the message is sent.\\n" +  //$NON-NLS-1$
		"The default value is zero; no retries are attempted.\\n")  //$NON-NLS-1$
	public void setMaxMessageSendRetries(int maxMessageSendRetries) {
		this.maxMessageSendRetries = maxMessageSendRetries;
	}

	@Parameter(optional = true, description = "\\n" +  //$NON-NLS-1$
		"This optional parameter specifies the time in milliseconds to wait before the next delivery attempt.\\n" +  //$NON-NLS-1$
		"If the **maxMessageSendRetries** is specified, you must also specify a value for this parameter.\\n")  //$NON-NLS-1$
	public void setMessageSendRetryDelay(long messageSendRetryDelay) {
		this.messageSendRetryDelay = messageSendRetryDelay;
	}

	@Parameter(optional = true, description = "\\n" +  //$NON-NLS-1$
		"This optional parameter specifies the path name of the file that contains the connection and access specifications,\\n" +  //$NON-NLS-1$
		"which are identified by the connection and access parameters.\\n" +  //$NON-NLS-1$
		"If the parameter is not specified, the operator uses the file that is in the default location `../etc/connections.xml`.\\n")  //$NON-NLS-1$
	public void setConnectionDocument(String connectionDocument) {
		this.connectionDocument = connectionDocument;
	}

    public List<String> getJmsHeaderProperties() {
        return jmsHeaderProperties;
    }

    @Parameter(optional = true, description = JMSOpDescriptions.JMS_HEADER_PROPERTIES_DESC)
    public void setJmsHeaderProperties(List<String> jmsHeaderProperties) {
        this.jmsHeaderProperties = jmsHeaderProperties;
    }
    
	public String getConnectionDocument() {
	
		if (connectionDocument == null)
		{
			connectionDocument = getOperatorContext().getPE().getApplicationDirectory() + "/etc/connections.xml"; //$NON-NLS-1$
		}
		
		if (!connectionDocument.startsWith("/")) //$NON-NLS-1$
		{
			connectionDocument = getOperatorContext().getPE().getApplicationDirectory() + File.separator + connectionDocument;
		}
		
		return connectionDocument;
	}

	// Add the context checks

	/*
	 * The method checkErrorOutputPort validates that the stream on error output
	 * port contains the optional attribute of type which is the incoming tuple,
	 * and an rstring which will contain the error message in order.
	 */
	@ContextCheck
	public static void checkErrorOutputPort(OperatorContextChecker checker) {
		OperatorContext context = checker.getOperatorContext();
		// Check if the operator has an error port defined
		if (context.getNumberOfStreamingOutputs() == 1) {
			StreamingOutput<OutputTuple> streamingOutputPort = context
					.getStreamingOutputs().get(0);
			// The optional error output port can have no more than two
			// attributes.
			if (streamingOutputPort.getStreamSchema().getAttributeCount() > 2) {
				logger.log(LogLevel.ERROR, "ATMOST_TWO_ATTR"); //$NON-NLS-1$

			}
			// The optional error output port must have at least one attribute.
			if (streamingOutputPort.getStreamSchema().getAttributeCount() < 1) {
				logger.log(LogLevel.ERROR, "ATLEAST_ONE_ATTR"); //$NON-NLS-1$

			}
			// If only one attribute is specified, that attribute in the
			// optional error output port must be an rstring.
			if (streamingOutputPort.getStreamSchema().getAttributeCount() == 1) {
				if (streamingOutputPort.getStreamSchema().getAttribute(0)
						.getType().getMetaType() != Type.MetaType.RSTRING) {
					logger.log(LogLevel.ERROR, "ERROR_PORT_FIRST_ATTR_RSTRING"); //$NON-NLS-1$
				}
			}
			// If two attributes are specified, the first attribute in the
			// optional error output port must be a tuple.
			// and the second attribute must be rstring.
			if (streamingOutputPort.getStreamSchema().getAttributeCount() == 2) {
				if (streamingOutputPort.getStreamSchema().getAttribute(0)
						.getType().getMetaType() != Type.MetaType.TUPLE) {
					logger.log(LogLevel.ERROR, "ERROR_PORT_FIRST_ATTR_TUPLE"); //$NON-NLS-1$

				}
				if (streamingOutputPort.getStreamSchema().getAttribute(1)
						.getType().getMetaType() != Type.MetaType.RSTRING) {
					logger.log(LogLevel.ERROR, "ERROR_PORT_SECOND_ATTR_RSTRING"); //$NON-NLS-1$

				}
			}
		}
	}
	
	@ContextCheck(compile = true, runtime = false)
	public static void checkCompileTimeConsistentRegion(OperatorContextChecker checker) {
		ConsistentRegionContext consistentRegionContext = checker.getOperatorContext().getOptionalContext(ConsistentRegionContext.class);
		OperatorContext context = checker.getOperatorContext();
		
		if(consistentRegionContext != null) {
			
			if(consistentRegionContext.isStartOfRegion()) {
				checker.setInvalidContext(Messages.getString("OP_CANNOT_BE_START_OF_CONSISTENT_REGION"), new String[] {"JMSSink"}); //$NON-NLS-1$ //$NON-NLS-2$
			}
			
			if(!context.getParameterNames().contains("consistentRegionQueueName")) { //$NON-NLS-1$
				checker.setInvalidContext(Messages.getString("CONSISTENTREGIONQUEUENAME_MUST_BE_SET_WHEN_PARTICIPATING_IN_CONSISTENT_REGION"), new String[] {}); //$NON-NLS-1$
			}
			
			if(context.getParameterNames().contains("reconnectionPolicy") || //$NON-NLS-1$
			   context.getParameterNames().contains("reconnectionBound") ||  //$NON-NLS-1$
			   context.getParameterNames().contains("period") ||  //$NON-NLS-1$
			   context.getParameterNames().contains("maxMessageSendRetries") ||  //$NON-NLS-1$
			   context.getParameterNames().contains("messageSendRetryDelay")) { //$NON-NLS-1$
				
				checker.setInvalidContext(Messages.getString("PARAMS_NOT_ALLOWED_WHEN_PARTICIPATING_IN_CONSISTENT_REGION"), new String[] {}); //$NON-NLS-1$
			}
		}
		else {
			
			if(context.getParameterNames().contains("consistentRegionQueueName")) { //$NON-NLS-1$
				checker.setInvalidContext(Messages.getString("CONSISTENTREGIONQUEUENAME_CANNOT_BE_SPECIFIED_WHEN_NOT_PARTICIPATING_IN_CONSISTENT_REGION"), new String[] {}); //$NON-NLS-1$
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

		if ((context.getParameterNames().contains("reconnectionBound"))) { //$NON-NLS-1$
			// reconnectionBound value should be non negative.
			if (Integer.parseInt(context
					.getParameterValues("reconnectionBound").get(0)) < 0) { //$NON-NLS-1$

				logger.log(LogLevel.ERROR, "REC_BOUND_NEG"); //$NON-NLS-1$
				checker.setInvalidContext(
						Messages.getString("PARAM_VALUE_SHOULD_BE_GREATER_OR_EQUAL_TO_ZERO"), //$NON-NLS-1$
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
					logger.log(LogLevel.ERROR, "REC_BOUND_NOT_ALLOWED"); //$NON-NLS-1$
					checker.setInvalidContext(
							Messages.getString("RECONNECTIONBOUND_CAN_APPEAR_ONLY_WHEN_RECONNECTIONPOLICY_IS_BOUNDEDRETRY"), //$NON-NLS-1$
							new String[] { context.getParameterValues("reconnectionBound").get(0) }); //$NON-NLS-1$

				}
			}

		}
		
		// maxMessageSendRetries can not be negative number if present
		if(context.getParameterNames().contains("maxMessageSendRetries")) { //$NON-NLS-1$
			if(Integer.parseInt(context.getParameterValues("maxMessageSendRetries").get(0)) < 0) { //$NON-NLS-1$
				logger.log(LogLevel.ERROR, "MESSAGE_RESEND_NEG", new Object[] {"maxMessageSendRetries"}); //$NON-NLS-1$ //$NON-NLS-2$
				checker.setInvalidContext(
						Messages.getString("PARAM_VALUE_SHOULD_BE_GREATER_OR_EQUAL_TO_ZERO"),  //$NON-NLS-1$
						new String[] {"maxMessageSendRetries", context.getParameterValues("maxMessageSendRetries").get(0)}); //$NON-NLS-1$
			}
			
		}
		
		// messageSendRetryDelay can not be negative number if present
		if(context.getParameterNames().contains("messageSendRetryDelay")) { //$NON-NLS-1$
			if(Long.parseLong(context.getParameterValues("messageSendRetryDelay").get(0)) < 0) { //$NON-NLS-1$
				logger.log(LogLevel.ERROR, "MESSAGE_RESEND_NEG", new Object[] {"messageSendRetryDelay"}); //$NON-NLS-1$ //$NON-NLS-2$
				checker.setInvalidContext(
						Messages.getString("PARAM_VALUE_SHOULD_BE_GREATER_OR_EQUAL_TO_ZERO"),  //$NON-NLS-1$
						new String[] {"messageSendRetryDelay", context.getParameterValues("messageSendRetryDelay").get(0)}); //$NON-NLS-1$
			}
		}
		
		// consistentRegionQueueName must not be null if present
		if(context.getParameterNames().contains("consistentRegionQueueName")) { //$NON-NLS-1$
			
			String consistentRegionQueueName = context.getParameterValues("consistentRegionQueueName").get(0); //$NON-NLS-1$
			if(consistentRegionQueueName == null || consistentRegionQueueName.trim().length() == 0) {
				logger.log(LogLevel.ERROR, "CONSISTENTREGIONQUEUENAME_VALUE_MUST_BE_NON_EMPTY"); //$NON-NLS-1$
				checker.setInvalidContext(
						Messages.getString("CONSISTENTREGIONQUEUENAME_VALUE_MUST_BE_NON_EMPTY"),  //$NON-NLS-1$
						new String[] {});
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
				logger.log(LogLevel.ERROR, "PROPERTY_NOT_FOUND_IN_APP_CONFIG", new String[] {passwordPropName, appConfigName} ); //$NON-NLS-1$
				checker.setInvalidContext(
						Messages.getString("PROPERTY_NOT_FOUND_IN_APP_CONFIG"), //$NON-NLS-1$
						new Object[] {passwordPropName, appConfigName});
			
			}
        }
		
		tracer.log(TraceLevel.TRACE, "End checkParametersRuntime()"); //$NON-NLS-1$
	}

	// add compile time check for either period or reconnectionBound to be
	// present only when reconnectionPolicy is present
	// and messageRetryDelay to be present only when maxMessageRetriesis present
	@ContextCheck(compile = true)
	public static void checkParameters(OperatorContextChecker checker) {
		ConsistentRegionContext consistentRegionContext = checker.getOperatorContext().getOptionalContext(ConsistentRegionContext.class);
		
		if(consistentRegionContext == null) {
			checker.checkDependentParameters("period", "reconnectionPolicy"); //$NON-NLS-1$ //$NON-NLS-2$
			checker.checkDependentParameters("reconnectionBound", //$NON-NLS-1$
					"reconnectionPolicy"); //$NON-NLS-1$
			
			checker.checkDependentParameters("maxMessageSendRetries", "messageSendRetryDelay"); //$NON-NLS-1$ //$NON-NLS-2$
			checker.checkDependentParameters("messageSendRetryDelay", "maxMessageSendRetries"); //$NON-NLS-1$ //$NON-NLS-2$
		}
		
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
		
		consistentRegionContext = context.getOptionalContext(ConsistentRegionContext.class);
		
		operatorUniqueID = context.getPE().getDomainId() + "_" + context.getPE().getInstanceId() + "_" + context.getPE().getJobId() + "_" + context.getName(); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		String msgSelectorCR = (consistentRegionContext == null) ? null : JMSSink.OP_CKP_NAME_PROPERTITY + "=" + "'" + operatorUniqueID + "'"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		
		/*
		 * Set appropriate variables if the optional error output port is
		 * specified. Also set errorOutputPort to the output port at index 0
		 */
		if (context.getNumberOfStreamingOutputs() == 1) {
			hasErrorPort = true;
			errorOutputPort = getOutput(0);
			if (errorOutputPort.getStreamSchema().getAttributeCount() == 2) {
				embeddedSchema = errorOutputPort.newTuple().getTuple(0)
						.getStreamSchema();
				hasOptionalTupleInErrorPort = true;
			}
		}

		// check if connections file is valid
		StreamSchema streamSchema = getInput(0).getStreamSchema();

		ConnectionDocumentParser connectionDocumentParser = new ConnectionDocumentParser();

		connectionDocumentParser.parseAndValidateConnectionDocument(
				getConnectionDocument(), connection, access, streamSchema, true, context.getPE().getApplicationDirectory());

		// codepage parameter can come only if message class is bytes
		// Since the message class is extracted runtime during the parsing of
		// connections document in initialize function , we need to keep this
		// check here
		if (connectionDocumentParser.getMessageType() != MessageClass.bytes
				&& context.getParameterNames().contains("codepage")) { //$NON-NLS-1$
			throw new ParseConnectionDocumentException(
					Messages.getString("CODEPAGE_APPEARS_ONLY_WHEN_MSG_CLASS_IS_BYTES")); //$NON-NLS-1$
		}
		
		PropertyProvider propertyProvider = null;
		
		if(getAppConfigName() != null) {
			propertyProvider = new PropertyProvider(context.getPE(), getAppConfigName());
		}

		// parsing connection document is successful, we can go ahead and create
		// connection
		// The jmsConnectionHelper will throw a runtime error and abort the
		// application in case of errors.
		jmsConnectionHelper = new JMSConnectionHelper(connectionDocumentParser, reconnectionPolicy,
				reconnectionBound, period, true, maxMessageSendRetries, 
				messageSendRetryDelay, nReconnectionAttempts, nFailedInserts, logger, (consistentRegionContext != null), 
				msgSelectorCR, propertyProvider, userPropName, passwordPropName, getConsistentRegionQueueName());
		
		// Initialize JMS connection if operator is in a consistent region.
		// When this operator is in a consistent region, a transacted session is used,
		// So the connection/message send retry logic does not apply, here the noRetry version
		// of the connection initialization method is used, thus it makes sure to do it in Initial method than in process method.
		if(consistentRegionContext != null) {
			jmsConnectionHelper.createInitialConnectionNoRetry();
		}

		// Create the appropriate JMS message handlers as specified by the
		// messageType.
		switch (connectionDocumentParser.getMessageType()) {
		case map:
			mhandler = new MapMessageHandler(
					connectionDocumentParser.getNativeSchemaObjects(),
					nTruncatedInserts);

			break;
		case stream:
			mhandler = new StreamMessageHandler(
					connectionDocumentParser.getNativeSchemaObjects(),
					nTruncatedInserts);

			break;
		case bytes:
			mhandler = new BytesMessageHandler(
					connectionDocumentParser.getNativeSchemaObjects(),
					codepage, nTruncatedInserts);

			break;
		case empty:
			mhandler = new EmptyMessageHandler(
					connectionDocumentParser.getNativeSchemaObjects());

			break;
		case wbe:
			mhandler = new WBETextMessageHandler(
					connectionDocumentParser.getNativeSchemaObjects(),
					getInput(0).getName(), nTruncatedInserts);

			break;
		case wbe22:
			mhandler = new WBE22TextMessageHandler(
					connectionDocumentParser.getNativeSchemaObjects(),
					getInput(0).getName(), nTruncatedInserts);

			break;
		case xml:
			mhandler = new XMLTextMessageHandler(
					connectionDocumentParser.getNativeSchemaObjects(),
					getInput(0).getName(), nTruncatedInserts);

			break;
		case text:
			mhandler = new TextMessageHandler(connectionDocumentParser.getNativeSchemaObjects());
			break;
		default:
			throw new RuntimeException(Messages.getString("NO_VALID_MSG_CLASS_SPECIFIED")); //$NON-NLS-1$
		}

		// register for data governance
		registerForDataGovernance(connectionDocumentParser.getProviderURL(), connectionDocumentParser.getDestination());

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
		logger.log(TraceLevel.INFO, "JMSSink - Registering for data governance with providerURL: " + providerURL //$NON-NLS-1$
				+ " destination: " + destination); //$NON-NLS-1$
		DataGovernanceUtil.registerForDataGovernance(this, destination, IGovernanceConstants.ASSET_JMS_MESSAGE_TYPE,
				providerURL, IGovernanceConstants.ASSET_JMS_SERVER_TYPE, false, "JMSSink"); //$NON-NLS-1$
	}

	@Override
	public void process(StreamingInput<Tuple> stream, Tuple tuple) throws Exception {
		
		if(consistentRegionContext == null) {
		    boolean msgSent = false;
			// Operator is not in a consistent region
		    if (isInitialConnection) {
		        jmsConnectionHelper.createInitialConnection();	
		        isInitialConnection = false;
		    }	
		    
            Message message;
            try {
                // Construct the JMS message based on the message type taking the
                // attributes from the tuple. If the session is closed, we will be thrown out by JMSException.
                message = mhandler.convertTupleToMessage(tuple, jmsConnectionHelper.getSession());
                writeJmsHeaderPropertyValuesIntoJmsMessage(tuple, message);
                msgSent = jmsConnectionHelper.sendMessage(message);
            }
            catch (UnsupportedEncodingException | ParserConfigurationException | TransformerException e) {
                tracer.log (LogLevel.ERROR, "Failure creating JMS message from input tuple: " + e.toString()); //$NON-NLS-1$
                // no further action; tuple is dropped and sent to error port if present
            }
            catch (/*JMS*/Exception e) {
                tracer.log (LogLevel.ERROR, "failure creating or sending JMS message: " + e.toString()
                        + ". Trying to reconnect and re-send message"); //$NON-NLS-1$
                try {
                    isInitialConnection = true;
                    jmsConnectionHelper.closeConnection();
                    jmsConnectionHelper.createInitialConnection();
                    isInitialConnection = false;
                    message = mhandler.convertTupleToMessage(tuple, jmsConnectionHelper.getSession());
                    msgSent = jmsConnectionHelper.sendMessage(message);
                } catch (Exception finalExc) {
                    tracer.log (LogLevel.ERROR, "Tuple dropped. Final failure re-sending tuple: " + finalExc.toString()); //$NON-NLS-1$
                    // no further action; tuple is dropped and sent to error port if present
                }
            }
            if (!msgSent) {
                if (tracer.isLoggable(LogLevel.FINE)) tracer.log(LogLevel.FINE, "tuple dropped"); //$NON-NLS-1$
                logger.log(LogLevel.ERROR, "EXCEPTION_SINK"); //$NON-NLS-1$
                if (hasErrorPort) {
                    // throws Exception
                    sendOutputErrorMsg(tuple,
                            Messages.getString("EXCEPTION_SINK")); //$NON-NLS-1$
                }
            }
		}
		else {
		    // consistent region
		    // Construct the JMS message based on the message type taking the
		    // attributes from the tuple.
		    // propagate all exceptions to the runtime to restart the consistent region in case of failure
		    Message message = mhandler.convertTupleToMessage(tuple,
		            jmsConnectionHelper.getSession());
			jmsConnectionHelper.sendMessageNoRetry(message);
		}
	}



	/**
	 * Reads JMS Header property values from the current
	 * input tuple and stores them in property values of the
	 * current JMS message according to the mapping defined
	 * via 'jmsHeaderProperties' parameter.
	 *  
	 * @param msg		The current JMS message.
	 * @param tuple		The output tuple.
	 */
	private void writeJmsHeaderPropertyValuesIntoJmsMessage(Tuple tuple, Message msg) {
		
		// If we do not access property values, return
		if(! operatorAccessesJMSHeaderPropertyValues ) return;
		
		for(PropertyAttributeType pat : patTriplets) {
			
			PropertyType	typeSpec	= pat.getTypeSpecification();
			String			propName	= pat.getPropertyName();
			int				attrIdx		= pat.getAttributeIdx();
	
			try {
				switch (typeSpec) {
				case BOOL:
					{
						boolean value = tuple.getBoolean(attrIdx);
						msg.setBooleanProperty(propName, value);
					}
					break;
				case BYTE:
					{
						byte value = tuple.getByte(attrIdx);
						msg.setByteProperty(propName, value);
					}
					break;
				case SHORT:
					{
						short value = tuple.getShort(attrIdx);
						msg.setShortProperty(propName, value);
					}
					break;
				case INT:
					{
						int value = tuple.getInt(attrIdx);
						msg.setIntProperty(propName, value);
					}
					break;
				case LONG:
					{
						long value = tuple.getLong(attrIdx);
						msg.setLongProperty(propName, value);
					}
					break;
				case FLOAT:
					{
						float value = tuple.getFloat(attrIdx);
						msg.setFloatProperty(propName, value);
					}
					break;
				case DOUBLE:
					{
						double value = tuple.getDouble(attrIdx);
						msg.setDoubleProperty(propName, value);
					}
					break;
				case STRING:
					{
						String value = tuple.getString(attrIdx);
						msg.setStringProperty(propName, value);
					}
					break;
				case OBJECT:
					{
						Object value = tuple.getObject(attrIdx);
						msg.setObjectProperty(propName, value);
					}
					break;
				}
				
			}
			catch (JMSException e) {
				String errMsg = Messages.getString("ERROR_WHILE_WRITING_JMSHEADERPROPS", e.getMessage());	//$NON-NLS-1$
				logger.log(LogLevel.ERROR, errMsg);
			}

		}

	}

	// Method to send the error message to the error output port if one is
	// specified
	private void sendOutputErrorMsg(Tuple tuple, String errorMessage)
			throws Exception {

		OutputTuple errorTuple = errorOutputPort.newTuple();
		String consolidatedErrorMessage = errorMessage;
		// If the error output port specifies the optional input tuple, populate
		// its elements from the input tuple which caused this error.
		if (hasOptionalTupleInErrorPort) {
			Tuple embeddedTuple = embeddedSchema.getTuple(tuple);
			errorTuple.setTuple(0, embeddedTuple);
			errorTuple.setString(1, consolidatedErrorMessage);
		} else {
			// Set only the error message
			errorTuple.setString(0, consolidatedErrorMessage);
		}
		// submit the tuple.
		errorOutputPort.submit(errorTuple);

	}

	@Override
	public void shutdown() throws Exception {
		// close the connection.
		super.shutdown();
		jmsConnectionHelper.closeConnection();
	}

	@Override
	public void close() throws IOException {
		logger.log(LogLevel.INFO, "STATEHANDLER_CLOSE"); //$NON-NLS-1$
	}

	@Override
	public void checkpoint(Checkpoint checkpoint) throws Exception {
		logger.log(LogLevel.INFO, "CHECKPOINT", checkpoint.getSequenceId()); //$NON-NLS-1$
		
		long currentCheckpointId = checkpoint.getSequenceId();
		long committedCheckpointId = 0;
		boolean commit = true;
		
		
		// The entire checkpoint should fail if any of the JMS operation fails.
		// For example, if connection drops at receiveMssage, this means the tuples sent
		// before current checkpoint are lost because we are using transacted session for CR
		// It is not necessary to try to establish the connection again,
		// any JMSException received should be propagated back to the CR framework.
			
		// retrieve a message from CR queue
		Message crMsg = jmsConnectionHelper.receiveCRMessage(500);

		// No checkpoint message yet, it may happen for the first checkpoint
		if(crMsg != null) {
		    committedCheckpointId = crMsg.getLongProperty(JMSSink.CKP_ID_PROPERTITY);
		}
			
		// Something is wrong here as committedCheckpointId should be greater than 1 when a successful checkpoint has been made
		if(committedCheckpointId == 0 && lastSuccessfulCheckpointId > 0) {
			logger.log(LogLevel.ERROR, "CHECKPOINT_CANNOT_PROCEED_MISSING_CHECKPOINT_MSG"); //$NON-NLS-1$
		    throw new Exception(Messages.getString("CHECKPOINT_CANNOT_PROCEED_MISSING_CHECKPOINT_MSG")); //$NON-NLS-1$
		}
			
		if((currentCheckpointId - lastSuccessfulCheckpointId > 1) &&
		   (lastSuccessfulCheckpointId < committedCheckpointId)) {
			// this transaction has been processed before, and this is a duplicate
			// discard this transaction. 
			logger.log(LogLevel.INFO, "DISCARD_TRANSACTION_AS_IT_HAS_BEEN_PROCESSED_LAST_TIME"); //$NON-NLS-1$
		    commit = false;
				
		}
			
		if(commit) {
			jmsConnectionHelper.sendCRMessage(createCheckpointMsg(currentCheckpointId));
			jmsConnectionHelper.commitSession();
		}
		else {
			jmsConnectionHelper.roolbackSession();
		}
			
		lastSuccessfulCheckpointId = currentCheckpointId;
			
	}
	
	private Message createCheckpointMsg(long currentCheckpointId) throws Exception {
		
		Message message;
		message = jmsConnectionHelper.getSession().createMessage();
		message.setStringProperty(JMSSink.OP_CKP_NAME_PROPERTITY, operatorUniqueID);
		message.setLongProperty(JMSSink.CKP_ID_PROPERTITY, currentCheckpointId);
		
		return message;
	}

	@Override
	public void drain() throws Exception {
		logger.log(LogLevel.INFO, "DRAIN"); //$NON-NLS-1$
	}

	@Override
	public void reset(Checkpoint checkpoint) throws Exception {
		logger.log(LogLevel.INFO, "RESET_CHECKPOINT", checkpoint.getSequenceId()); //$NON-NLS-1$
	
		lastSuccessfulCheckpointId = checkpoint.getSequenceId();
		jmsConnectionHelper.roolbackSession();
		
	}

	@Override
	public void resetToInitialState() throws Exception {
		logger.log(LogLevel.INFO, "RESET_TO_INITIAL_STATE"); //$NON-NLS-1$
		
		lastSuccessfulCheckpointId = 0;
		jmsConnectionHelper.roolbackSession();
	}

	@Override
	public void retireCheckpoint(long id) throws Exception {
		logger.log(LogLevel.INFO, "RETIRE_CHECKPOINT", id);		 //$NON-NLS-1$
	}


	public static final String DESC = "\\n" +  //$NON-NLS-1$
"The `JMSSink` operator creates messages from InfoSphere Streams tuples\\n" +  //$NON-NLS-1$
"and writes the messages to a WebSphere MQ or an Apache Active MQ queue or topic.\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"The incoming tuple from InfoSphere Streams can contain one or more attributes, each of which can be of the following data types:\\n" +  //$NON-NLS-1$
"int8, uint8, int16, uint16, int32, uint32, int64, uint64, float32, float64, boolean, blob, rstring, decimal32,\\n" +  //$NON-NLS-1$
"decimal64, decimal128, ustring, or timestamp. The input tuple is serialized into a JMS message.\\n" +  //$NON-NLS-1$
"For the `JMSSink` operator, the following message classes are supported:\\n" +  //$NON-NLS-1$
"map, stream, bytes, xml, wbe, wbe22, and empty.\\n" +  //$NON-NLS-1$
"The type of message is specified as the value of the message_class attribute in the connection specifications document.\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"# SSL Support\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"The `JMSSink` operator provides support for SSL via these parameters: *sslConnection*, *keyStore*, *keyStorePassword* and *trustStore*.\\n" +  //$NON-NLS-1$
"When *sslConnection* is set to `true`, the *keyStore*, *keyStorePassword* and *trustStore* parameters must be set. \\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"**Note:** The `JMSSink` operator configures SSL by setting the JVM system properties via calls to `System.property()`. \\n" +  //$NON-NLS-1$
"Java operators that are fused into the same PE share the same JVM. This implies that any other Java operators fused into the \\n" +  //$NON-NLS-1$
"same PE as the `JMSSink` operator will have these SSL properties set. If this is undesirable, then the `JMSSink` operator\\n" +  //$NON-NLS-1$
"should be placed into it's own PE. \\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"# Behavior in a consistent region\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"The `JMSSink` operator can be an operator within the reachability graph of a operator-driven consistent region.\\n" +  //$NON-NLS-1$
"It cannot be the start of a consistent region.\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"When this operator is participating in a consistent region, the parameter **consistentRegionQueueName** must be specified.\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"`JMSSink` operator is using transacted session to achieve the purpose of consistent region and parameters **reconnectionPolicy**, **reconnectionBound**, **period**, **maxMessageSendRetries** and **messageSendRetryDelay** must not be specified.\\n" +  //$NON-NLS-1$
"This is due to the fact that if connection problem occurs, the messages sent since the last successful checkpoint would be lost and it is not necessary to continue to process message even after connection is established again.\\n" +  //$NON-NLS-1$
"In this case the operator needs to reset to the last checkpoint and re-send the messages again, thus re-connection policy would not apply when it is participating in a consistent region\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"# Exceptions\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"The following list describes the common types of exceptions that can occur:\\n" +  //$NON-NLS-1$
" * Run time errors that halt the operator execution.\\n" +  //$NON-NLS-1$
"  * The `JMSSink` operator throws an exception and terminates in the following cases.\\n" +  //$NON-NLS-1$
"    For some exceptions, the trace and log information is logged in the console logs\\n" +  //$NON-NLS-1$
"    and also output to the optional output port if the application is configured to use the optional port.\\n" +  //$NON-NLS-1$
"    * During the initial connection attempt or during transient reconnection failures,\\n" +  //$NON-NLS-1$
"      if the **reconnectionPolicy** is set to `NoRetry` and the operator does not have a successful connection,\\n" +  //$NON-NLS-1$
"      or the **reconnectionPolicy** is set to `BoundedRetry` and the operator does not have a successful connection\\n" +  //$NON-NLS-1$
"      after the number of attempts that are specified in the **reconnectionBound** parameter. Successive data is lost.\\n" +  //$NON-NLS-1$
"    * The queue name is unknown.\\n" +  //$NON-NLS-1$
"    * The queue manager name is unknown.\\n" +  //$NON-NLS-1$
"    * The operator is unable to connect to the host.\\n" +  //$NON-NLS-1$
"    * The operator is unable to connect o the port.\\n" +  //$NON-NLS-1$
"    * There is a mismatch between the data type of one or more attributes in the native schema\\n" +  //$NON-NLS-1$
"      and the data type of attributes in the input stream.\\n" +  //$NON-NLS-1$
"    * One or more native schema attributes do not have a matching attribute in the input stream schema.\\n" +  //$NON-NLS-1$
"    * The **connectionDocument** parameter refers to an file that does not exist.\\n" +  //$NON-NLS-1$
"    * The **connectionDocument** parameter is not specified and the `connections.xml` file is not present in the default location.\\n" +  //$NON-NLS-1$
"    * An invalid value is specified for the message_class attribute of the access specification.\\n" +  //$NON-NLS-1$
"    * A negative length is specified for a string or blob data types in the native schema for a map, stream, xml, wbe, or wbe22 message class.\\n" +  //$NON-NLS-1$
"    * A negative length other than -2 or -4 is specified for a string/blob data type in the native_schema for a bytes message class.\\n" +  //$NON-NLS-1$
"    * The message_class attribute is empty, but the &lt;native_schema> element is not empty.\\n" +  //$NON-NLS-1$
"    * The **jmsHeaderProperties** parameter is specified but the contained configuration is erroneous.\\n" +  //$NON-NLS-1$
" * Run time errors that cause a message to be dropped and an error message to be logged.\\n" +  //$NON-NLS-1$
"  * The `JMSink` operator throws an exception and discards the message in the following cases.\\n" +  //$NON-NLS-1$
"    The trace and log information for these exceptions is logged in the console logs\\n" +  //$NON-NLS-1$
"    and also output to the optional output port if the application is configured to use the optional port.\\n" +  //$NON-NLS-1$
"    *  The data being written is longer than the maximum message length specified in the queue.\\n" +  //$NON-NLS-1$
"       The discarded message is not sent to the WebSphere MQ or Apache Active MQ queue or topic.\\n" +  //$NON-NLS-1$
"    * The **reconnectionBound** parameter is specified, but the **reconnectionPolicy** parameter\\n" +  //$NON-NLS-1$
"      is set to a value other than `BoundedRetry`.\\n" +  //$NON-NLS-1$
" * Compile time errors.\\n" +  //$NON-NLS-1$
"  * The `JMSink` operator throws a compile time error in the following cases.\\n" +  //$NON-NLS-1$
"    The trace and log information for these exceptions is logged in the console logs\\n" +  //$NON-NLS-1$
"    and also output to the optional output port if the application is configured to use the optional port.\\n" +  //$NON-NLS-1$
"    * The mandatory parameters, connection and access are not specified.\\n" +  //$NON-NLS-1$
"    * The **period** parameter is specified, but the **reconnectionPolicy** parameter is not specified.\\n" +  //$NON-NLS-1$
"    * The **reconnectionBound** parameter is specified, but the **reconnectionPolicy** parameter is not specified.\\n" +  //$NON-NLS-1$
"    * The environment variables **STREAMS_MESSAGING_WMQ_HOME** and **STREAMS_MESSAGING_AMQ_HOME**\\n" +  //$NON-NLS-1$
"      are not set to the locations where the WMQ and AMQ libraries are installed.\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"+ Examples\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"This example show the use of multiple JMSSink operators with different parameter combinations.\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"	composite Main {\\n" +  //$NON-NLS-1$
"	graph\\n" +  //$NON-NLS-1$
"	\\n" +  //$NON-NLS-1$
"	stream &lt;int32 id, rstring fname, rstring lname>\\n" +  //$NON-NLS-1$
"	MyPersonNamesStream  = Beacon()\\n" +  //$NON-NLS-1$
"	{\\n" +  //$NON-NLS-1$
"	param\\n" +  //$NON-NLS-1$
"		iterations:10u;\\n" +  //$NON-NLS-1$
"	}\\n" +  //$NON-NLS-1$
"	\\n" +  //$NON-NLS-1$
"	// JMSSink operator with connections document in the default directory ../etc/connections.xml\\n" +  //$NON-NLS-1$
"	// (relative to the data directory)\\n" +  //$NON-NLS-1$
"	() as  MySink1 = JMSSink( MyPersonNamesStream )\\n" +  //$NON-NLS-1$
"	{\\n" +  //$NON-NLS-1$
"	param\\n" +  //$NON-NLS-1$
"		connection : &quot;amqConn&quot;;	\\n" +  //$NON-NLS-1$
"		access     : &quot;amqAccess&quot;;\\n" +  //$NON-NLS-1$
"	}\\n" +  //$NON-NLS-1$
"	\\n" +  //$NON-NLS-1$
"	// JMSSink operator with fully qualified name of connections document\\n" +  //$NON-NLS-1$
"	() as  MySink2 = JMSSink( MyPersonNamesStream )\\n" +  //$NON-NLS-1$
"	{\\n" +  //$NON-NLS-1$
"	param\\n" +  //$NON-NLS-1$
"		connectionDocument   : &quot;/home/streamsuser/connections/JMSconnections.xml&quot;;\\n" +  //$NON-NLS-1$
"		connection           : &quot;amqConn&quot;;\\n" +  //$NON-NLS-1$
"		access               : &quot;amqAccess&quot;;\\n" +  //$NON-NLS-1$
"	}\\n" +  //$NON-NLS-1$
"	\\n" +  //$NON-NLS-1$
"	// JMSSink operator with optional output error port specified\\n" +  //$NON-NLS-1$
"	stream &lt;tuple&lt; int32 id, rstring fname, rstring lname> inTuple, rstring errorMessage>\\n" +  //$NON-NLS-1$
"	MySink3  = JMSSink(MyPersonNamesStream )\\n" +  //$NON-NLS-1$
"	{\\n" +  //$NON-NLS-1$
"	param\\n" +  //$NON-NLS-1$
"		connection : &quot;amqConn&quot;;	\\n" +  //$NON-NLS-1$
"		access     : &quot;amqAccess&quot;;\\n" +  //$NON-NLS-1$
"	}\\n" +  //$NON-NLS-1$
"	// JMSSink operator with reconnectionPolicy specified as  NoRetry\\n" +  //$NON-NLS-1$
"	() as  MySink4 = JMSSink( MyPersonNamesStream )\\n" +  //$NON-NLS-1$
"	{\\n" +  //$NON-NLS-1$
"	param\\n" +  //$NON-NLS-1$
"		connection         : &quot;amqConn&quot;;\\n" +  //$NON-NLS-1$
"		access             : &quot;amqAccess&quot;;\\n" +  //$NON-NLS-1$
"		reconnectionPolicy : &quot;NoRetry&quot;;\\n" +  //$NON-NLS-1$
"	}\\n" +  //$NON-NLS-1$
"	\\n" +  //$NON-NLS-1$
"	// JMSSink operator with optional period and reconnectionPolicy specified\\n" +  //$NON-NLS-1$
"	() as  MySink5 = JMSSink( MyPersonNamesStream )\\n" +  //$NON-NLS-1$
"	{\\n" +  //$NON-NLS-1$
"	param\\n" +  //$NON-NLS-1$
"		connection: &quot;amqConn&quot;;	\\n" +  //$NON-NLS-1$
"		access: &quot;amqAccess&quot;;\\n" +  //$NON-NLS-1$
"		reconnectionPolicy : &quot;InfiniteRetry&quot;;\\n" +  //$NON-NLS-1$
"		period: 1.20;\\n" +  //$NON-NLS-1$
"	}\\n" +  //$NON-NLS-1$
"	// JMSSink operator with reconnectionPolicy specified as BoundedRetry\\n" +  //$NON-NLS-1$
"	() as  MySink6 = JMSSink( MyPersonNamesStream )\\n" +  //$NON-NLS-1$
"	{\\n" +  //$NON-NLS-1$
"	param\\n" +  //$NON-NLS-1$
"		connection: &quot;amqConn&quot;;	\\n" +  //$NON-NLS-1$
"		access: &quot;amqAccess&quot;; \\n" +  //$NON-NLS-1$
"		reconnectionPolicy : &quot;BoundedRetry&quot;;\\n" +  //$NON-NLS-1$
"		reconnectionBound : 2;\\n" +  //$NON-NLS-1$
"		period: 1.20;\\n" +  //$NON-NLS-1$
"	}\\n" +  //$NON-NLS-1$
"	}\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"The following example shows a sample connections.xml file:\\n" +  //$NON-NLS-1$
"\\n" +  //$NON-NLS-1$
"	&lt;st:connections 	xmlns:st=&quot;http://www.ibm.com/xmlns/prod/streams/adapters&quot;\\n" +  //$NON-NLS-1$
"						xmlns:xsi=&quot;http://www.w3.org/2001/XMLSchema-instance&quot;>\\n" +  //$NON-NLS-1$
"	\\n" +  //$NON-NLS-1$
"	&lt;connection_specifications>\\n" +  //$NON-NLS-1$
"		&lt;connection_specification name=&quot;amqConn&quot;>\\n" +  //$NON-NLS-1$
"		 &lt;JMS initial_context=&quot;org.apache.activemq.jndi.ActiveMQInitialContextFactory&quot;\\n" +  //$NON-NLS-1$
"			provider_url = &quot;tcp://machine1.com:61616&quot;\\n" +  //$NON-NLS-1$
"			connection_factory=&quot;ConnectionFactory&quot;/>\\n" +  //$NON-NLS-1$
"		&lt;/connection_specification>\\n" +  //$NON-NLS-1$
"	&lt;/connection_specifications>\\n" +  //$NON-NLS-1$
"	\\n" +  //$NON-NLS-1$
"	&lt;access_specifications>\\n" +  //$NON-NLS-1$
"		&lt;access_specification name=&quot;amqAccess&quot;>\\n" +  //$NON-NLS-1$
"			&lt;destination identifier=&quot;dynamicQueues/MapQueue&quot; message_class=&quot;bytes&quot; />\\n" +  //$NON-NLS-1$
"				&lt;uses_connection connection=&quot;amqConn&quot;/>\\n" +  //$NON-NLS-1$
"				&lt;native_schema>\\n" +  //$NON-NLS-1$
"					&lt;attribute name=&quot;id&quot; type=&quot;Int&quot; />\\n" +  //$NON-NLS-1$
"					&lt;attribute name=&quot;fname&quot; type=&quot;Bytes&quot; length=&quot;15&quot; />\\n" +  //$NON-NLS-1$
"					&lt;attribute name=&quot;lname&quot; type=&quot;Bytes&quot; length=&quot;20&quot; />\\n" +  //$NON-NLS-1$
"					&lt;attribute name=&quot;age&quot; type=&quot;Int&quot; />\\n" +  //$NON-NLS-1$
"					&lt;attribute name=&quot;gender&quot; type=&quot;Bytes&quot; length=&quot;1&quot; />\\n" +  //$NON-NLS-1$
"					&lt;attribute name=&quot;score&quot; type=&quot;Float&quot; />\\n" +  //$NON-NLS-1$
"					&lt;attribute name=&quot;total&quot; type=&quot;Double&quot; />\\n" +  //$NON-NLS-1$
"				&lt;/native_schema>\\n" +  //$NON-NLS-1$
"		&lt;/access_specification>\\n" +  //$NON-NLS-1$
"	&lt;/access_specifications>\\n" +  //$NON-NLS-1$
"	&lt;/st:connections>\\n";  //$NON-NLS-1$

}
